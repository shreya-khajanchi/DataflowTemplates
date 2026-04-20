/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.transforms;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.writer.DataWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

/**
 * A {@link PTransform} that generates the remaining columns for a record, batches them, and writes
 * to the sink.
 *
 * <p>Ordering guarantees:
 *
 * <ul>
 *   <li><b>INSERT</b> buffers are flushed in topological (parent→child) order so FK- or
 *       interleave-dependent rows never hit the sink before their ancestor.
 *   <li><b>DELETE</b> events scheduled for the same wall-clock second are processed children-first
 *       (reverse-topological order) so parents are never removed while dependents still exist.
 *   <li><b>UPDATE</b> events have no ordering requirement among themselves.
 * </ul>
 */
public class BatchAndWrite extends PTransform<PCollection<KV<String, Row>>, PDone> {

  private final String sinkType;
  private final String sinkOptionsPath;
  private final Integer batchSize;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public BatchAndWrite(
      String sinkType,
      String sinkOptionsPath,
      Integer batchSize,
      PCollectionView<DataGeneratorSchema> schemaView) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = batchSize;
    this.schemaView = schemaView;
  }

  @Override
  public PDone expand(PCollection<KV<String, Row>> input) {
    input.apply(
        "BatchAndWriteFn",
        ParDo.of(new BatchAndWriteFn(sinkType, sinkOptionsPath, batchSize, schemaView))
            .withSideInputs(schemaView));
    return PDone.in(input.getPipeline());
  }

  /**
   * A lifecycle event (UPDATE or DELETE) scheduled against a previously-inserted row.
   *
   * <p>The primary key is carried as an ordered map of {@code (columnName -> value)} so that both
   * composite and non-integer PKs round-trip correctly. The map is a {@link LinkedHashMap} so
   * iteration order matches the declared PK column order.
   */
  @org.apache.beam.sdk.coders.DefaultCoder(org.apache.beam.sdk.coders.SerializableCoder.class)
  public static class LifecycleEvent implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public LinkedHashMap<String, Object> pkValues;
    public String type;
    public String tableName;

    public LifecycleEvent() {}

    public LifecycleEvent(LinkedHashMap<String, Object> pkValues, String type, String tableName) {
      this.pkValues = pkValues;
      this.type = type;
      this.tableName = tableName;
    }
  }

  static class BatchAndWriteFn extends DoFn<KV<String, Row>, Void> {
    private final String sinkType;
    private final String sinkOptionsPath;
    private final Integer configuredBatchSize;
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int batchSize;
    protected transient DataWriter writer;
    protected transient Faker faker;
    private transient DataGeneratorSchema schema;
    private transient List<String> logicalShardIds;

    /**
     * Root-to-leaf topological ordering of tables; populated lazily from {@link #schema}. Used to
     * flush INSERT buffers parent-first and DELETE buffers child-first (reverse).
     */
    private transient List<String> insertTopoOrder;

    static class BufferValue {
      final DataGeneratorTable table;
      final List<Row> rows;

      BufferValue(DataGeneratorTable table) {
        this.table = table;
        this.rows = new ArrayList<>();
      }
    }

    // Buffer: BufferKey -> BufferValue
    private transient Map<String, BufferValue> buffers;
    private transient int insertQps;
    private transient int updateQps;
    private transient int deleteQps;

    private static final org.slf4j.Logger LOG =
        org.slf4j.LoggerFactory.getLogger(BatchAndWriteFn.class);

    /** Slack between the last scheduled UPDATE and the scheduled DELETE, in milliseconds. */
    private static final long UPDATE_INTERVAL_MS = 5000L;
    /** Buffer applied when capping {@code numUpdates} against an ancestor's forced delete. */
    private static final long ANCESTOR_DELETE_SLACK_MS = UPDATE_INTERVAL_MS;

    private final Counter insertsGenerated =
        Metrics.counter(BatchAndWriteFn.class, "insertsGenerated");
    private final Counter updatesGenerated =
        Metrics.counter(BatchAndWriteFn.class, "updatesGenerated");
    private final Counter deletesGenerated =
        Metrics.counter(BatchAndWriteFn.class, "deletesGenerated");
    private final Counter batchesWritten = Metrics.counter(BatchAndWriteFn.class, "batchesWritten");
    private final Counter recordsWritten = Metrics.counter(BatchAndWriteFn.class, "recordsWritten");
    private final Counter unresolvableFkChildrenDropped =
        Metrics.counter(BatchAndWriteFn.class, "unresolvableFkChildrenDropped");

    @StateId("activeKeys")
    private final StateSpec<MapState<String, Row>> activeKeysSpec =
        StateSpecs.map(
            org.apache.beam.sdk.coders.StringUtf8Coder.of(),
            org.apache.beam.sdk.coders.SerializableCoder.of(Row.class));

    @StateId("eventQueue")
    private final StateSpec<MapState<Long, List<LifecycleEvent>>> eventQueueSpec =
        StateSpecs.map(
            org.apache.beam.sdk.coders.VarLongCoder.of(),
            org.apache.beam.sdk.coders.ListCoder.of(
                org.apache.beam.sdk.coders.SerializableCoder.of(LifecycleEvent.class)));

    @TimerId("eventTimer")
    private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId("activeTimestamps")
    private final StateSpec<org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>>>
        activeTimestampsSpec =
            StateSpecs.value(
                org.apache.beam.sdk.coders.SerializableCoder.of(
                    new org.apache.beam.sdk.values.TypeDescriptor<java.util.TreeSet<Long>>() {}));

    @StateId("tableMapState")
    private final StateSpec<MapState<String, DataGeneratorTable>> tableMapSpec =
        StateSpecs.map(
            org.apache.beam.sdk.coders.StringUtf8Coder.of(),
            org.apache.beam.sdk.coders.SerializableCoder.of(DataGeneratorTable.class));

    public BatchAndWriteFn(
        DataGeneratorOptions options, PCollectionView<DataGeneratorSchema> schemaView) {
      this.sinkType = options.getSinkType().name();
      this.sinkOptionsPath = options.getSinkOptions();
      this.configuredBatchSize = options.getBatchSize();
      this.schemaView = schemaView;
      this.schema = null;
      this.batchSize = options.getBatchSize() != null ? options.getBatchSize() : 100;
    }

    public BatchAndWriteFn(
        String sinkType,
        String sinkOptionsPath,
        Integer batchSize,
        PCollectionView<DataGeneratorSchema> schemaView) {
      this.sinkType = sinkType;
      this.sinkOptionsPath = sinkOptionsPath;
      this.configuredBatchSize = batchSize;
      this.schemaView = schemaView;
      this.schema = null;
      this.batchSize = batchSize != null ? batchSize : 100;
    }

    @Setup
    public void setup(org.apache.beam.sdk.options.PipelineOptions options) {
      DataGeneratorOptions genOptions = options.as(DataGeneratorOptions.class);
      this.insertQps = genOptions.getInsertQps();
      this.updateQps = genOptions.getUpdateQps();
      this.deleteQps = genOptions.getDeleteQps();
      if (this.writer == null) {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)) {
          this.writer =
              new com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter(sinkOptionsJson);
          try {
            ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
            List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkOptionsPath);
            this.logicalShardIds =
                shards.stream().map(Shard::getLogicalShardId).collect(Collectors.toList());
          } catch (Exception e) {
            throw new RuntimeException("Failed to read shards from " + sinkOptionsPath, e);
          }
        } else {
          this.writer =
              new com.google.cloud.teleport.v2.templates.writer.SpannerDataWriter(sinkOptionsJson);
        }
      }
      if (this.faker == null) {
        java.security.SecureRandom secureRandom = new java.security.SecureRandom();
        this.faker = new Faker(new java.util.Random(secureRandom.nextLong()));
      }
    }

    private String readSinkOptions(String path) {
      try (java.nio.channels.ReadableByteChannel channel =
          org.apache.beam.sdk.io.FileSystems.open(
              org.apache.beam.sdk.io.FileSystems.matchNewResource(path, false))) {
        try (java.io.Reader reader =
            new java.io.InputStreamReader(
                java.nio.channels.Channels.newInputStream(channel),
                java.nio.charset.StandardCharsets.UTF_8)) {
          return com.google.common.io.CharStreams.toString(reader);
        }
      } catch (java.io.IOException e) {
        throw new RuntimeException("Failed to read sink options from " + path, e);
      }
    }

    @StartBundle
    public void startBundle() {
      this.buffers = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
        @TimerId("eventTimer") Timer eventTimer) {
      if (schema == null) {
        schema = c.sideInput(schemaView);
        insertTopoOrder = buildInsertTopoOrder(schema);
      }
      String key = c.element().getKey();
      String tableName = key.split("#")[0];
      Row row = c.element().getValue();
      DataGeneratorTable table = schema.tables().get(tableName);

      if (table == null) {
        Metrics.counter(BatchAndWriteFn.class, "tableNotFound_" + tableName).inc();
        return;
      }

      tableMapState.put(tableName, table);

      LinkedHashMap<String, Object> pkMap = pkValuesOf(row, table);
      if (!pkMap.isEmpty()) {
        String stateKey = stateKeyOf(tableName, pkMap);
        LOG.info("State key {}", stateKey);

        org.apache.beam.sdk.state.ReadableState<Row> state = activeKeys.get(stateKey);
        if (state != null && state.read() != null) {
          LOG.info(
              "Collision/Retry detected for key: {}. Skipping to maintain integrity.", stateKey);
          return;
        }
      }

      // Root-level call: no sticky shard yet (will be chosen in processTable), no ancestor delete
      // constraint.
      processTable(
          table,
          row,
          activeKeys,
          eventQueueState,
          activeTimestamps,
          tableMapState,
          eventTimer,
          /* forcedDeleteTimestamp= */ 0L,
          /* earliestAncestorDelete= */ Long.MAX_VALUE,
          /* stickyShardId= */ null,
          new HashMap<>());
    }

    /**
     * Processes a single table record: generates the full Row, buffers the INSERT, and recursively
     * processes any child tables.
     *
     * @param forcedDeleteTimestamp if &gt; 0, a delete MUST happen at this wall-clock time
     *     (cascaded from a parent). Child updates are capped so the last update precedes this.
     * @param earliestAncestorDelete min wall-clock delete time across ALL ancestor tables (the
     *     tightest constraint), or {@link Long#MAX_VALUE} if no ancestor has a scheduled delete.
     * @param stickyShardId if non-null, the shard id to use for this row (propagated from root);
     *     if null, a shard is chosen here and propagated to children.
     */
    private void processTable(
        DataGeneratorTable table,
        Row row,
        MapState<String, Row> activeKeys,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        MapState<String, DataGeneratorTable> tableMapState,
        Timer eventTimer,
        long forcedDeleteTimestamp,
        long earliestAncestorDelete,
        String stickyShardId,
        Map<String, Row> ancestorRows) {
      insertsGenerated.inc();
      String tableName = table.name();

      tableMapState.put(tableName, table);

      // 0. Resolve sticky shard id (propagated from root). If still unresolved, pick one here.
      String resolvedShardId = resolveShardId(row, stickyShardId);

      // 1. Complete the row (generate any missing columns, including uniques derived from PK).
      Row fullRow = completeRow(table, row, resolvedShardId);

      LinkedHashMap<String, Object> pkMap = pkValuesOf(fullRow, table);
      if (!pkMap.isEmpty()) {
        activeKeys.put(stateKeyOf(tableName, pkMap), createReducedRow(fullRow, table));
      }

      // 2. Buffer Row for current record.
      bufferRow(tableName, fullRow, Constants.MUTATION_INSERT, table, resolvedShardId);

      // 3. Compute lifecycle timestamps for THIS table.
      long deleteTimestamp = 0L;
      int numUpdates = 0;
      long now = System.currentTimeMillis();

      if (!pkMap.isEmpty()) {
        int tableInsertQps = table.insertQps();
        int tableUpdateQps = table.updateQps() != null ? table.updateQps() : updateQps;
        int tableDeleteQps = table.deleteQps() != null ? table.deleteQps() : deleteQps;

        double updateRatio = tableInsertQps > 0 ? (double) tableUpdateQps / tableInsertQps : 0;
        double deleteRatio = tableInsertQps > 0 ? (double) tableDeleteQps / tableInsertQps : 0;

        numUpdates = (int) updateRatio;
        double fractionalUpdate = updateRatio - numUpdates;
        if (java.util.concurrent.ThreadLocalRandom.current().nextDouble() < fractionalUpdate) {
          numUpdates++;
        }

        if (forcedDeleteTimestamp > 0) {
          deleteTimestamp = forcedDeleteTimestamp;
        } else {
          boolean hasDelete =
              java.util.concurrent.ThreadLocalRandom.current().nextDouble() < deleteRatio;
          if (hasDelete) {
            // Delete 5s after last update (or 5s after insert if no updates).
            deleteTimestamp = now + UPDATE_INTERVAL_MS * numUpdates + UPDATE_INTERVAL_MS;
          }
        }

        // Cap number of updates so the last update lands strictly before the earliest ancestor
        // delete AND before our own forced delete (whichever is tighter).
        long myDeleteBound = deleteTimestamp > 0 ? deleteTimestamp : Long.MAX_VALUE;
        long effectiveDeleteBound = Math.min(myDeleteBound, earliestAncestorDelete);
        if (effectiveDeleteBound < Long.MAX_VALUE) {
          long budget = effectiveDeleteBound - now - ANCESTOR_DELETE_SLACK_MS;
          long maxUpdates = budget > 0 ? budget / UPDATE_INTERVAL_MS : 0;
          numUpdates = (int) Math.min(numUpdates, Math.max(0, maxUpdates));
        }
      }

      // 4. Recurse into children FIRST (so nested generation completes before we schedule update
      //    timers for THIS row — children inherit our delete timestamp and the min-ancestor
      //    constraint).
      Map<String, Row> updatedAncestorRows = new HashMap<>(ancestorRows);
      updatedAncestorRows.put(tableName, fullRow);

      long childEarliestAncestorDelete = earliestAncestorDelete;
      if (deleteTimestamp > 0) {
        childEarliestAncestorDelete = Math.min(childEarliestAncestorDelete, deleteTimestamp);
      }

      if (table.childTables() != null && !table.childTables().isEmpty()) {
        for (String childTableName : table.childTables()) {
          DataGeneratorTable childTable = schema.tables().get(childTableName);
          if (childTable != null) {
            generateAndWriteChildren(
                table,
                fullRow,
                childTable,
                activeKeys,
                eventQueueState,
                activeTimestamps,
                tableMapState,
                eventTimer,
                deleteTimestamp,
                childEarliestAncestorDelete,
                resolvedShardId,
                updatedAncestorRows);
          } else {
            Metrics.counter(BatchAndWriteFn.class, "childTableNotFound_" + childTableName).inc();
          }
        }
      }

      // 5. Schedule lifecycle events for this record LAST.
      if (!pkMap.isEmpty()) {
        try {
          for (int i = 1; i <= numUpdates; i++) {
            scheduleEvent(
                now + UPDATE_INTERVAL_MS * i,
                new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, tableName),
                eventQueueState,
                activeTimestamps,
                eventTimer);
          }
          if (deleteTimestamp > 0) {
            scheduleEvent(
                deleteTimestamp,
                new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, tableName),
                eventQueueState,
                activeTimestamps,
                eventTimer);
          }
        } catch (Exception e) {
          LOG.error("Error scheduling events", e);
        }
      }
    }

    private void generateAndWriteChildren(
        DataGeneratorTable parentTable,
        Row parentRow,
        DataGeneratorTable childTable,
        MapState<String, Row> activeKeys,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        MapState<String, DataGeneratorTable> tableMapState,
        Timer eventTimer,
        long forcedDeleteTimestamp,
        long earliestAncestorDelete,
        String stickyShardId,
        Map<String, Row> ancestorRows) {
      double parentQps = Math.max(1, parentTable.insertQps());
      double childQps = childTable.insertQps();
      double ratio = childQps / parentQps;

      int numChildren = (int) ratio;
      if (faker.random().nextDouble() < (ratio - numChildren)) {
        numChildren++;
      }

      for (int i = 0; i < numChildren; i++) {
        Row childRow = generateChildRow(parentTable, parentRow, childTable, ancestorRows,
            activeKeys, stickyShardId);
        if (childRow == null) {
          // FK could not be resolved; skip this child emission to maintain integrity.
          unresolvableFkChildrenDropped.inc();
          continue;
        }
        processTable(
            childTable,
            childRow,
            activeKeys,
            eventQueueState,
            activeTimestamps,
            tableMapState,
            eventTimer,
            forcedDeleteTimestamp,
            earliestAncestorDelete,
            stickyShardId,
            ancestorRows);
      }
    }

    /**
     * Generates a child row, populating FK columns from ancestor rows (or from the current parent
     * row if the ancestor is the direct parent). Returns {@code null} when any FK cannot be
     * resolved — the caller is expected to skip such rows rather than emit a row with random FK
     * values that would violate the constraint at the sink.
     *
     * <p>When the child has multiple FKs pointing to the SAME parent table (e.g. {@code
     * manager_id} and {@code assistant_id} both referencing {@code Employee}), secondary FKs are
     * populated from a DIFFERENT previously-active row of that table (pulled from {@code
     * activeKeys}) so the two columns don't end up with identical values.
     */
    private Row generateChildRow(
        DataGeneratorTable parentTable,
        Row parentRow,
        DataGeneratorTable childTable,
        Map<String, Row> ancestorRows,
        MapState<String, Row> activeKeys,
        String stickyShardId) {
      Map<String, Object> columnValues = new HashMap<>();

      // Track how many FKs per referenced-table we've resolved; used to pick distinct rows when
      // the same parent table is referenced more than once.
      Map<String, Integer> fkOrdinalByTable = new HashMap<>();

      boolean resolvedAllFks = true;

      if (childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty()) {
        for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
          String targetTable = fk.referencedTable();
          int ordinal = fkOrdinalByTable.getOrDefault(targetTable, 0);
          fkOrdinalByTable.put(targetTable, ordinal + 1);

          Row source;
          if (ordinal == 0 && ancestorRows.containsKey(targetTable)) {
            // First FK to this parent: use the row generated in the current ancestor chain.
            source = ancestorRows.get(targetTable);
          } else if (ancestorRows.containsKey(targetTable)) {
            // Subsequent FK to the SAME parent: pick a different row from active keys.
            source = pickDifferentActiveRow(activeKeys, targetTable, ancestorRows.get(targetTable));
            if (source == null) {
              // No alternative row available yet — fall back to the same ancestor row.
              source = ancestorRows.get(targetTable);
            }
          } else {
            // Not in the current ancestor chain: try to find any active row for that table.
            source = pickAnyActiveRow(activeKeys, targetTable);
          }

          if (source == null) {
            // Last-ditch: try the immediate parent row iff the referenced columns exist on it.
            boolean allPresent = true;
            for (String parentCol : fk.referencedColumns()) {
              if (!parentRow.getSchema().hasField(parentCol)) {
                allPresent = false;
                break;
              }
            }
            if (allPresent) {
              source = parentRow;
            } else {
              // Cannot resolve this FK — bail out so the caller can skip this child.
              LOG.warn(
                  "Cannot resolve FK {} from {} -> {}: no ancestor or active row available",
                  fk.name(),
                  childTable.name(),
                  targetTable);
              resolvedAllFks = false;
              break;
            }
          }

          for (int i = 0; i < fk.keyColumns().size(); i++) {
            String childCol = fk.keyColumns().get(i);
            String targetCol = fk.referencedColumns().get(i);
            columnValues.put(childCol, getFieldFromRow(source, targetCol));
          }
        }
      }

      if (!resolvedAllFks) {
        return null;
      }

      boolean hasFks = childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty();
      if (!hasFks
          && childTable.interleavedInTable() != null
          && childTable.interleavedInTable().equals(parentTable.name())) {
        // For interleaved tables without explicit FK, assume child PK starts with parent PK
        // columns. Rely on column-name matching.
        for (String pk : parentTable.primaryKeys()) {
          Object val = getFieldFromRow(parentRow, pk);
          if (val != null) {
            columnValues.put(pk, val);
          }
        }
      }

      // Compute the PK up front so unique-column derivations can hash against it.
      LinkedHashMap<String, Object> provisionalPk = new LinkedHashMap<>();
      for (String pkCol : childTable.primaryKeys()) {
        Object v = columnValues.get(pkCol);
        if (v == null) {
          DataGeneratorColumn col = findColumn(childTable, pkCol);
          if (col != null) {
            v = generateValue(col);
            columnValues.put(pkCol, v);
          }
        }
        provisionalPk.put(pkCol, v);
      }

      Set<String> uniqueCols = uniqueColumnNames(childTable);

      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (DataGeneratorColumn col : childTable.columns()) {
        Object val = columnValues.get(col.name());
        if (val == null) {
          if (uniqueCols.contains(col.name())) {
            val = deriveUniqueValue(col, childTable.name(), provisionalPk);
          } else {
            val = generateValue(col);
          }
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }

      // Sticky shard: always propagate the parent's shard to the child so FK-related rows co-
      // locate. When present on the parent row's schema use that; otherwise use the sticky id
      // that was picked at root level.
      String shardId = null;
      if (parentRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = parentRow.getString(Constants.SHARD_ID_COLUMN_NAME);
      } else if (stickyShardId != null && !stickyShardId.isEmpty()) {
        shardId = stickyShardId;
      }

      if (shardId != null) {
        schemaBuilder.addField(
            Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
        values.add(shardId);
      }

      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private DataGeneratorColumn findColumn(DataGeneratorTable table, String colName) {
      for (DataGeneratorColumn c : table.columns()) {
        if (c.name().equals(colName)) {
          return c;
        }
      }
      return null;
    }

    /**
     * Returns any previously-activated row of {@code tableName} whose PK differs from {@code
     * avoid}, or {@code null} if none exists. Comparison is by derived state key (table + PK)
     * rather than object identity because the row stored in state is the reduced form.
     */
    private Row pickDifferentActiveRow(
        MapState<String, Row> activeKeys, String tableName, Row avoid) {
      String prefix = tableName + ":";
      String avoidKey = null;
      if (avoid != null && schema != null && schema.tables().containsKey(tableName)) {
        LinkedHashMap<String, Object> avoidPk = pkValuesOf(avoid, schema.tables().get(tableName));
        if (!avoidPk.isEmpty()) {
          avoidKey = stateKeyOf(tableName, avoidPk);
        }
      }
      Iterable<Map.Entry<String, Row>> entries = activeKeys.entries().read();
      if (entries == null) {
        return null;
      }
      for (Map.Entry<String, Row> e : entries) {
        if (e.getKey() == null || !e.getKey().startsWith(prefix)) {
          continue;
        }
        if (avoidKey != null && avoidKey.equals(e.getKey())) {
          continue;
        }
        Row candidate = e.getValue();
        if (candidate != null) {
          return candidate;
        }
      }
      return null;
    }

    private Row pickAnyActiveRow(MapState<String, Row> activeKeys, String tableName) {
      String prefix = tableName + ":";
      Iterable<Map.Entry<String, Row>> entries = activeKeys.entries().read();
      if (entries == null) {
        return null;
      }
      for (Map.Entry<String, Row> e : entries) {
        if (e.getKey() != null && e.getKey().startsWith(prefix) && e.getValue() != null) {
          return e.getValue();
        }
      }
      return null;
    }

    private void scheduleEvent(
        long timestamp,
        LifecycleEvent event,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        Timer eventTimer) {

      long snappedTimestamp = (timestamp / 1000) * 1000;

      List<LifecycleEvent> events = eventQueueState.get(snappedTimestamp).read();
      if (events == null) {
        events = new ArrayList<>();
      }
      events.add(event);
      eventQueueState.put(snappedTimestamp, events);

      java.util.TreeSet<Long> timestamps = activeTimestamps.read();
      if (timestamps == null) {
        timestamps = new java.util.TreeSet<>();
      }
      timestamps.add(snappedTimestamp);
      activeTimestamps.write(timestamps);

      eventTimer.set(org.joda.time.Instant.ofEpochMilli(timestamps.first()));
    }

    /**
     * Extracts primary-key column values from {@code row} in declared PK-column order. Preserves
     * the original types (Long, String, Integer, byte[], ...) so the values can be rebuilt in
     * {@code generateUpdateRow} / {@code generateDeleteRow} without lossy stringification.
     */
    private LinkedHashMap<String, Object> pkValuesOf(Row row, DataGeneratorTable table) {
      LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
      for (DataGeneratorColumn col : table.columns()) {
        if (col.isPrimaryKey() && row.getSchema().hasField(col.name())) {
          pk.put(col.name(), row.getValue(col.name()));
        }
      }
      return pk;
    }

    /** Deterministic stringification used as a key into {@code activeKeys} state. */
    private String stateKeyOf(String tableName, LinkedHashMap<String, Object> pkMap) {
      StringBuilder sb = new StringBuilder(tableName).append(":");
      boolean first = true;
      for (Map.Entry<String, Object> e : pkMap.entrySet()) {
        if (!first) {
          sb.append("#");
        }
        first = false;
        sb.append(e.getValue() == null ? "null" : e.getValue().toString());
      }
      return sb.toString();
    }

    private Object getFieldFromRow(Row row, String fieldName) {
      try {
        return row.getValue(fieldName);
      } catch (IllegalArgumentException e) {
        return null;
      }
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
        @TimerId("eventTimer") Timer eventTimer) {

      java.util.TreeSet<Long> timestamps = activeTimestamps.read();
      if (timestamps == null || timestamps.isEmpty()) {
        return;
      }

      long now = System.currentTimeMillis();
      java.util.List<Long> toRemove = new java.util.ArrayList<>();

      for (Long ts : timestamps) {
        if (ts <= now) {
          List<LifecycleEvent> events = eventQueueState.get(ts).read();
          if (events != null) {
            // Fix B: sort events so children DELETE before their parents, and parents update
            // before children. All DELETEs are emitted after all UPDATEs/others to minimise
            // FK-dangling windows.
            List<LifecycleEvent> ordered = orderEventsForTick(events, tableMapState);
            for (LifecycleEvent event : ordered) {
              processEvent(event, activeKeys, eventTimer, tableMapState);
            }
            eventQueueState.remove(ts);
          }
          toRemove.add(ts);
        } else {
          break;
        }
      }

      timestamps.removeAll(toRemove);
      activeTimestamps.write(timestamps);

      if (!timestamps.isEmpty()) {
        eventTimer.set(org.joda.time.Instant.ofEpochMilli(timestamps.first()));
      }
    }

    /**
     * Sort lifecycle events scheduled for the same wall-clock second into the correct order:
     *
     * <ol>
     *   <li>UPDATEs first, ascending by DAG depth (parents before children — rare that it
     *       matters but consistent).
     *   <li>DELETEs last, DESCENDING by DAG depth (deepest child first) so FK / interleave
     *       constraints are satisfied when the sink writes them in buffer order.
     * </ol>
     */
    private List<LifecycleEvent> orderEventsForTick(
        List<LifecycleEvent> events, MapState<String, DataGeneratorTable> tableMapState) {
      // Cache depth lookups to avoid N state reads.
      Map<String, Integer> depthCache = new HashMap<>();
      Comparator<LifecycleEvent> cmp =
          (a, b) -> {
            int priA = isDelete(a) ? 1 : 0;
            int priB = isDelete(b) ? 1 : 0;
            if (priA != priB) {
              return Integer.compare(priA, priB);
            }
            int depthA = depthCache.computeIfAbsent(a.tableName, t -> depthOf(t, tableMapState));
            int depthB = depthCache.computeIfAbsent(b.tableName, t -> depthOf(t, tableMapState));
            if (isDelete(a)) {
              // Deepest first for DELETE.
              return Integer.compare(depthB, depthA);
            }
            // Shallowest first otherwise.
            return Integer.compare(depthA, depthB);
          };
      List<LifecycleEvent> copy = new ArrayList<>(events);
      copy.sort(cmp);
      return copy;
    }

    private boolean isDelete(LifecycleEvent e) {
      return Constants.MUTATION_DELETE.equals(e.type);
    }

    private int depthOf(String tableName, MapState<String, DataGeneratorTable> tableMapState) {
      try {
        DataGeneratorTable t = tableMapState.get(tableName).read();
        if (t != null) {
          return t.depth();
        }
      } catch (Exception ignored) {
        // Fall through to schema side-input if available.
      }
      if (schema != null && schema.tables().containsKey(tableName)) {
        return schema.tables().get(tableName).depth();
      }
      return 0;
    }

    private void processEvent(
        LifecycleEvent event,
        MapState<String, Row> activeKeys,
        Timer eventTimer,
        MapState<String, DataGeneratorTable> tableMapState) {
      String stateKey = stateKeyOf(event.tableName, event.pkValues);
      try {
        if (activeKeys.get(stateKey).read() == null) {
          return; // Key no longer active
        }
      } catch (Exception e) {
        return;
      }

      DataGeneratorTable table = null;
      try {
        table = tableMapState.get(event.tableName).read();
      } catch (Exception e) {
        // Ignore
      }
      if (table == null) {
        return;
      }

      String shardId = "";
      Row originalRow = null;
      try {
        originalRow = activeKeys.get(stateKey).read();
        if (originalRow != null
            && originalRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
          shardId = originalRow.getString(Constants.SHARD_ID_COLUMN_NAME);
        }
      } catch (Exception ignored) {
        // shardId stays empty; bufferMutation will fall back to random.
      }

      if (Constants.MUTATION_UPDATE.equals(event.type)) {
        Row updateRow = generateUpdateRow(event.pkValues, table, originalRow);
        bufferRow(event.tableName, updateRow, Constants.MUTATION_UPDATE, table, shardId);
        updatesGenerated.inc();
      } else if (Constants.MUTATION_DELETE.equals(event.type)) {
        Row deleteRow = generateDeleteRow(event.pkValues, table);
        bufferRow(event.tableName, deleteRow, Constants.MUTATION_DELETE, table, shardId);
        deletesGenerated.inc();

        try {
          activeKeys.remove(stateKey);
        } catch (Exception e) {
        }
      }
    }

    Row generateUpdateRow(
        LinkedHashMap<String, Object> pkValues, DataGeneratorTable table, Row originalRow) {
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      Set<String> fkColumns = new HashSet<>();
      if (table.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : table.foreignKeys()) {
          fkColumns.addAll(fk.keyColumns());
        }
      }

      Set<String> uniqueColumns = uniqueColumnNames(table);

      for (DataGeneratorColumn col : table.columns()) {
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        if (col.isPrimaryKey()) {
          values.add(pkValues.get(col.name()));
        } else if (fkColumns.contains(col.name()) || uniqueColumns.contains(col.name())) {
          Object val =
              originalRow != null && originalRow.getSchema().hasField(col.name())
                  ? originalRow.getValue(col.name())
                  : uniqueColumns.contains(col.name())
                      ? deriveUniqueValue(col, table.name(), pkValues)
                      : generateValue(col);
          values.add(val);
        } else {
          values.add(generateValue(col));
        }
      }
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Row generateDeleteRow(LinkedHashMap<String, Object> pkValues, DataGeneratorTable table) {
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (DataGeneratorColumn col : table.columns()) {
        Schema.FieldType fieldType = DataGeneratorUtils.mapToBeamFieldType(col.logicalType());
        if (col.isPrimaryKey()) {
          schemaBuilder.addField(Schema.Field.of(col.name(), fieldType));
          values.add(pkValues.get(col.name()));
        } else {
          schemaBuilder.addField(Schema.Field.nullable(col.name(), fieldType));
          values.add(null);
        }
      }
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Row createReducedRow(Row fullRow, DataGeneratorTable table) {
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      Set<String> fkColumns = new HashSet<>();
      if (table.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : table.foreignKeys()) {
          fkColumns.addAll(fk.keyColumns());
        }
      }

      Set<String> uniqueColumns = uniqueColumnNames(table);

      for (DataGeneratorColumn col : table.columns()) {
        if (col.isPrimaryKey()
            || fkColumns.contains(col.name())
            || uniqueColumns.contains(col.name())) {
          schemaBuilder.addField(
              Schema.Field.of(
                  col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
          values.add(fullRow.getValue(col.name()));
        }
      }
      // Preserve shard id on the reduced row so lifecycle events can route correctly.
      if (fullRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        schemaBuilder.addField(
            Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
        values.add(fullRow.getString(Constants.SHARD_ID_COLUMN_NAME));
      }
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    /** Buffers a row, flushing in topo order if the buffer tripped the batch-size limit. */
    private void bufferRow(
        String tableName,
        Row row,
        String operation,
        DataGeneratorTable table,
        String shardIdHint) {
      String shardId = shardIdHint == null ? "" : shardIdHint;
      if (shardId.isEmpty() && row.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = row.getString(Constants.SHARD_ID_COLUMN_NAME);
      }
      if (shardId.isEmpty()
          && Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)
          && logicalShardIds != null
          && !logicalShardIds.isEmpty()) {
        shardId =
            logicalShardIds.get(
                java.util.concurrent.ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
      }
      String bufferKey = tableName + "#" + (shardId.isEmpty() ? "default" : shardId) + "#"
          + operation;

      buffers.computeIfAbsent(bufferKey, k -> new BufferValue(table)).rows.add(row);
      if (buffers.get(bufferKey).rows.size() >= batchSize) {
        // Fix A: any INSERT flush must happen parent-first across the entire DAG; any DELETE
        // flush must happen child-first across the entire DAG. UPDATE is order-free.
        if (Constants.MUTATION_INSERT.equals(operation)) {
          flushInsertsInTopoOrder();
        } else if (Constants.MUTATION_DELETE.equals(operation)) {
          flushDeletesInReverseTopoOrder();
        } else {
          flush(bufferKey);
        }
      }
    }

    /** Resolves the sticky shard id for this row: (1) propagated from root, (2) picked here. */
    private String resolveShardId(Row row, String inherited) {
      if (inherited != null && !inherited.isEmpty()) {
        return inherited;
      }
      if (row.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        return row.getString(Constants.SHARD_ID_COLUMN_NAME);
      }
      if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)
          && logicalShardIds != null
          && !logicalShardIds.isEmpty()) {
        return logicalShardIds.get(
            java.util.concurrent.ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
      }
      return "";
    }

    @FinishBundle
    public void finishBundle() {
      // Fix A / B: final flush must respect DAG order.
      flushInsertsInTopoOrder();
      flushUpdates();
      flushDeletesInReverseTopoOrder();
    }

    @Teardown
    public void teardown() {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          throw new RuntimeException("Failed to close writer", e);
        }
      }
    }

    private void flushInsertsInTopoOrder() {
      List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
      // Flush buffers whose tableName matches each table in parent-first order.
      for (String table : order) {
        flushByTableAndOp(table, Constants.MUTATION_INSERT);
      }
      // Belt-and-suspenders: any buffer for a table not in the topo order (e.g. new table seen
      // mid-pipeline before topo rebuilt) still gets flushed.
      for (String bufferKey : new ArrayList<>(buffers.keySet())) {
        String[] parts = bufferKey.split("#");
        if (parts.length >= 3 && Constants.MUTATION_INSERT.equals(parts[2])) {
          flush(bufferKey);
        }
      }
    }

    private void flushDeletesInReverseTopoOrder() {
      List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
      for (int i = order.size() - 1; i >= 0; i--) {
        flushByTableAndOp(order.get(i), Constants.MUTATION_DELETE);
      }
      for (String bufferKey : new ArrayList<>(buffers.keySet())) {
        String[] parts = bufferKey.split("#");
        if (parts.length >= 3 && Constants.MUTATION_DELETE.equals(parts[2])) {
          flush(bufferKey);
        }
      }
    }

    private void flushUpdates() {
      for (String bufferKey : new ArrayList<>(buffers.keySet())) {
        String[] parts = bufferKey.split("#");
        if (parts.length >= 3 && Constants.MUTATION_UPDATE.equals(parts[2])) {
          flush(bufferKey);
        }
      }
    }

    private void flushByTableAndOp(String tableName, String op) {
      String prefix = tableName + "#";
      String suffix = "#" + op;
      for (String bufferKey : new ArrayList<>(buffers.keySet())) {
        if (bufferKey.startsWith(prefix) && bufferKey.endsWith(suffix)) {
          flush(bufferKey);
        }
      }
    }

    private void flush(String bufferKey) {
      String[] parts = bufferKey.split("#");
      String tableName = parts[0];
      String shardId = parts.length > 1 ? parts[1] : "";
      String operation = Constants.MUTATION_INSERT;
      if (parts.length > 2) {
        operation = parts[2];
      }
      if ("default".equals(shardId)) {
        shardId = "";
      }

      BufferValue bv = buffers.get(bufferKey);
      List<Row> batch = bv != null ? bv.rows : null;
      DataGeneratorTable table = bv != null ? bv.table : null;

      if (batch != null && !batch.isEmpty()) {
        writer.write(batch, table, shardId, operation);
        batchesWritten.inc();
        recordsWritten.inc(batch.size());
        if (!shardId.isEmpty()) {
          Metrics.counter(BatchAndWriteFn.class, "recordsWritten_" + shardId).inc(batch.size());
        }
        batch.clear();
      }
      // Drop empty buffer entry to avoid linearly scanning it on subsequent flushes.
      if (bv != null && bv.rows.isEmpty()) {
        buffers.remove(bufferKey);
      }
    }

    /**
     * Completes a partial row by generating any missing columns. Unique columns get a
     * deterministic value derived from PK + column name so two generations of the same PK in
     * different workers cannot collide.
     */
    private Row completeRow(DataGeneratorTable table, Row partialRow, String shardIdHint) {
      boolean hasAllColumns = true;
      for (DataGeneratorColumn col : table.columns()) {
        if (!partialRow.getSchema().hasField(col.name())) {
          hasAllColumns = false;
          break;
        }
      }

      // Precompute PK map from partialRow (or from columns we're about to generate).
      LinkedHashMap<String, Object> pkMap = new LinkedHashMap<>();
      for (DataGeneratorColumn col : table.columns()) {
        if (col.isPrimaryKey() && partialRow.getSchema().hasField(col.name())) {
          pkMap.put(col.name(), partialRow.getValue(col.name()));
        }
      }

      Set<String> uniqueCols = uniqueColumnNames(table);

      if (hasAllColumns && !needsUniqueRewrite(table, partialRow, uniqueCols)) {
        return partialRow;
      }

      Map<String, Object> currentValues = new HashMap<>();
      for (Schema.Field field : partialRow.getSchema().getFields()) {
        currentValues.put(field.getName(), partialRow.getValue(field.getName()));
      }

      List<Object> values = new ArrayList<>();
      Schema.Builder schemaBuilder = Schema.builder();

      for (DataGeneratorColumn col : table.columns()) {
        Object val = currentValues.get(col.name());
        if (val == null) {
          if (uniqueCols.contains(col.name())) {
            val = deriveUniqueValue(col, table.name(), pkMap);
          } else {
            val = generateValue(col);
          }
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }

      // Preserve / inject shard id (from the hint when the partial row has none).
      String shardId = null;
      if (partialRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = partialRow.getString(Constants.SHARD_ID_COLUMN_NAME);
      } else if (shardIdHint != null && !shardIdHint.isEmpty()) {
        shardId = shardIdHint;
      }
      if (shardId != null) {
        schemaBuilder.addField(
            Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
        values.add(shardId);
      }

      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    /**
     * True if any unique column on the partialRow was produced by a generic random generator
     * upstream (no PK-derived hashing), in which case we overwrite it with a collision-safe value.
     * Today we don't have enough signal to detect that at the row level, so we defer to the
     * ordinary column-missing path (only rewrite when missing).
     */
    private boolean needsUniqueRewrite(
        DataGeneratorTable table, Row partialRow, Set<String> uniqueCols) {
      // Future hook: check column-level metadata marking "collision-unsafe" provenance.
      return false;
    }

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }

    private Set<String> uniqueColumnNames(DataGeneratorTable table) {
      Set<String> uniqueColumns = new HashSet<>();
      if (table.uniqueKeys() != null) {
        for (com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey uk :
            table.uniqueKeys()) {
          uniqueColumns.addAll(uk.keyColumns());
        }
      }
      return uniqueColumns;
    }

    /**
     * Deterministic, collision-free value for a unique column, derived from {@code (table,
     * column, pkValues)}. Because PKs are unique per row, the derived value is also unique —
     * regardless of how many workers generate in parallel.
     */
    private Object deriveUniqueValue(
        DataGeneratorColumn col, String tableName, Map<String, Object> pkValues) {
      long hash = stableHash(tableName, col.name(), pkValues);
      com.google.cloud.teleport.v2.templates.model.LogicalType lt = col.logicalType();
      switch (lt) {
        case INT64:
          return hash;
        case BOOLEAN:
          return (hash & 1L) == 1L;
        case FLOAT64:
        case NUMERIC:
          return ((double) hash) / Long.MAX_VALUE;
        case BYTES:
          return longToBytes(hash);
        case DATE:
          // Derive a stable date from hash; unique per row.
          return java.time.LocalDate.ofEpochDay(Math.floorMod(hash, 36525L));
        case TIMESTAMP:
          return org.joda.time.Instant.ofEpochMilli(Math.floorMod(hash, 4102444800000L));
        case UUID:
        case STRING:
        case JSON:
        case ENUM:
        default:
          // Fit within declared size when known.
          String base = tableName + "_" + col.name() + "_" + Long.toHexString(hash);
          if (col.size() != null && col.size() > 0 && base.length() > col.size()) {
            return base.substring(0, (int) Math.min(base.length(), col.size().longValue()));
          }
          return base;
      }
    }

    private static byte[] longToBytes(long v) {
      byte[] out = new byte[8];
      for (int i = 7; i >= 0; i--) {
        out[i] = (byte) (v & 0xff);
        v >>= 8;
      }
      return out;
    }

    private static long stableHash(String tableName, String colName, Map<String, Object> pkValues) {
      // 64-bit FNV-1a — deterministic across JVMs/workers.
      long h = 0xcbf29ce484222325L;
      h = fnv(h, tableName);
      h = fnv(h, "|");
      h = fnv(h, colName);
      h = fnv(h, "|");
      if (pkValues != null) {
        for (Map.Entry<String, Object> e : pkValues.entrySet()) {
          h = fnv(h, e.getKey());
          h = fnv(h, "=");
          h = fnv(h, e.getValue() == null ? "null" : e.getValue().toString());
          h = fnv(h, ";");
        }
      }
      return h;
    }

    private static long fnv(long h, String s) {
      for (int i = 0; i < s.length(); i++) {
        h ^= s.charAt(i);
        h *= 0x100000001b3L;
      }
      return h;
    }

    /**
     * Kahn-ish topological sort of tables: roots first, then direct children, etc. Ties are
     * broken by table name for determinism.
     */
    private List<String> buildInsertTopoOrder(DataGeneratorSchema schema) {
      Map<String, DataGeneratorTable> tables = schema.tables();
      List<String> order = new ArrayList<>();
      // We rely on the pre-computed depth on each DataGeneratorTable (populated by
      // SchemaUtils.setSchemaDAG). Sort ascending by depth, ties by name.
      List<DataGeneratorTable> sorted = new ArrayList<>(tables.values());
      sorted.sort(
          Comparator.comparingInt(DataGeneratorTable::depth)
              .thenComparing(DataGeneratorTable::name));
      for (DataGeneratorTable t : sorted) {
        order.add(t.name());
      }
      return order;
    }
  }
}
