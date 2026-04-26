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
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
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
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

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
 *
 * <p>The output {@code PCollection<String>} carries dead-letter records — JSON-encoded failures
 * produced when row generation throws or the sink rejects a batch. Callers should pipe this output
 * to {@link WriteFailuresToGcs} (or another sink) when a {@code deadLetterQueueDirectory} is
 * configured. When no failures occur, the output is empty.
 */
public class BatchAndWrite extends PTransform<PCollection<KV<String, Row>>, PCollection<String>> {

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
  public PCollection<String> expand(PCollection<KV<String, Row>> input) {
    return input
        .apply(
            "BatchAndWriteFn",
            ParDo.of(new BatchAndWriteFn(sinkType, sinkOptionsPath, batchSize, schemaView))
                .withSideInputs(schemaView))
        .setCoder(org.apache.beam.sdk.coders.StringUtf8Coder.of());
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

  static class BatchAndWriteFn extends DoFn<KV<String, Row>, String> {
    private final String sinkType;
    private final String sinkOptionsPath;
    private final Integer configuredBatchSize;
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int batchSize;
    protected transient DataWriter writer;
    protected transient Faker faker;

    /**
     * Cached schema side-input. Populated on first {@link #ensureSchemaInitialized} call. {@code
     * volatile} is defensive — Beam guarantees a DoFn instance is used by at most one thread at a
     * time, but marking the field makes the single-write / single-read invariant explicit and
     * immune to JIT re-ordering concerns.
     */
    private transient volatile DataGeneratorSchema schema;

    private transient List<String> logicalShardIds;

    /**
     * Root-to-leaf topological ordering of tables derived from {@link #schema}. Populated
     * together with {@code schema} so the two fields are always in-sync. Used to flush INSERT
     * buffers parent-first and DELETE buffers child-first (reverse).
     */
    private transient volatile List<String> insertTopoOrder;

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

    /**
     * Per-bundle accumulator for dead-letter records. {@link #flush(String)} catches sink-write
     * exceptions and pushes one JSON failure per row in the failed batch into this list. Each
     * lifecycle method (process / timer / finishBundle) drains the list into its own output
     * context so the records flow downstream regardless of where the failure happened.
     */
    private transient List<String> pendingDlq;

    private final Counter writeFailures = Metrics.counter(BatchAndWriteFn.class, "writeFailures");
    private final Counter generationFailures =
        Metrics.counter(BatchAndWriteFn.class, "generationFailures");

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

    /**
     * Pending event timestamps (snapped to the second) for this key, in ascending order. We use a
     * {@code List<Long>} kept sorted on insertion rather than a {@code TreeSet} so we don't rely
     * on {@code SerializableCoder} preserving {@code TreeSet}'s internal ordering through
     * checkpoint / restore — the {@code ListCoder + VarLongCoder} pair is explicitly order-
     * preserving and more compact.
     */
    @StateId("activeTimestamps")
    private final StateSpec<org.apache.beam.sdk.state.ValueState<List<Long>>>
        activeTimestampsSpec =
            StateSpecs.value(
                org.apache.beam.sdk.coders.ListCoder.of(
                    org.apache.beam.sdk.coders.VarLongCoder.of()));

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
      // Reset lazily-populated caches so each DoFn instance has a clean starting state.
      // schema / insertTopoOrder are repopulated on the first processElement call via
      // ensureSchemaInitialized (which is the only place that can access side inputs).
      this.schema = null;
      this.insertTopoOrder = null;
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
      this.pendingDlq = new ArrayList<>();
    }

    /**
     * Drains {@link #pendingDlq} into the supplied output sink (typically {@code
     * ProcessContext::output} or {@code OnTimerContext::output}) and clears the buffer. Safe to
     * call when the buffer is null or empty.
     */
    private void drainPendingDlq(java.util.function.Consumer<String> sink) {
      if (pendingDlq == null || pendingDlq.isEmpty()) {
        return;
      }
      for (String record : pendingDlq) {
        sink.accept(record);
      }
      pendingDlq.clear();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<List<Long>> activeTimestamps,
        @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
        @TimerId("eventTimer") Timer eventTimer) {
      ensureSchemaInitialized(c);
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
      try {
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
      } catch (Exception genError) {
        LOG.error("Generation failed for table {}", tableName, genError);
        generationFailures.inc();
        pendingDlq.add(
            FailureRecord.toJson(
                tableName, FailureRecord.OPERATION_GENERATION, row, genError));
      }

      drainPendingDlq(c::output);
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
        org.apache.beam.sdk.state.ValueState<List<Long>> activeTimestamps,
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
        org.apache.beam.sdk.state.ValueState<List<Long>> activeTimestamps,
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
            stickyShardId);
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
     * Generates a child row, populating FK columns from the ancestor chain (the tick cascade).
     *
     * <p>In the cascading tick model, every FK parent of a child is placed in the child's
     * ancestor chain by {@link com.google.cloud.teleport.v2.templates.utils.SchemaUtils#setSchemaDAG}
     * — so when we reach this method, {@code ancestorRows} is guaranteed to contain exactly one
     * row for each referenced parent table. A missing ancestor indicates a schema or DAG bug and
     * is treated as an unresolvable FK (log + skip).
     *
     * <p>The generator does not currently support schemas where a child has multiple FKs pointing
     * to the same parent table; such schemas are rejected at load time by {@link
     * com.google.cloud.teleport.v2.templates.utils.SchemaUtils#validateNoDuplicateFkTargets}.
     */
    private Row generateChildRow(
        DataGeneratorTable parentTable,
        Row parentRow,
        DataGeneratorTable childTable,
        Map<String, Row> ancestorRows,
        String stickyShardId) {
      Map<String, Object> columnValues = new HashMap<>();

      boolean resolvedAllFks = true;

      if (childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty()) {
        for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
          String targetTable = fk.referencedTable();
          Row source = ancestorRows.get(targetTable);
          if (source == null) {
            // In a well-formed schema every FK target is in the ancestor chain. A miss here means
            // either (a) the referenced table is not defined in the schema, or (b) DAG
            // construction did not place it upstream of this child. Both are bugs; fail loud on
            // this row rather than emit random FK values that would violate the constraint.
            LOG.warn(
                "Cannot resolve FK {} from {} -> {}: target table is not in the ancestor chain."
                    + " Check that the referenced table exists in the schema.",
                fk.name(),
                childTable.name(),
                targetTable);
            resolvedAllFks = false;
            break;
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

      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (DataGeneratorColumn col : childTable.columns()) {
        // skip=true columns are excluded from the emitted row schema entirely so the sink
        // writes its DEFAULT (or NULL) for them. PK columns can never be skipped — that's
        // enforced at schema-config load time in SchemaLoader.applyOverrides.
        if (col.skip()) {
          continue;
        }
        Object val = columnValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col)));
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

    private void scheduleEvent(
        long timestamp,
        LifecycleEvent event,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        org.apache.beam.sdk.state.ValueState<List<Long>> activeTimestamps,
        Timer eventTimer) {

      long snappedTimestamp = (timestamp / 1000) * 1000;

      List<LifecycleEvent> events = eventQueueState.get(snappedTimestamp).read();
      if (events == null) {
        events = new ArrayList<>();
      }
      events.add(event);
      eventQueueState.put(snappedTimestamp, events);

      List<Long> timestamps = activeTimestamps.read();
      if (timestamps == null) {
        timestamps = new ArrayList<>();
      }
      // Keep the list sorted ascending and duplicate-free so timestamps.get(0) is always the
      // earliest pending event and reads are O(log n) via binarySearch.
      int idx = Collections.binarySearch(timestamps, snappedTimestamp);
      if (idx < 0) {
        timestamps.add(-(idx + 1), snappedTimestamp);
        activeTimestamps.write(timestamps);
      }

      eventTimer.set(org.joda.time.Instant.ofEpochMilli(timestamps.get(0)));
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
        sb.append(canonicalizeValue(e.getValue()));
      }
      return sb.toString();
    }

    /**
     * Stable string form of a value for state-keying. {@code Object.toString()} is not
     * deterministic for {@code byte[]} or {@code ByteBuffer} (it returns the JVM-assigned object
     * id like {@code "[B@6a5fc7f7"}), which would make {@link #stateKeyOf} produce different keys
     * for the same logical PK across workers or restarts. Hex-encode byte arrays so the canonical
     * form is identical everywhere.
     */
    private static String canonicalizeValue(Object v) {
      if (v == null) {
        return "null";
      }
      if (v instanceof byte[]) {
        return "0x" + bytesToHex((byte[]) v);
      }
      if (v instanceof java.nio.ByteBuffer) {
        java.nio.ByteBuffer bb = ((java.nio.ByteBuffer) v).duplicate();
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return "0x" + bytesToHex(bytes);
      }
      return v.toString();
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private static String bytesToHex(byte[] bytes) {
      char[] out = new char[bytes.length * 2];
      for (int i = 0; i < bytes.length; i++) {
        int b = bytes[i] & 0xff;
        out[i * 2] = HEX_CHARS[b >>> 4];
        out[i * 2 + 1] = HEX_CHARS[b & 0x0f];
      }
      return new String(out);
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
        OnTimerContext c,
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<List<Long>> activeTimestamps,
        @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
        @TimerId("eventTimer") Timer eventTimer) {

      // Defensive: timer fires can race with a worker restart that skipped @StartBundle for this
      // bundle (rare but observed under autoscale). Initialize lazily so we never NPE on the
      // pendingDlq list below.
      if (pendingDlq == null) {
        pendingDlq = new ArrayList<>();
      }
      if (buffers == null) {
        buffers = new HashMap<>();
      }

      List<Long> timestamps = activeTimestamps.read();
      if (timestamps == null || timestamps.isEmpty()) {
        drainPendingDlq(c::output);
        return;
      }

      long now = System.currentTimeMillis();
      // The list is maintained in ascending order; once we hit a future timestamp we can stop.
      int firstFutureIdx = 0;

      for (Long ts : timestamps) {
        if (ts <= now) {
          List<LifecycleEvent> events = eventQueueState.get(ts).read();
          if (events != null) {
            // Fix B: sort events so children DELETE before their parents, and parents update
            // before children. All DELETEs are emitted after all UPDATEs/others to minimise
            // FK-dangling windows.
            List<LifecycleEvent> ordered = orderEventsForTick(events, tableMapState);
            for (LifecycleEvent event : ordered) {
              try {
                processEvent(event, activeKeys, eventTimer, tableMapState);
              } catch (Exception genError) {
                LOG.error(
                    "Lifecycle event generation failed for table {} ({})",
                    event.tableName,
                    event.type,
                    genError);
                generationFailures.inc();
                pendingDlq.add(
                    FailureRecord.toJson(event.tableName, event.type, null, genError));
              }
            }
            eventQueueState.remove(ts);
          }
          firstFutureIdx++;
        } else {
          break;
        }
      }

      // Drop all processed (past-or-now) timestamps from the head of the sorted list.
      timestamps = new ArrayList<>(timestamps.subList(firstFutureIdx, timestamps.size()));
      activeTimestamps.write(timestamps);

      if (!timestamps.isEmpty()) {
        eventTimer.set(org.joda.time.Instant.ofEpochMilli(timestamps.get(0)));
      }

      drainPendingDlq(c::output);
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
        if (col.skip()) {
          continue;
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col)));
        if (col.isPrimaryKey()) {
          values.add(pkValues.get(col.name()));
        } else if (uniqueColumns.contains(col.name())) {
          // Unique columns must NOT churn on UPDATE. Preserve the original inserted value from
          // state, same treatment PKs get via {@code pkValues}. Re-deriving would either
          // collide with another row's existing unique value (violating the constraint) or
          // change the row's logical identity between updates. {@code createReducedRow}
          // guarantees every unique column is written into {@code activeKeys} state at INSERT
          // time, so {@code originalRow} should always carry the field; if it doesn't, emit
          // {@code null} and let the sink surface the constraint error rather than silently
          // mutating the value.
          Object originalVal =
              (originalRow != null && originalRow.getSchema().hasField(col.name()))
                  ? originalRow.getValue(col.name())
                  : null;
          values.add(originalVal);
        } else if (fkColumns.contains(col.name())) {
          Object val =
              originalRow != null && originalRow.getSchema().hasField(col.name())
                  ? originalRow.getValue(col.name())
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
        if (col.skip()) {
          continue;
        }
        Schema.FieldType fieldType = DataGeneratorUtils.mapToBeamFieldType(col);
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
        if (col.skip()) {
          continue;
        }
        if (col.isPrimaryKey()
            || fkColumns.contains(col.name())
            || uniqueColumns.contains(col.name())) {
          schemaBuilder.addField(
              Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col)));
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
    public void finishBundle(FinishBundleContext c) {
      // Fix A / B: final flush must respect DAG order.
      flushInsertsInTopoOrder();
      flushUpdates();
      flushDeletesInReverseTopoOrder();

      // FinishBundle has no element-time so we stamp the DLQ records with wall-clock time. The
      // downstream WriteFailuresToGcs windowing only cares about the relative ordering and a
      // bounded skew, so processing-time-ish stamps are appropriate here.
      if (pendingDlq != null && !pendingDlq.isEmpty()) {
        Instant now = Instant.now();
        for (String record : pendingDlq) {
          c.output(record, now, GlobalWindow.INSTANCE);
        }
        pendingDlq.clear();
      }
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
        try {
          writer.write(batch, table, shardId, operation);
          batchesWritten.inc();
          recordsWritten.inc(batch.size());
          if (!shardId.isEmpty()) {
            Metrics.counter(BatchAndWriteFn.class, "recordsWritten_" + shardId).inc(batch.size());
          }
        } catch (Exception writeError) {
          // Sink rejected the whole batch. Emit one DLQ record per row so callers see exactly
          // which rows did not land. Do NOT rethrow — that would make Beam retry the entire
          // bundle (including non-failing buffers) and potentially loop forever on a poison
          // record. The DLQ + counter is the contract operators rely on for visibility.
          LOG.error(
              "Sink write failed for table {} ({}), {} rows routed to DLQ",
              tableName,
              operation,
              batch.size(),
              writeError);
          writeFailures.inc(batch.size());
          for (Row r : batch) {
            pendingDlq.add(FailureRecord.toJson(tableName, operation, r, writeError));
          }
        } finally {
          batch.clear();
        }
      }
      // Drop empty buffer entry to avoid linearly scanning it on subsequent flushes.
      if (bv != null && bv.rows.isEmpty()) {
        buffers.remove(bufferKey);
      }
    }

    /**
     * Completes a partial row by generating any missing columns through {@link
     * DataGeneratorUtils#generateValue}. Every column — including those under a unique constraint —
     * is produced uniformly via the same value generator; collisions on unique columns are handled
     * by the sink's upsert semantics, not by ad-hoc derivation here.
     */
    private Row completeRow(DataGeneratorTable table, Row partialRow, String shardIdHint) {
      boolean hasAllColumns = true;
      for (DataGeneratorColumn col : table.columns()) {
        if (col.skip()) {
          continue;
        }
        if (!partialRow.getSchema().hasField(col.name())) {
          hasAllColumns = false;
          break;
        }
      }

      if (hasAllColumns) {
        return partialRow;
      }

      Map<String, Object> currentValues = new HashMap<>();
      for (Schema.Field field : partialRow.getSchema().getFields()) {
        currentValues.put(field.getName(), partialRow.getValue(field.getName()));
      }

      List<Object> values = new ArrayList<>();
      Schema.Builder schemaBuilder = Schema.builder();

      for (DataGeneratorColumn col : table.columns()) {
        if (col.skip()) {
          continue;
        }
        Object val = currentValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col)));
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
     * One-shot materialization of the schema side-input and its derived topological ordering.
     *
     * <p>Side inputs are only accessible from {@code @ProcessElement} (not {@code @StartBundle} or
     * {@code @Setup}), so this must run on the first element. Once populated, both fields stay
     * consistent for the rest of the DoFn instance's lifetime — the underlying schema is a
     * singleton {@link PCollectionView} that doesn't change during pipeline execution. Subsequent
     * calls are no-ops.
     *
     * <p>Thread-safety note: Beam guarantees a DoFn instance is used by at most one thread at a
     * time, so no locking is required. The fields are {@code volatile} for defensive clarity.
     */
    private void ensureSchemaInitialized(ProcessContext c) {
      if (schema != null) {
        return;
      }
      DataGeneratorSchema loaded = c.sideInput(schemaView);
      List<String> topo = buildInsertTopoOrder(loaded);
      // Publish insertTopoOrder first so any reader that sees a non-null schema is guaranteed to
      // also see a populated topo order.
      this.insertTopoOrder = topo;
      this.schema = loaded;
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
