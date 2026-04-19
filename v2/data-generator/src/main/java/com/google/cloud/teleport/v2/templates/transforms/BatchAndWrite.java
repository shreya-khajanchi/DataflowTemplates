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
import com.google.cloud.teleport.v2.templates.utils.SeedUtils;
import com.google.cloud.teleport.v2.templates.writer.DataWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

  @org.apache.beam.sdk.coders.DefaultCoder(org.apache.beam.sdk.coders.SerializableCoder.class)
  public static class LifecycleEvent implements java.io.Serializable {
    public String id;
    public String type;
    public String tableName;

    public LifecycleEvent() {}

    public LifecycleEvent(String id, String type, String tableName) {
      this.id = id;
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

    static class BufferValue {
      final com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table;
      final java.util.List<Row> rows;

      BufferValue(com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table) {
        this.table = table;
        this.rows = new java.util.ArrayList<>();
      }
    }

    // Buffer: BufferKey -> BufferValue
    private transient Map<String, BufferValue> buffers;
    private transient int insertQps;
    private transient int updateQps;
    private transient int deleteQps;

    private static final org.slf4j.Logger LOG =
        org.slf4j.LoggerFactory.getLogger(BatchAndWriteFn.class);

    private final Counter insertsGenerated =
        Metrics.counter(BatchAndWriteFn.class, "insertsGenerated");
    private final Counter updatesGenerated =
        Metrics.counter(BatchAndWriteFn.class, "updatesGenerated");
    private final Counter deletesGenerated =
        Metrics.counter(BatchAndWriteFn.class, "deletesGenerated");
    private final Counter batchesWritten = Metrics.counter(BatchAndWriteFn.class, "batchesWritten");
    private final Counter recordsWritten = Metrics.counter(BatchAndWriteFn.class, "recordsWritten");

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
    private final StateSpec<
            MapState<String, com.google.cloud.teleport.v2.templates.model.DataGeneratorTable>>
        tableMapSpec =
            StateSpecs.map(
                org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                org.apache.beam.sdk.coders.SerializableCoder.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.class));

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
      com.google.cloud.teleport.v2.templates.DataGeneratorOptions genOptions =
          options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class);
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
        // Seed from multiple independent entropy sources (see SeedUtils javadoc). Using only
        // `new SecureRandom().nextLong()` is unsafe on Dataflow: autoscaled worker VMs are
        // cloned from the same image and can have correlated /dev/urandom state when @Setup
        // runs, previously producing identical non-PK Faker values across workers as well.
        this.faker = new Faker(new java.util.Random(SeedUtils.generate()));
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
      System.out.println("StartBundle called");
      this.buffers = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        @StateId("tableMapState")
            MapState<String, com.google.cloud.teleport.v2.templates.model.DataGeneratorTable>
                tableMapState,
        @TimerId("eventTimer") Timer eventTimer) {
      if (schema == null) {
        schema = c.sideInput(schemaView);
      }
      String key = c.element().getKey();
      String tableName = key.split("#")[0];
      Row row = c.element().getValue();
      com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table =
          schema.tables().get(tableName);

      if (table == null) {
        Metrics.counter(BatchAndWriteFn.class, "tableNotFound_" + tableName).inc();
        return;
      }

      tableMapState.put(tableName, table);

      String pkValue = getPkValue(row, table);
      if (pkValue != null) {
        String stateKey = tableName + ":" + pkValue;
        org.apache.beam.sdk.state.ReadableState<Row> state = activeKeys.get(stateKey);
        if (state != null && state.read() != null) {
          LOG.info(
              "Collision/Retry detected for key: {}. Skipping to maintain integrity.", stateKey);
          return;
        }
      }

      // Process this table and its children (Inserts)
      processTable(
          table,
          row,
          activeKeys,
          eventQueueState,
          activeTimestamps,
          eventTimer,
          0L,
          new HashMap<>());
    }

    /**
     * Processes a single table record: generates mutation, buffers/writes it, and recursively
     * processes children.
     */
    private void processTable(
        DataGeneratorTable table,
        Row row,
        MapState<String, Row> activeKeys,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        Timer eventTimer,
        long forcedDeleteTimestamp,
        Map<String, Row> ancestorRows) {
      insertsGenerated.inc();
      String tableName = table.name();

      // 0. Ensure Row has all columns (generate missing if needed)
      Row fullRow = completeRow(table, row);

      // 1. Buffer Row for current record
      String shardId = "";
      if (fullRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = fullRow.getString(Constants.SHARD_ID_COLUMN_NAME);
      } else if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)
          && logicalShardIds != null
          && !logicalShardIds.isEmpty()) {
        shardId =
            logicalShardIds.get(
                java.util.concurrent.ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
      }
      String bufferKey =
          tableName
              + "#"
              + (shardId.isEmpty() ? "default" : shardId)
              + "#"
              + Constants.MUTATION_INSERT;

      buffers.computeIfAbsent(bufferKey, k -> new BufferValue(table)).rows.add(fullRow);
      if (buffers.get(bufferKey).rows.size() >= batchSize) {
        flush(bufferKey);
      }

      // Calculate Lifecycle Events based on QPS ratios
      String pkValue = getPkValue(fullRow, table);
      long deleteTimestamp = 0;
      int numUpdates = 0;
      long now = System.currentTimeMillis();

      if (pkValue != null) {
        // Calculate ratios
        int tableInsertQps = table.insertQps();
        int tableUpdateQps = table.updateQps() > 0 ? table.updateQps() : updateQps;
        int tableDeleteQps = table.deleteQps() > 0 ? table.deleteQps() : deleteQps;

        double updateRatio = tableInsertQps > 0 ? (double) tableUpdateQps / tableInsertQps : 0;
        double deleteRatio = tableInsertQps > 0 ? (double) tableDeleteQps / tableInsertQps : 0;

        // Determine number of updates
        numUpdates = (int) updateRatio;
        double fractionalUpdate = updateRatio - numUpdates;
        if (java.util.concurrent.ThreadLocalRandom.current().nextDouble() < fractionalUpdate) {
          numUpdates++;
        }

        if (forcedDeleteTimestamp > 0) {
          deleteTimestamp = forcedDeleteTimestamp;
          // Cap number of updates so they happen before delete
          long maxUpdates = (forcedDeleteTimestamp - now - 1000) / 5000;
          numUpdates = (int) Math.min(numUpdates, Math.max(0, maxUpdates));
        } else {
          // Determine if delete should happen
          boolean hasDelete =
              java.util.concurrent.ThreadLocalRandom.current().nextDouble() < deleteRatio;
          if (hasDelete) {
            deleteTimestamp = now + 5000 * numUpdates + 10000;
          }
        }
      }

      Map<String, Row> updatedAncestorRows = new HashMap<>(ancestorRows);
      updatedAncestorRows.put(tableName, fullRow);

      // 2. Process Children FIRST (Cascading Generation)
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
                eventTimer,
                deleteTimestamp,
                updatedAncestorRows);
          } else {
            // Should not happen if schema DAG is built correctly
            Metrics.counter(BatchAndWriteFn.class, "childTableNotFound_" + childTableName).inc();
          }
        }
      }

      // 3. Schedule Lifecycle Events for THIS record LAST
      if (pkValue != null) {
        try {
          activeKeys.put(tableName + ":" + pkValue, createReducedRow(fullRow, table));

          // Schedule updates
          for (int i = 1; i <= numUpdates; i++) {
            scheduleEvent(
                now + 5000 * i,
                new LifecycleEvent(pkValue, Constants.MUTATION_UPDATE, tableName),
                eventQueueState,
                activeTimestamps,
                eventTimer);
          }

          // Schedule delete
          if (deleteTimestamp > 0) {
            scheduleEvent(
                deleteTimestamp,
                new LifecycleEvent(pkValue, Constants.MUTATION_DELETE, tableName),
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
        Timer eventTimer,
        long forcedDeleteTimestamp,
        Map<String, Row> ancestorRows) {
      // Calculate number of children to generate based on QPS ratio
      // Avoid division by zero
      double parentQps = Math.max(1, parentTable.insertQps());
      double childQps = childTable.insertQps();
      double ratio = childQps / parentQps;

      int numChildren = (int) ratio;
      // Probabilistic rounding for fractional ratio
      if (faker.random().nextDouble() < (ratio - numChildren)) {
        numChildren++;
      }

      for (int i = 0; i < numChildren; i++) {
        Row childRow = generateChildRow(parentTable, parentRow, childTable, ancestorRows);
        processTable(
            childTable,
            childRow,
            activeKeys,
            eventQueueState,
            activeTimestamps,
            eventTimer,
            forcedDeleteTimestamp,
            ancestorRows);
      }
    }

    private Row generateChildRow(
        DataGeneratorTable parentTable,
        Row parentRow,
        DataGeneratorTable childTable,
        Map<String, Row> ancestorRows) {
      // 1. Determine values for all columns
      // Foreign Keys must match Parent
      // Other columns generated via Faker

      Map<String, Object> columnValues = new HashMap<>();

      // A. Populate FKs from Parent
      // Find the ForeignKey definition that points to parentTable
      // We assume SchemaUtils.setSchemaDAG has set up the parent-child relationship
      // correctly
      // such that 'childTable' is indeed a child of 'parentTable'.
      // There might be multiple FKs? We try to match one.

      boolean fkFound = false;
      if (childTable.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
          String targetTable = fk.referencedTable();

          if (ancestorRows.containsKey(targetTable)) {
            Row ancestorRow = ancestorRows.get(targetTable);
            for (int i = 0; i < fk.keyColumns().size(); i++) {
              String childCol = fk.keyColumns().get(i);
              String targetCol = fk.referencedColumns().get(i);
              Object val = getFieldFromRow(ancestorRow, targetCol);
              columnValues.put(childCol, val);
            }
            fkFound = true;
          } else {
            // Fallback to looking in parentRow
            for (int i = 0; i < fk.keyColumns().size(); i++) {
              String childCol = fk.keyColumns().get(i);
              String parentCol = fk.referencedColumns().get(i);
              Object val = getFieldFromRow(parentRow, parentCol);
              columnValues.put(childCol, val);
            }
            fkFound = true;
          }
        }
      }

      if (!fkFound
          && childTable.interleavedInTable() != null
          && childTable.interleavedInTable().equals(parentTable.name())) {
        // For interleaved tables without explicit FK, assume child PK starts with
        // parent PK
        // columns.
        // Rely on column name matching to copy PKs from parent to child.
        for (String pk : parentTable.primaryKeys()) {
          Object val = getFieldFromRow(parentRow, pk);
          if (val != null) {
            columnValues.put(pk, val);
          }
        }
      }

      // B. Generate remaining columns
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (DataGeneratorColumn col : childTable.columns()) {
        Object val = columnValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
        }

        // Map to Beam Schema Type
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }

      // Propagate shard ID if present in parent
      String shardId = null;
      if (parentRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = parentRow.getString(Constants.SHARD_ID_COLUMN_NAME);
        schemaBuilder.addField(
            Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      }

      if (shardId != null) {
        values.add(shardId);
      }

      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
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

    private String getPkValue(Row row, DataGeneratorTable table) {
      StringBuilder sb = new StringBuilder();
      for (DataGeneratorColumn col : table.columns()) {
        if (col.isPrimaryKey()) {
          if (row.getSchema().hasField(col.name())) {
            Object val = row.getValue(col.name());
            if (sb.length() > 0) {
              sb.append("#");
            }
            sb.append(val != null ? val.toString() : "null");
          }
        }
      }
      return sb.length() > 0 ? sb.toString() : null;
    }

    private Map<String, Object> getNonPkValues(Row row, DataGeneratorTable table) {
      Map<String, Object> values = new HashMap<>();
      for (DataGeneratorColumn col : table.columns()) {
        if (!col.isPrimaryKey()) {
          if (row.getSchema().hasField(col.name())) {
            values.put(col.name(), row.getValue(col.name()));
          }
        }
      }
      return values;
    }

    private Object getFieldFromRow(Row row, String fieldName) {

      try {
        return row.getValue(fieldName);
      } catch (IllegalArgumentException e) {
        // Field not found. This effectively means we can't propagate this value.
        // If it's a generated non-PK column, we missed it.
        return null;
      }
    }

    @OnTimer("eventTimer")
    public void onTimer(
        @StateId("activeKeys") MapState<String, Row> activeKeys,
        @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
        @StateId("activeTimestamps")
            org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> activeTimestamps,
        @StateId("tableMapState")
            MapState<String, com.google.cloud.teleport.v2.templates.model.DataGeneratorTable>
                tableMapState,
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
            for (LifecycleEvent event : events) {
              processEvent(event, activeKeys, eventQueueState, eventTimer, tableMapState);
            }
            eventQueueState.remove(ts);
          }
          toRemove.add(ts);
        } else {
          break; // Stop early!
        }
      }

      timestamps.removeAll(toRemove);
      activeTimestamps.write(timestamps);

      if (!timestamps.isEmpty()) {
        eventTimer.set(org.joda.time.Instant.ofEpochMilli(timestamps.first()));
      }
    }

    private void processEvent(
        LifecycleEvent event,
        MapState<String, Row> activeKeys,
        MapState<Long, List<LifecycleEvent>> eventQueueState,
        Timer eventTimer,
        MapState<String, com.google.cloud.teleport.v2.templates.model.DataGeneratorTable>
            tableMapState) {
      String key = event.tableName + ":" + event.id;
      try {
        if (activeKeys.get(key).read() == null) {
          return; // Key no longer active
        }
      } catch (Exception e) {
        return;
      }

      com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table = null;
      try {
        table = tableMapState.get(event.tableName).read();
      } catch (Exception e) {
        // Ignore
      }
      if (table == null) {
        return;
      }

      if (Constants.MUTATION_UPDATE.equals(event.type)) {
        Row originalRow = activeKeys.get(event.tableName + ":" + event.id).read();
        Row updateRow = generateUpdateRow(event.id, table, originalRow);
        bufferMutation(event.tableName, updateRow, Constants.MUTATION_UPDATE, table);
        updatesGenerated.inc();
      } else if (Constants.MUTATION_DELETE.equals(event.type)) {
        Row deleteRow = generateDeleteRow(event.id, table);
        bufferMutation(event.tableName, deleteRow, Constants.MUTATION_DELETE, table);
        deletesGenerated.inc();

        try {
          activeKeys.remove(key);
        } catch (Exception e) {
        }
      }
    }

    Row generateUpdateRow(String id, DataGeneratorTable table, Row originalRow) {
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      Set<String> fkColumns = new HashSet<>();
      if (table.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : table.foreignKeys()) {
          fkColumns.addAll(fk.keyColumns());
        }
      }

      Set<String> uniqueColumns = new HashSet<>();
      if (table.uniqueKeys() != null) {
        for (com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey uk :
            table.uniqueKeys()) {
          uniqueColumns.addAll(uk.keyColumns());
        }
      }

      for (DataGeneratorColumn col : table.columns()) {
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        if (col.isPrimaryKey()) {
          values.add(parsePkValue(id, col));
        } else if (fkColumns.contains(col.name()) || uniqueColumns.contains(col.name())) {
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

    private Row generateDeleteRow(String id, DataGeneratorTable table) {
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn col : table.columns()) {
        org.apache.beam.sdk.schemas.Schema.FieldType fieldType =
            DataGeneratorUtils.mapToBeamFieldType(col.logicalType());
        if (col.isPrimaryKey()) {
          schemaBuilder.addField(
              org.apache.beam.sdk.schemas.Schema.Field.of(col.name(), fieldType));
          values.add(parsePkValue(id, col));
        } else {
          schemaBuilder.addField(
              org.apache.beam.sdk.schemas.Schema.Field.nullable(col.name(), fieldType));
          values.add(null);
        }
      }
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Object parsePkValue(String id, DataGeneratorColumn col) {
      if ("INT".equalsIgnoreCase(col.originalType())
          || "BIGINT".equalsIgnoreCase(col.originalType())) {
        return Long.parseLong(id);
      }
      return id;
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

      Set<String> uniqueColumns = new HashSet<>();
      if (table.uniqueKeys() != null) {
        for (com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey uk :
            table.uniqueKeys()) {
          uniqueColumns.addAll(uk.keyColumns());
        }
      }

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
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private void bufferMutation(
        String tableName,
        Row row,
        String operation,
        com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table) {
      String shardId = "";
      if (row.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = row.getString(Constants.SHARD_ID_COLUMN_NAME);
      } else if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType)
          && logicalShardIds != null
          && !logicalShardIds.isEmpty()) {
        shardId =
            logicalShardIds.get(
                java.util.concurrent.ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
      }
      String bufferKey =
          tableName + "#" + (shardId.isEmpty() ? "default" : shardId) + "#" + operation;

      buffers.computeIfAbsent(bufferKey, k -> new BufferValue(table)).rows.add(row);
      if (buffers.get(bufferKey).rows.size() >= batchSize) {
        flush(bufferKey);
      }
    }

    @FinishBundle
    public void finishBundle() {
      System.out.println("FinishBundle called");
      for (String bufferKey : buffers.keySet()) {
        BufferValue bv = buffers.get(bufferKey);
        if (bv != null && !bv.rows.isEmpty()) {
          flush(bufferKey);
        }
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
      com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table =
          bv != null ? bv.table : null;

      if (batch != null && !batch.isEmpty()) {
        writer.write(batch, table, shardId, operation);
        batchesWritten.inc();
        recordsWritten.inc(batch.size());
        if (!shardId.isEmpty()) {
          Metrics.counter(BatchAndWriteFn.class, "recordsWritten_" + shardId).inc(batch.size());
        }
        batch.clear();
      }
    }

    private Row completeRow(DataGeneratorTable table, Row partialRow) {
      boolean hasAllColumns = true;
      for (DataGeneratorColumn col : table.columns()) {
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
        Object val = currentValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }

      // Preserve shard ID if present
      String shardId = null;
      if (partialRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
        shardId = partialRow.getString(Constants.SHARD_ID_COLUMN_NAME);
        schemaBuilder.addField(
            Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      }

      if (shardId != null) {
        values.add(shardId);
      }

      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }
  }
}
