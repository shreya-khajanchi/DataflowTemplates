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
package com.google.cloud.teleport.v2.templates.dofn;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.mysql.MySqlDataWriter;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.spanner.SpannerDataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import com.google.common.annotations.VisibleForTesting;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
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
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful {@link DoFn} that completes partial generated rows, recursively generates child rows
 * across the schema DAG, batches mutations by {@code (table, shard, operation)}, and dispatches
 * them to the configured {@link DataWriter}.
 *
 * <p>Lifecycle events (UPDATEs and an optional DELETE) are scheduled against a processing-time
 * {@link Timer} so per-row update/delete QPS targets are honoured at the configured cadence.
 *
 * <p>Output is a {@code PCollection<String>} of JSON-encoded {@link FailureRecord}s — one per row
 * that could not be generated or that the sink rejected. Sink-write failures do not crash the
 * bundle; the rows are diverted to the dead-letter output and pipeline progress continues.
 *
 * <p>Pure helpers (row construction, primary-key keying) live in {@link RowAssembler}.
 *
 * <p>Ordering guarantees:
 *
 * <ul>
 *   <li><b>INSERT</b> buffers are flushed in topological (parent&rarr;child) order.
 *   <li><b>DELETE</b> buffers are flushed in reverse-topological (child&rarr;parent) order.
 *   <li><b>UPDATE</b> events have no ordering requirement among themselves.
 * </ul>
 */
public class BatchAndWriteFn extends DoFn<KV<String, Row>, String> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchAndWriteFn.class);

  /** Default batch size used when the constructor is given {@code null}. */
  static final int DEFAULT_BATCH_SIZE = 100;

  /** Slack between successive UPDATEs and the trailing DELETE for a given row, in milliseconds. */
  private static final long UPDATE_INTERVAL_MS = 5000L;

  /** Buffer applied when capping {@code numUpdates} against an ancestor's forced delete. */
  private static final long ANCESTOR_DELETE_SLACK_MS = UPDATE_INTERVAL_MS;

  // Constructor-supplied configuration. Pipeline options are intentionally NOT referenced here.
  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final int batchSize;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  // Transient per-worker state.
  private transient DataWriter writer;
  private transient Faker faker;
  private transient volatile DataGeneratorSchema schema;
  private transient volatile List<String> insertTopoOrder;
  private transient volatile Map<String, Integer> topoIndex;
  private transient List<String> logicalShardIds;
  private transient Map<String, BufferValue> buffers;
  private transient List<String> pendingDlq;

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
  private final Counter writeFailures = Metrics.counter(BatchAndWriteFn.class, "writeFailures");
  private final Counter generationFailures =
      Metrics.counter(BatchAndWriteFn.class, "generationFailures");

  @StateId("activeKeys")
  private final StateSpec<MapState<String, Row>> activeKeysSpec =
      StateSpecs.map(StringUtf8Coder.of(), SerializableCoder.of(Row.class));

  @StateId("eventQueue")
  private final StateSpec<MapState<Long, List<LifecycleEvent>>> eventQueueSpec =
      StateSpecs.map(
          VarLongCoder.of(), ListCoder.of(SerializableCoder.of(LifecycleEvent.class)));

  /**
   * Pending event timestamps (snapped to the second) for this key, in ascending order. We use a
   * sorted {@code List<Long>} (with {@code ListCoder + VarLongCoder}) rather than a {@code
   * TreeSet} so the order is preserved verbatim through checkpoint / restore.
   */
  @StateId("activeTimestamps")
  private final StateSpec<ValueState<List<Long>>> activeTimestampsSpec =
      StateSpecs.value(ListCoder.of(VarLongCoder.of()));

  @StateId("tableMapState")
  private final StateSpec<MapState<String, DataGeneratorTable>> tableMapSpec =
      StateSpecs.map(StringUtf8Coder.of(), SerializableCoder.of(DataGeneratorTable.class));

  @TimerId("eventTimer")
  private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  /**
   * @param sinkType which sink writer implementation to instantiate
   * @param sinkOptionsPath path/URI to the sink-specific configuration document
   * @param batchSize maximum rows buffered per {@code (table, shard, op)}; {@code null} or
   *     non-positive falls back to {@link #DEFAULT_BATCH_SIZE}
   * @param schemaView side input carrying the {@link DataGeneratorSchema}
   */
  public BatchAndWriteFn(
      SinkType sinkType,
      String sinkOptionsPath,
      Integer batchSize,
      PCollectionView<DataGeneratorSchema> schemaView) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = (batchSize != null && batchSize > 0) ? batchSize : DEFAULT_BATCH_SIZE;
    this.schemaView = schemaView;
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  @Setup
  public void setup() {
    // Reset lazily-populated caches so each DoFn instance starts clean. Repopulated on the
    // first @ProcessElement call (side inputs are only accessible there).
    this.schema = null;
    this.insertTopoOrder = null;
    this.topoIndex = null;
    if (writer == null) {
      writer = createWriter(sinkType, sinkOptionsPath);
      if (sinkType == SinkType.MYSQL) {
        try {
          ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
          List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkOptionsPath);
          this.logicalShardIds =
              shards.stream().map(Shard::getLogicalShardId).collect(Collectors.toList());
        } catch (Exception e) {
          throw new RuntimeException("Failed to read shards from " + sinkOptionsPath, e);
        }
      }
    }
    if (faker == null) {
      faker = new Faker(new Random(new SecureRandom().nextLong()));
    }
  }

  @VisibleForTesting
  protected DataWriter createWriter(SinkType type, String configPath) {
    switch (type) {
      case MYSQL:
        return new MySqlDataWriter(configPath);
      case SPANNER:
        return new SpannerDataWriter(configPath);
      default:
        throw new IllegalArgumentException("Unsupported sink type: " + type);
    }
  }

  @StartBundle
  public void startBundle() {
    this.buffers = new HashMap<>();
    this.pendingDlq = new ArrayList<>();
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("activeKeys") MapState<String, Row> activeKeys,
      @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
      @StateId("activeTimestamps") ValueState<List<Long>> activeTimestamps,
      @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
      @TimerId("eventTimer") Timer eventTimer) {

    ensureSchemaInitialized(c);

    String key = c.element().getKey();
    String tableName = key.split("#")[0];
    Row row = c.element().getValue();
    DataGeneratorTable table = schema.tables().get(tableName);

    if (table == null) {
      Metrics.counter(BatchAndWriteFn.class, "tableNotFound_" + tableName).inc();
      drainPendingDlq(c::output);
      return;
    }

    tableMapState.put(tableName, table);

    LinkedHashMap<String, Object> pkMap = RowAssembler.pkValuesOf(row, table);
    if (!pkMap.isEmpty()) {
      String stateKey = RowAssembler.stateKeyOf(tableName, pkMap);
      Row existing = activeKeys.get(stateKey).read();
      if (existing != null) {
        LOG.info("Collision/retry detected for key {}; skipping to maintain integrity.", stateKey);
        drainPendingDlq(c::output);
        return;
      }
    }

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
          FailureRecord.toJson(tableName, FailureRecord.OPERATION_GENERATION, row, genError));
    }

    drainPendingDlq(c::output);
  }

  @OnTimer("eventTimer")
  public void onTimer(
      OnTimerContext c,
      @StateId("activeKeys") MapState<String, Row> activeKeys,
      @StateId("eventQueue") MapState<Long, List<LifecycleEvent>> eventQueueState,
      @StateId("activeTimestamps") ValueState<List<Long>> activeTimestamps,
      @StateId("tableMapState") MapState<String, DataGeneratorTable> tableMapState,
      @TimerId("eventTimer") Timer eventTimer) {

    // Defensive: timer fires can race with worker restart that skipped @StartBundle for this
    // bundle (rare but observed under autoscale). Lazy-init so we never NPE below.
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
    int firstFutureIdx = 0;
    for (Long ts : timestamps) {
      if (ts > now) {
        break;
      }
      List<LifecycleEvent> events = eventQueueState.get(ts).read();
      if (events != null) {
        for (LifecycleEvent event : orderEventsForTick(events, tableMapState)) {
          try {
            processEvent(event, activeKeys, tableMapState);
          } catch (Exception genError) {
            LOG.error(
                "Lifecycle event generation failed for table {} ({})",
                event.tableName,
                event.type,
                genError);
            generationFailures.inc();
            pendingDlq.add(FailureRecord.toJson(event.tableName, event.type, null, genError));
          }
        }
        eventQueueState.remove(ts);
      }
      firstFutureIdx++;
    }

    timestamps = new ArrayList<>(timestamps.subList(firstFutureIdx, timestamps.size()));
    activeTimestamps.write(timestamps);
    if (!timestamps.isEmpty()) {
      eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
    }

    drainPendingDlq(c::output);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    flushInsertsInTopoOrder();
    flushUpdates();
    flushDeletesInReverseTopoOrder();

    // FinishBundle has no element timestamp; stamp DLQ records with wall-clock time.
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

  /**
   * One-shot materialization of the schema side-input and its derived topological ordering.
   * Subsequent calls are no-ops. Beam guarantees a DoFn instance is used by at most one thread at
   * a time, so no locking is needed; the {@code volatile} fields are defensive.
   */
  private void ensureSchemaInitialized(ProcessContext c) {
    if (schema != null) {
      return;
    }
    DataGeneratorSchema loaded = c.sideInput(schemaView);
    List<String> topo = buildInsertTopoOrder(loaded);
    // Publish the topo state before publishing the schema so a reader that sees a non-null
    // schema is guaranteed to also see a populated topo order.
    this.insertTopoOrder = topo;
    this.topoIndex = buildTopoIndex(topo);
    this.schema = loaded;
  }

  private void drainPendingDlq(Consumer<String> sink) {
    if (pendingDlq == null || pendingDlq.isEmpty()) {
      return;
    }
    for (String record : pendingDlq) {
      sink.accept(record);
    }
    pendingDlq.clear();
  }

  // ===========================================================================
  // INSERT path: processTable + recursive child generation
  // ===========================================================================

  /**
   * Recursively processes a single table record: completes the row, buffers the INSERT, recurses
   * into child tables, then schedules UPDATE/DELETE events.
   *
   * @param forcedDeleteTimestamp if &gt; 0, a delete MUST happen at this wall-clock time (cascaded
   *     from a parent). Child updates are capped so the last update precedes this.
   * @param earliestAncestorDelete min wall-clock delete time across ALL ancestor tables, or {@link
   *     Long#MAX_VALUE} when no ancestor has a scheduled delete.
   * @param stickyShardId if non-null, the shard id propagated from root; otherwise picked here.
   */
  private void processTable(
      DataGeneratorTable table,
      Row row,
      MapState<String, Row> activeKeys,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      long forcedDeleteTimestamp,
      long earliestAncestorDelete,
      String stickyShardId,
      Map<String, Row> ancestorRows) {

    insertsGenerated.inc();
    String tableName = table.name();
    tableMapState.put(tableName, table);

    String resolvedShardId = resolveShardId(row, stickyShardId);
    Row fullRow = RowAssembler.completeRow(table, row, resolvedShardId, faker);

    LinkedHashMap<String, Object> pkMap = RowAssembler.pkValuesOf(fullRow, table);
    if (!pkMap.isEmpty()) {
      activeKeys.put(
          RowAssembler.stateKeyOf(tableName, pkMap), RowAssembler.createReducedRow(fullRow, table));
    }

    bufferRow(tableName, fullRow, Constants.MUTATION_INSERT, table, resolvedShardId);

    long now = System.currentTimeMillis();
    long deleteTimestamp = 0L;
    int numUpdates = 0;

    if (!pkMap.isEmpty()) {
      int tableInsertQps = table.insertQps();
      int tableUpdateQps = table.updateQps();
      int tableDeleteQps = table.deleteQps();

      double updateRatio = tableInsertQps > 0 ? (double) tableUpdateQps / tableInsertQps : 0.0;
      double deleteRatio = tableInsertQps > 0 ? (double) tableDeleteQps / tableInsertQps : 0.0;

      numUpdates = (int) updateRatio;
      double fractionalUpdate = updateRatio - numUpdates;
      if (ThreadLocalRandom.current().nextDouble() < fractionalUpdate) {
        numUpdates++;
      }

      if (forcedDeleteTimestamp > 0) {
        deleteTimestamp = forcedDeleteTimestamp;
      } else if (ThreadLocalRandom.current().nextDouble() < deleteRatio) {
        deleteTimestamp = now + UPDATE_INTERVAL_MS * numUpdates + UPDATE_INTERVAL_MS;
      }

      // Cap updates so the last one lands strictly before the earliest ancestor delete AND our
      // own forced delete (whichever is tighter).
      long myDeleteBound = deleteTimestamp > 0 ? deleteTimestamp : Long.MAX_VALUE;
      long effectiveDeleteBound = Math.min(myDeleteBound, earliestAncestorDelete);
      if (effectiveDeleteBound < Long.MAX_VALUE) {
        long budget = effectiveDeleteBound - now - ANCESTOR_DELETE_SLACK_MS;
        long maxUpdates = budget > 0 ? budget / UPDATE_INTERVAL_MS : 0;
        numUpdates = (int) Math.min(numUpdates, Math.max(0, maxUpdates));
      }
    }

    Map<String, Row> updatedAncestorRows = new HashMap<>(ancestorRows);
    updatedAncestorRows.put(tableName, fullRow);

    long childEarliestAncestorDelete =
        deleteTimestamp > 0
            ? Math.min(earliestAncestorDelete, deleteTimestamp)
            : earliestAncestorDelete;

    if (table.childTables() != null) {
      for (String childTableName : table.childTables()) {
        DataGeneratorTable childTable = schema.tables().get(childTableName);
        if (childTable == null) {
          Metrics.counter(BatchAndWriteFn.class, "childTableNotFound_" + childTableName).inc();
          continue;
        }
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
      }
    }

    if (!pkMap.isEmpty()) {
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
    }
  }

  private void generateAndWriteChildren(
      DataGeneratorTable parentTable,
      Row parentRow,
      DataGeneratorTable childTable,
      MapState<String, Row> activeKeys,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
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
      Row childRow = generateChildRow(parentTable, parentRow, childTable, ancestorRows, stickyShardId);
      if (childRow == null) {
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
   * Generates a child row, populating FK columns from the ancestor chain. In a well-formed schema
   * every FK target is in the ancestor chain (placed there by {@code SchemaUtils.setSchemaDAG}); a
   * missing entry is a schema/DAG bug and the row is skipped rather than emitted with random FK
   * values.
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
        Row source = ancestorRows.get(fk.referencedTable());
        if (source == null) {
          LOG.warn(
              "Cannot resolve FK {} from {} -> {}: target table is not in the ancestor chain.",
              fk.name(),
              childTable.name(),
              fk.referencedTable());
          resolvedAllFks = false;
          break;
        }
        for (int i = 0; i < fk.keyColumns().size(); i++) {
          columnValues.put(
              fk.keyColumns().get(i), getFieldFromRow(source, fk.referencedColumns().get(i)));
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
      // Interleaved child without explicit FK: assume PK starts with parent PK columns.
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
      if (col.isSkipped()) {
        continue;
      }
      Object val = columnValues.get(col.name());
      if (val == null) {
        val = DataGeneratorUtils.generateValue(col, faker);
      }
      schemaBuilder.addField(
          Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      values.add(val);
    }

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

  private Object getFieldFromRow(Row row, String fieldName) {
    try {
      return row.getValue(fieldName);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // ===========================================================================
  // Lifecycle event scheduling and dispatch
  // ===========================================================================

  private void scheduleEvent(
      long timestamp,
      LifecycleEvent event,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
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
    int idx = Collections.binarySearch(timestamps, snappedTimestamp);
    if (idx < 0) {
      timestamps.add(-(idx + 1), snappedTimestamp);
      activeTimestamps.write(timestamps);
    }
    eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
  }

  /**
   * Sort lifecycle events scheduled for the same wall-clock second so:
   *
   * <ol>
   *   <li>UPDATEs come first (shallowest first by topological position),
   *   <li>DELETEs come last and deepest-first, so children are removed before their parents.
   * </ol>
   */
  private List<LifecycleEvent> orderEventsForTick(
      List<LifecycleEvent> events, MapState<String, DataGeneratorTable> tableMapState) {
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
          return isDelete(a)
              ? Integer.compare(depthB, depthA) // deepest first for DELETE
              : Integer.compare(depthA, depthB); // shallowest first otherwise
        };
    List<LifecycleEvent> copy = new ArrayList<>(events);
    copy.sort(cmp);
    return copy;
  }

  private boolean isDelete(LifecycleEvent e) {
    return Constants.MUTATION_DELETE.equals(e.type);
  }

  /** Topological-order index serves as a depth proxy: roots first, leaves last. */
  private int depthOf(String tableName, MapState<String, DataGeneratorTable> tableMapState) {
    if (topoIndex != null && topoIndex.containsKey(tableName)) {
      return topoIndex.get(tableName);
    }
    if (schema != null && insertTopoOrder == null) {
      // Defensive: lazy rebuild if a prior worker's topo state was lost.
      insertTopoOrder = buildInsertTopoOrder(schema);
      topoIndex = buildTopoIndex(insertTopoOrder);
      Integer idx = topoIndex.get(tableName);
      return idx == null ? 0 : idx;
    }
    return 0;
  }

  private void processEvent(
      LifecycleEvent event,
      MapState<String, Row> activeKeys,
      MapState<String, DataGeneratorTable> tableMapState) {

    String stateKey = RowAssembler.stateKeyOf(event.tableName, event.pkValues);
    Row originalRow = activeKeys.get(stateKey).read();
    if (originalRow == null) {
      return; // Key no longer active.
    }

    DataGeneratorTable table = tableMapState.get(event.tableName).read();
    if (table == null) {
      return;
    }

    String shardId =
        originalRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)
            ? originalRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : "";

    if (Constants.MUTATION_UPDATE.equals(event.type)) {
      Row updateRow = RowAssembler.generateUpdateRow(event.pkValues, table, originalRow, faker);
      bufferRow(event.tableName, updateRow, Constants.MUTATION_UPDATE, table, shardId);
      updatesGenerated.inc();
    } else if (Constants.MUTATION_DELETE.equals(event.type)) {
      Row deleteRow = RowAssembler.generateDeleteRow(event.pkValues, table);
      bufferRow(event.tableName, deleteRow, Constants.MUTATION_DELETE, table, shardId);
      deletesGenerated.inc();
      activeKeys.remove(stateKey);
    }
  }

  // ===========================================================================
  // Buffering & flushing
  // ===========================================================================

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
    if (shardId.isEmpty() && sinkType == SinkType.MYSQL && hasShards()) {
      shardId = logicalShardIds.get(ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
    }

    String bufferKey =
        tableName + "#" + (shardId.isEmpty() ? "default" : shardId) + "#" + operation;
    buffers.computeIfAbsent(bufferKey, k -> new BufferValue(table)).rows.add(row);

    if (buffers.get(bufferKey).rows.size() >= batchSize) {
      if (Constants.MUTATION_INSERT.equals(operation)) {
        flushInsertsInTopoOrder();
      } else if (Constants.MUTATION_DELETE.equals(operation)) {
        flushDeletesInReverseTopoOrder();
      } else {
        flush(bufferKey);
      }
    }
  }

  private boolean hasShards() {
    return logicalShardIds != null && !logicalShardIds.isEmpty();
  }

  private String resolveShardId(Row row, String inherited) {
    if (inherited != null && !inherited.isEmpty()) {
      return inherited;
    }
    if (row.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
      return row.getString(Constants.SHARD_ID_COLUMN_NAME);
    }
    if (sinkType == SinkType.MYSQL && hasShards()) {
      return logicalShardIds.get(ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
    }
    return "";
  }

  private void flushInsertsInTopoOrder() {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
    for (String table : order) {
      flushByTableAndOp(table, Constants.MUTATION_INSERT);
    }
    flushAllByOp(Constants.MUTATION_INSERT);
  }

  private void flushDeletesInReverseTopoOrder() {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    List<String> order = insertTopoOrder != null ? insertTopoOrder : Collections.emptyList();
    for (int i = order.size() - 1; i >= 0; i--) {
      flushByTableAndOp(order.get(i), Constants.MUTATION_DELETE);
    }
    flushAllByOp(Constants.MUTATION_DELETE);
  }

  private void flushUpdates() {
    if (buffers == null || buffers.isEmpty()) {
      return;
    }
    flushAllByOp(Constants.MUTATION_UPDATE);
  }

  /** Belt-and-suspenders: flush any leftover buffer for {@code op} not in the topo order. */
  private void flushAllByOp(String op) {
    for (String bufferKey : new ArrayList<>(buffers.keySet())) {
      String[] parts = bufferKey.split("#");
      if (parts.length >= 3 && op.equals(parts[2])) {
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

  /**
   * Writes one buffer to the sink. Sink-write failures are caught and routed to the DLQ so the
   * bundle continues; rethrowing would make Beam retry the entire bundle (including unrelated
   * buffers) and potentially loop forever on a poison record.
   */
  private void flush(String bufferKey) {
    String[] parts = bufferKey.split("#");
    String tableName = parts[0];
    String shardId = parts.length > 1 ? parts[1] : "";
    String operation = parts.length > 2 ? parts[2] : Constants.MUTATION_INSERT;
    if ("default".equals(shardId)) {
      shardId = "";
    }

    BufferValue bv = buffers.get(bufferKey);
    List<Row> batch = bv != null ? bv.rows : null;
    DataGeneratorTable table = bv != null ? bv.table : null;
    if (batch == null || batch.isEmpty()) {
      if (bv != null && bv.rows.isEmpty()) {
        buffers.remove(bufferKey);
      }
      return;
    }

    int maxShardConnections = Constants.DEFAULT_JDBC_POOL_SIZE;
    try {
      switch (operation) {
        case Constants.MUTATION_INSERT:
          writer.insert(batch, table, shardId, maxShardConnections);
          break;
        case Constants.MUTATION_UPDATE:
          writer.update(batch, table, shardId, maxShardConnections);
          break;
        case Constants.MUTATION_DELETE:
          writer.delete(batch, table, shardId, maxShardConnections);
          break;
        default:
          throw new IllegalStateException("Unknown operation: " + operation);
      }
      batchesWritten.inc();
      recordsWritten.inc(batch.size());
      if (!shardId.isEmpty()) {
        Metrics.counter(BatchAndWriteFn.class, "recordsWritten_" + shardId).inc(batch.size());
      }
      Metrics.counter(BatchAndWriteFn.class, operation.toLowerCase() + "_" + tableName)
          .inc(batch.size());
    } catch (Exception writeError) {
      LOG.error(
          "Sink write failed for table {} ({}); {} rows routed to DLQ",
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
      buffers.remove(bufferKey);
    }
  }

  // ===========================================================================
  // Topological order
  // ===========================================================================

  /**
   * Topological order of tables: roots first (sorted by name), BFS through {@code childTables()}.
   * Tables not reachable from any root are appended at the end so they still get flushed.
   */
  @VisibleForTesting
  static List<String> buildInsertTopoOrder(DataGeneratorSchema schema) {
    Map<String, DataGeneratorTable> tables = schema.tables();
    List<String> order = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    Deque<String> queue = new ArrayDeque<>();

    List<String> roots =
        tables.values().stream()
            .filter(DataGeneratorTable::isRoot)
            .map(DataGeneratorTable::name)
            .sorted()
            .collect(Collectors.toList());
    queue.addAll(roots);

    while (!queue.isEmpty()) {
      String name = queue.poll();
      if (!seen.add(name)) {
        continue;
      }
      order.add(name);
      DataGeneratorTable t = tables.get(name);
      if (t != null && t.childTables() != null) {
        List<String> sortedChildren = new ArrayList<>(t.childTables());
        Collections.sort(sortedChildren);
        queue.addAll(sortedChildren);
      }
    }
    for (String name : new TreeSet<>(tables.keySet())) {
      if (seen.add(name)) {
        order.add(name);
      }
    }
    return order;
  }

  private static Map<String, Integer> buildTopoIndex(List<String> topoOrder) {
    Map<String, Integer> index = new HashMap<>();
    for (int i = 0; i < topoOrder.size(); i++) {
      index.put(topoOrder.get(i), i);
    }
    return index;
  }

  /** Per-{@code (table, shard, op)} buffer of pending rows plus the table's metadata. */
  private static final class BufferValue {
    final DataGeneratorTable table;
    final List<Row> rows;

    BufferValue(DataGeneratorTable table) {
      this.table = table;
      this.rows = new ArrayList<>();
    }
  }
}
