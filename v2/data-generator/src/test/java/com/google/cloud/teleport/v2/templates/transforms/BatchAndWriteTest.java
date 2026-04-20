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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.cloud.teleport.v2.templates.writer.DataWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BatchAndWriteTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DoFn<KV<String, Row>, Void>.ProcessContext mockContext;

  @Test
  @Ignore("DirectRunner creates new DoFn instance per element, cannot test batch > 1 this way")
  public void testBatchAndWriteFlushesOnBatchSize() throws IOException {
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getBatchSize()).thenReturn(2);
    when(options.getSinkType()).thenReturn(DataGeneratorOptions.SinkType.SPANNER);
    when(options.getSinkOptions()).thenReturn("{\"type\":\"spanner\"}");

    DataWriter mockWriter = mock(DataWriter.class);

    // Define strict schema for Row
    Schema schema = Schema.builder().addField("id", Schema.FieldType.INT64).build();

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("name")
                        .logicalType(LogicalType.STRING)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("STRING")
                        .isPrimaryKey(false)
                        .build()))
            .primaryKeys(ImmutableList.of("id"))
            .insertQps(1)
            .isRoot(true)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    // Create Schema
    DataGeneratorSchema dataGeneratorSchema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of(table.name(), table))
            .build();

    // Create PCollectionView within the pipeline context
    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline
            .apply("CreateSchema", Create.of(dataGeneratorSchema))
            .apply("ViewAsSingleton", View.asSingleton());

    Row row1 = Row.withSchema(schema).addValue(1L).build();
    Row row2 = Row.withSchema(schema).addValue(2L).build();
    Row row3 = Row.withSchema(schema).addValue(3L).build();

    // Create input PCollection
    PCollection<KV<String, Row>> input =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of(KV.of(table.name(), row1), new Instant(0)),
                TimestampedValue.of(KV.of(table.name(), row2), new Instant(0)),
                TimestampedValue.of(KV.of(table.name(), row3), new Instant(0))));

    // Setup the DoFn
    TestBatchAndWriteFn fn = new TestBatchAndWriteFn(options, schemaView, mockWriter);

    // Apply the transform
    input.apply(ParDo.of(fn).withSideInputs(schemaView));

    // Run the pipeline
    pipeline.run();

    // Verify writer was called.
    // Batch size is 2, so 3 elements should result in two calls to write.
    verify(mockWriter, times(2)).write(anyList(), any());
  }

  static class TestBatchAndWriteFn extends BatchAndWrite.BatchAndWriteFn {

    private static DataWriter staticWriter;

    public TestBatchAndWriteFn(
        DataGeneratorOptions options,
        PCollectionView<DataGeneratorSchema> schemaView,
        DataWriter writer) {
      super(
          options.getSinkType().name(),
          options.getSinkOptions(),
          options.getBatchSize(),
          schemaView);
      staticWriter = writer;
    }

    @Override
    public void setup(org.apache.beam.sdk.options.PipelineOptions options) {
      com.google.cloud.teleport.v2.templates.DataGeneratorOptions genOptions =
          options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class);
      try {
        java.lang.reflect.Field f1 =
            com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite.BatchAndWriteFn.class
                .getDeclaredField("insertQps");
        f1.setAccessible(true);
        f1.set(this, genOptions.getInsertQps());

        java.lang.reflect.Field f2 =
            com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite.BatchAndWriteFn.class
                .getDeclaredField("updateQps");
        f2.setAccessible(true);
        f2.set(this, genOptions.getUpdateQps());

        java.lang.reflect.Field f3 =
            com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite.BatchAndWriteFn.class
                .getDeclaredField("deleteQps");
        f3.setAccessible(true);
        f3.set(this, genOptions.getDeleteQps());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      this.writer = staticWriter;
      this.faker = new Faker();
    }

    public void setWriter(DataWriter writer) {
      try {
        java.lang.reflect.Field writerField =
            BatchAndWrite.BatchAndWriteFn.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        writerField.set(this, writer);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void setFaker(Faker faker) {
      try {
        java.lang.reflect.Field fakerField =
            BatchAndWrite.BatchAndWriteFn.class.getDeclaredField("faker");
        fakerField.setAccessible(true);
        fakerField.set(this, faker);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testCascadingGeneration() throws Exception {
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getBatchSize()).thenReturn(10);
    when(options.getSinkType()).thenReturn(DataGeneratorOptions.SinkType.SPANNER);
    when(options.getSinkOptions()).thenReturn("{\"type\":\"spanner\"}");
    when(options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class))
        .thenReturn(options);
    DataWriter mockWriter = mock(DataWriter.class);

    // Parent Table (Root)
    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Singers")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("SingerId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("Name")
                        .logicalType(LogicalType.STRING)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("STRING")
                        .isPrimaryKey(false)
                        .build()))
            .primaryKeys(ImmutableList.of("SingerId"))
            .insertQps(1)
            .isRoot(true)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    // Child Table
    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Albums")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("SingerId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("AlbumId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("Title")
                        .logicalType(LogicalType.STRING)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("STRING")
                        .isPrimaryKey(false)
                        .build()))
            .primaryKeys(ImmutableList.of("SingerId", "AlbumId"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("FK_Albums_Singers")
                        .referencedTable("Singers")
                        .keyColumns(ImmutableList.of("SingerId"))
                        .referencedColumns(ImmutableList.of("SingerId"))
                        .build()))
            .insertQps(2) // 2x Parent QPS -> 2 Children per Parent
            .isRoot(false)
            .uniqueKeys(ImmutableList.of())
            .build();

    // Rebuild Parent with Child name
    parentTable = parentTable.toBuilder().childTables(ImmutableList.of(childTable.name())).build();

    // Create Schema
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of(parentTable.name(), parentTable, childTable.name(), childTable))
            .build();

    // Input Row (Just PK for Parent)
    Schema parentPkSchema = Schema.builder().addField("SingerId", Schema.FieldType.INT64).build();
    Row parentPkRow = Row.withSchema(parentPkSchema).addValue(100L).build();
    KV<String, Row> inputElement = KV.of(parentTable.name(), parentPkRow);

    // Mock ProcessContext
    when(mockContext.element()).thenReturn(inputElement);

    PCollectionView<DataGeneratorSchema> schemaViewForCascading =
        pipeline
            .apply("CreateSchemaCascading", Create.of(schema))
            .apply("ViewAsSingletonCascading", View.asSingleton());
    when(mockContext.sideInput(schemaViewForCascading)).thenReturn(schema);

    // Run the pipeline to materialize the side input
    pipeline.run();

    TestBatchAndWriteFn fn = new TestBatchAndWriteFn(options, schemaViewForCascading, mockWriter);

    org.apache.beam.sdk.state.MapState<String, Row> mockActiveKeys =
        mock(org.apache.beam.sdk.state.MapState.class);
    org.apache.beam.sdk.state.MapState<Long, List<BatchAndWrite.LifecycleEvent>>
        mockEventQueueState = new FakeMapState<>();
    org.apache.beam.sdk.state.Timer mockEventTimer = mock(org.apache.beam.sdk.state.Timer.class);

    org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> mockActiveTimestamps =
        mock(org.apache.beam.sdk.state.ValueState.class);
    fn.setup(options);
    fn.startBundle();
    fn.processElement(
        mockContext,
        mockActiveKeys,
        mockEventQueueState,
        mockActiveTimestamps,
        new FakeMapState<>(),
        mockEventTimer);
    fn.finishBundle();

    // Verify Parent Flush
    verify(mockWriter).write(anyList(), eq(parentTable), eq(""), eq("INSERT"));

    // Verify Child Flush
    verify(mockWriter).write(anyList(), eq(childTable), eq(""), eq("INSERT"));
  }

  @Test
  public void testLifecycleEventScheduling() throws Exception {
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getBatchSize()).thenReturn(10);
    when(options.getSinkType()).thenReturn(DataGeneratorOptions.SinkType.MYSQL);
    when(options.getSinkOptions())
        .thenReturn(
            "[{\"logicalShardId\":\"shard1\",\"user\":\"root\",\"password\":\"\",\"host\":\"localhost\",\"port\":\"3306\",\"dbName\":\"test\"}]");
    when(options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class))
        .thenReturn(options);
    when(options.getInsertQps()).thenReturn(1000);
    when(options.getUpdateQps()).thenReturn(1000);
    when(options.getDeleteQps()).thenReturn(1000);
    com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter mockWriter =
        mock(com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter.class);

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("id"))
            .insertQps(1)
            .updateQps(1)
            .deleteQps(1)
            .isRoot(true)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of(table.name(), table))
            .build();

    Schema parentPkSchema = Schema.builder().addField("id", Schema.FieldType.INT64).build();
    Row parentPkRow = Row.withSchema(parentPkSchema).addValue(100L).build();
    KV<String, Row> inputElement = KV.of(table.name(), parentPkRow);

    when(mockContext.element()).thenReturn(inputElement);

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline
            .apply("CreateSchemaLifecycle", Create.of(schema))
            .apply("ViewAsSingletonLifecycle", View.asSingleton());
    when(mockContext.sideInput(schemaView)).thenReturn(schema);

    pipeline.run();

    TestBatchAndWriteFn fn = new TestBatchAndWriteFn(options, schemaView, mockWriter);

    org.apache.beam.sdk.state.MapState<String, Row> mockActiveKeys =
        mock(org.apache.beam.sdk.state.MapState.class);
    org.apache.beam.sdk.state.MapState<Long, List<BatchAndWrite.LifecycleEvent>>
        mockEventQueueState = mock(org.apache.beam.sdk.state.MapState.class);
    org.apache.beam.sdk.state.Timer mockEventTimer = mock(org.apache.beam.sdk.state.Timer.class);

    // Mock reading empty queue
    org.apache.beam.sdk.state.ReadableState<List<BatchAndWrite.LifecycleEvent>> mockReadableState =
        mock(org.apache.beam.sdk.state.ReadableState.class);
    when(mockEventQueueState.get(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(mockReadableState);
    when(mockReadableState.read()).thenReturn(null);

    org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> mockActiveTimestamps =
        mock(org.apache.beam.sdk.state.ValueState.class);
    fn.setup(options);
    fn.startBundle();
    fn.processElement(
        mockContext,
        mockActiveKeys,
        mockEventQueueState,
        mockActiveTimestamps,
        new FakeMapState<>(),
        mockEventTimer);
    fn.finishBundle();

    // Verify that events were scheduled (1 update, 1 delete)
    verify(mockEventQueueState, times(2))
        .put(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyList());
    verify(mockEventTimer, times(2)).set(any(org.joda.time.Instant.class));
    verify(mockActiveKeys).put(eq("Users:100"), any(org.apache.beam.sdk.values.Row.class));
  }

  @Test
  public void testChildDeleteScheduledBeforeParent() throws Exception {
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getBatchSize()).thenReturn(10);
    when(options.getSinkType()).thenReturn(DataGeneratorOptions.SinkType.MYSQL);
    when(options.getSinkOptions())
        .thenReturn(
            "[{\"logicalShardId\":\"shard1\",\"user\":\"root\",\"password\":\"\",\"host\":\"localhost\",\"port\":\"3306\",\"dbName\":\"test\"}]");
    when(options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class))
        .thenReturn(options);
    when(options.getInsertQps()).thenReturn(100);
    when(options.getUpdateQps()).thenReturn(0); // 0 updates to simplify
    when(options.getDeleteQps()).thenReturn(100); // Force delete

    com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter mockWriter =
        mock(com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter.class);

    // Parent Table (Root)
    DataGeneratorTable parentTable =
        DataGeneratorTable.builder()
            .name("Singers")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("SingerId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("SingerId"))
            .insertQps(1)
            .isRoot(true)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    // Child Table
    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Albums")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("SingerId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("AlbumId")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("SingerId", "AlbumId"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("FK_Albums_Singers")
                        .referencedTable("Singers")
                        .keyColumns(ImmutableList.of("SingerId"))
                        .referencedColumns(ImmutableList.of("SingerId"))
                        .build()))
            .insertQps(1)
            .isRoot(false)
            .uniqueKeys(ImmutableList.of())
            .build();

    parentTable = parentTable.toBuilder().childTables(ImmutableList.of(childTable.name())).build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.MYSQL)
            .tables(ImmutableMap.of(parentTable.name(), parentTable, childTable.name(), childTable))
            .build();

    Schema parentPkSchema = Schema.builder().addField("SingerId", Schema.FieldType.INT64).build();
    Row parentPkRow = Row.withSchema(parentPkSchema).addValue(100L).build();
    KV<String, Row> inputElement = KV.of(parentTable.name(), parentPkRow);

    org.apache.beam.sdk.transforms.DoFn.ProcessContext mockContext =
        mock(org.apache.beam.sdk.transforms.DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn(inputElement);

    org.apache.beam.sdk.values.PCollectionView<DataGeneratorSchema> schemaView =
        mock(org.apache.beam.sdk.values.PCollectionView.class);
    when(mockContext.sideInput(schemaView)).thenReturn(schema);

    TestBatchAndWriteFn fn = new TestBatchAndWriteFn(options, schemaView, mockWriter);

    org.apache.beam.sdk.state.MapState<String, Row> mockActiveKeys =
        mock(org.apache.beam.sdk.state.MapState.class);
    FakeMapState<Long, List<BatchAndWrite.LifecycleEvent>> mockEventQueueState =
        new FakeMapState<>();
    org.apache.beam.sdk.state.Timer mockEventTimer = mock(org.apache.beam.sdk.state.Timer.class);

    org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> mockActiveTimestamps =
        mock(org.apache.beam.sdk.state.ValueState.class);
    fn.setup(options);
    fn.startBundle();
    fn.processElement(
        mockContext,
        mockActiveKeys,
        mockEventQueueState,
        mockActiveTimestamps,
        new FakeMapState<>(),
        mockEventTimer);
    fn.finishBundle();

    // No need to capture, just read from fakeEventQueueState!
    java.util.TreeMap<Long, List<BatchAndWrite.LifecycleEvent>> finalQueue =
        mockEventQueueState.map;

    org.junit.Assert.assertFalse(finalQueue.isEmpty());
    java.util.Map.Entry<Long, List<BatchAndWrite.LifecycleEvent>> lastEntry =
        finalQueue.lastEntry();
    List<BatchAndWrite.LifecycleEvent> events = lastEntry.getValue();

    org.junit.Assert.assertTrue(events.size() >= 2);

    int childDeleteIdx = -1;
    int parentDeleteIdx = -1;

    for (int i = 0; i < events.size(); i++) {
      BatchAndWrite.LifecycleEvent e = events.get(i);
      if ("DELETE".equals(e.type)) {
        if ("Albums".equals(e.tableName)) {
          childDeleteIdx = i;
        } else if ("Singers".equals(e.tableName)) {
          parentDeleteIdx = i;
        }
      }
    }

    org.junit.Assert.assertTrue("Child delete not found", childDeleteIdx >= 0);
    org.junit.Assert.assertTrue("Parent delete not found", parentDeleteIdx >= 0);
    org.junit.Assert.assertTrue(
        "Child delete should be before Parent delete", childDeleteIdx < parentDeleteIdx);
  }

  private static class FakeMapState<K, V> implements org.apache.beam.sdk.state.MapState<K, V> {
    final java.util.TreeMap<K, V> map = new java.util.TreeMap<>();

    @Override
    public org.apache.beam.sdk.state.ReadableState<V> get(K key) {
      return new org.apache.beam.sdk.state.ReadableState<V>() {
        @Override
        public V read() {
          return map.get(key);
        }

        @Override
        public org.apache.beam.sdk.state.ReadableState<V> readLater() {
          return this;
        }
      };
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<V> getOrDefault(K key, V defaultValue) {
      return new org.apache.beam.sdk.state.ReadableState<V>() {
        @Override
        public V read() {
          return map.getOrDefault(key, defaultValue);
        }

        @Override
        public org.apache.beam.sdk.state.ReadableState<V> readLater() {
          return this;
        }
      };
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<V> computeIfAbsent(
        K key, java.util.function.Function<? super K, ? extends V> mappingFunction) {
      return new org.apache.beam.sdk.state.ReadableState<V>() {
        @Override
        public V read() {
          return map.computeIfAbsent(key, mappingFunction);
        }

        @Override
        public org.apache.beam.sdk.state.ReadableState<V> readLater() {
          return this;
        }
      };
    }

    @Override
    public void put(K key, V value) {
      map.put(key, value);
    }

    @Override
    public void remove(K key) {
      map.remove(key);
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<Iterable<java.util.Map.Entry<K, V>>> entries() {
      return new org.apache.beam.sdk.state.ReadableState<Iterable<java.util.Map.Entry<K, V>>>() {
        @Override
        public Iterable<java.util.Map.Entry<K, V>> read() {
          return map.entrySet();
        }

        @Override
        public org.apache.beam.sdk.state.ReadableState<Iterable<java.util.Map.Entry<K, V>>>
            readLater() {
          return this;
        }
      };
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<Boolean> isEmpty() {
      return new org.apache.beam.sdk.state.ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return map.isEmpty();
        }

        @Override
        public org.apache.beam.sdk.state.ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<Iterable<K>> keys() {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.beam.sdk.state.ReadableState<Iterable<V>> values() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testDiamondDependencyPropagation() throws Exception {
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getBatchSize()).thenReturn(1); // Flush immediately
    when(options.getSinkType()).thenReturn(DataGeneratorOptions.SinkType.MYSQL);
    when(options.getSinkOptions()).thenReturn("[]");
    when(options.as(com.google.cloud.teleport.v2.templates.DataGeneratorOptions.class))
        .thenReturn(options);
    when(options.getInsertQps()).thenReturn(100);
    when(options.getUpdateQps()).thenReturn(0);
    when(options.getDeleteQps()).thenReturn(0);

    com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter mockWriter =
        mock(com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter.class);

    List<List<Row>> capturedLists = new java.util.ArrayList<>();
    List<DataGeneratorTable> capturedTables = new java.util.ArrayList<>();

    org.mockito.Mockito.doAnswer(
            invocation -> {
              List<Row> batch = invocation.getArgument(0);
              DataGeneratorTable table = invocation.getArgument(1);
              capturedLists.add(new java.util.ArrayList<>(batch)); // Capture copy!
              capturedTables.add(table);
              return null;
            })
        .when(mockWriter)
        .write(
            org.mockito.ArgumentMatchers.anyList(),
            org.mockito.ArgumentMatchers.any(DataGeneratorTable.class),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyString());

    // P1
    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("id"))
            .insertQps(1)
            .isRoot(true)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    // P2
    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("id"))
            .insertQps(1)
            .isRoot(false)
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    // C1
    DataGeneratorTable c1 =
        DataGeneratorTable.builder()
            .name("C1")
            .columns(
                ImmutableList.of(
                    DataGeneratorColumn.builder()
                        .name("id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(true)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("p1_id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(false)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build(),
                    DataGeneratorColumn.builder()
                        .name("p2_id")
                        .logicalType(LogicalType.INT64)
                        .isPrimaryKey(false)
                        .isNullable(false)
                        .isGenerated(false)
                        .originalType("INT64")
                        .build()))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("FK_C1_P1")
                        .referencedTable("P1")
                        .keyColumns(ImmutableList.of("p1_id"))
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("FK_C1_P2")
                        .referencedTable("P2")
                        .keyColumns(ImmutableList.of("p2_id"))
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .insertQps(1)
            .isRoot(false)
            .uniqueKeys(ImmutableList.of())
            .build();

    p1 = p1.toBuilder().childTables(ImmutableList.of("P2")).build();
    p2 = p2.toBuilder().childTables(ImmutableList.of("C1")).build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.MYSQL)
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "C1", c1))
            .build();

    Schema p1Schema = Schema.builder().addField("id", Schema.FieldType.INT64).build();
    Row p1Row = Row.withSchema(p1Schema).addValue(100L).build();
    KV<String, Row> inputElement = KV.of("P1", p1Row);

    org.apache.beam.sdk.transforms.DoFn.ProcessContext mockContext =
        mock(org.apache.beam.sdk.transforms.DoFn.ProcessContext.class);
    when(mockContext.element()).thenReturn(inputElement);

    org.apache.beam.sdk.values.PCollectionView<DataGeneratorSchema> schemaView =
        mock(org.apache.beam.sdk.values.PCollectionView.class);
    when(mockContext.sideInput(schemaView)).thenReturn(schema);

    TestBatchAndWriteFn fn = new TestBatchAndWriteFn(options, schemaView, mockWriter);

    org.apache.beam.sdk.state.MapState<String, Row> mockActiveKeys =
        mock(org.apache.beam.sdk.state.MapState.class);
    org.apache.beam.sdk.state.MapState<Long, List<BatchAndWrite.LifecycleEvent>>
        mockEventQueueState = mock(org.apache.beam.sdk.state.MapState.class);
    org.apache.beam.sdk.state.Timer mockEventTimer = mock(org.apache.beam.sdk.state.Timer.class);

    // Mock reading empty queue
    org.apache.beam.sdk.state.ReadableState<List<BatchAndWrite.LifecycleEvent>> mockReadableState =
        mock(org.apache.beam.sdk.state.ReadableState.class);
    when(mockEventQueueState.get(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(mockReadableState);
    when(mockReadableState.read()).thenReturn(null);

    org.apache.beam.sdk.state.ValueState<java.util.TreeSet<Long>> mockActiveTimestamps =
        mock(org.apache.beam.sdk.state.ValueState.class);
    fn.setup(options);
    fn.startBundle();
    fn.processElement(
        mockContext,
        mockActiveKeys,
        mockEventQueueState,
        mockActiveTimestamps,
        new FakeMapState<>(),
        mockEventTimer);
    fn.finishBundle();

    // We used doAnswer to capture writes!

    boolean foundC1 = false;
    for (int i = 0; i < capturedTables.size(); i++) {
      if (capturedTables.get(i).name().equals("C1")) {
        foundC1 = true;
        List<Row> rows = capturedLists.get(i);
        org.junit.Assert.assertFalse(rows.isEmpty());
        Row c1Row = rows.get(0);
        org.junit.Assert.assertEquals(Long.valueOf(100L), c1Row.getInt64("p1_id"));
      }
    }
    org.junit.Assert.assertTrue("C1 should have been generated", foundC1);
  }

  @Test
  public void testGenerateUpdateRow_RetainsFkAndUnique() {
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn pkCol =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
            .name("id")
            .logicalType(com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
            .isPrimaryKey(true)
            .originalType("STRING")
            .isGenerated(false)
            .isNullable(false)
            .build();
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn fkCol =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
            .name("parent_id")
            .logicalType(com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
            .isPrimaryKey(false)
            .originalType("STRING")
            .isGenerated(false)
            .isNullable(false)
            .build();
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn ukCol =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
            .name("email")
            .logicalType(com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
            .isPrimaryKey(false)
            .originalType("STRING")
            .isGenerated(false)
            .isNullable(false)
            .build();
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn normalCol =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
            .name("name")
            .logicalType(com.google.cloud.teleport.v2.templates.model.LogicalType.STRING)
            .isPrimaryKey(false)
            .originalType("STRING")
            .isGenerated(true)
            .isNullable(false)
            .build();

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.builder()
            .name("Users")
            .columns(com.google.common.collect.ImmutableList.of(pkCol, fkCol, ukCol, normalCol))
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(
                com.google.common.collect.ImmutableList.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey.builder()
                        .name("fk_parents")
                        .keyColumns(com.google.common.collect.ImmutableList.of("parent_id"))
                        .referencedTable("Parents")
                        .referencedColumns(com.google.common.collect.ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(
                com.google.common.collect.ImmutableList.of(
                    com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey.builder()
                        .name("uk_email")
                        .keyColumns(com.google.common.collect.ImmutableList.of("email"))
                        .build()))
            .insertQps(100)
            .updateQps(0)
            .deleteQps(0)
            .isRoot(true)
            .childTables(com.google.common.collect.ImmutableList.of())
            .build();

    org.apache.beam.sdk.schemas.Schema schema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addField("id", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
            .addField("parent_id", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
            .addField("email", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
            .addField("name", org.apache.beam.sdk.schemas.Schema.FieldType.STRING)
            .build();

    org.apache.beam.sdk.values.Row originalRow =
        org.apache.beam.sdk.values.Row.withSchema(schema)
            .addValues("100", "parent_1", "user@example.com", "John Doe")
            .build();

    BatchAndWrite.BatchAndWriteFn fn = new BatchAndWrite.BatchAndWriteFn("MYSQL", null, 100, null);
    fn.faker = new com.github.javafaker.Faker();

    java.util.LinkedHashMap<String, Object> pkValues = new java.util.LinkedHashMap<>();
    pkValues.put("id", "100");
    org.apache.beam.sdk.values.Row updateRow = fn.generateUpdateRow(pkValues, table, originalRow);

    org.junit.Assert.assertEquals("100", updateRow.getString("id"));
    org.junit.Assert.assertEquals("parent_1", updateRow.getString("parent_id"));
    org.junit.Assert.assertEquals("user@example.com", updateRow.getString("email"));
    org.junit.Assert.assertNotNull(updateRow.getString("name"));
  }
}
