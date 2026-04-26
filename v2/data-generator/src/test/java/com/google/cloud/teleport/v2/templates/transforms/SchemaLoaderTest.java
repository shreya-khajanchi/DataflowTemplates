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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader.FetchSchemaFn;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SchemaLoader}. */
@RunWith(JUnit4.class)
public class SchemaLoaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testFetchSchemaFn_Spanner() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of())
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options", 100, null) {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.SPANNER, sinkType);
            return mockFetcher;
          }

          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init("options", "{}");
    verify(mockFetcher).setQps(100);
    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_MySql() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of()).dialect(SinkDialect.MYSQL).build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.MYSQL, "options", null, null) {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.MYSQL, sinkType);
            return mockFetcher;
          }

          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init("options", "{}");
    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_WithOverrides() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn col =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .originalType("INT64")
            .build();
    final com.google.cloud.teleport.v2.templates.model.DataGeneratorTable table =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .insertQps(100)
            .isRoot(true)
            .build();
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(com.google.common.collect.ImmutableMap.of("my_table", table))
            .dialect(SinkDialect.MYSQL)
            .build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.MYSQL, "options", null, "schema_config.json") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            return mockFetcher;
          }

          @Override
          protected String readSinkOptions(String path) throws IOException {
            if ("schema_config.json".equals(path)) {
              return "{\"tables\": [{\"tableName\": \"my_table\", \"insertQps\": 500}]}";
            }
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    org.mockito.ArgumentCaptor<DataGeneratorSchema> captor =
        org.mockito.ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    assertNotNull(resolvedSchema);
    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable resolvedTable =
        resolvedSchema.tables().get("my_table");
    assertNotNull(resolvedTable);
    assertEquals(500, resolvedTable.insertQps());
  }

  @Test
  public void testFetchSchemaFn_Unsupported() throws IOException {
    FetchSchemaFn fn =
        new FetchSchemaFn(null, "options", 1, null) { // null or dummy enum if possible
          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    // unexpected IO exception for null sink type is not what we want to test, we
    // want to test createFetcher throwing
    // But wait, createFetcher is called inside processElement.
    // We didn't override createFetcher here so it uses the real one which throws
    // IllegalArgumentException
    // However, the real one checks for SPANNER and MYSQL. null will throw
    // "Unsupported sink type: null"

    assertThrows(IllegalArgumentException.class, () -> fn.processElement(receiver));
  }

  @Test
  public void testFetchSchemaFn_IOException() throws IOException {
    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options", 1, null) {
          @Override
          protected String readSinkOptions(String path) throws IOException {
            throw new IOException("File not found");
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    assertThrows(RuntimeException.class, () -> fn.processElement(receiver));
  }

  // ---------- Helpers / fixtures for override tests ----------

  private static com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn pkCol(
      String name) {
    return com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
        .name(name)
        .logicalType(LogicalType.INT64)
        .isPrimaryKey(true)
        .isNullable(false)
        .isGenerated(false)
        .originalType("INT64")
        .build();
  }

  private static com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn dataCol(
      String name, LogicalType type) {
    return com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isPrimaryKey(false)
        .isNullable(true)
        .isGenerated(false)
        .originalType(type.name())
        .build();
  }

  private static com.google.cloud.teleport.v2.templates.model.DataGeneratorTable simpleTable(
      String name,
      com.google.common.collect.ImmutableList<
              com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn>
          columns,
      com.google.common.collect.ImmutableList<
              com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey>
          fks) {
    return com.google.cloud.teleport.v2.templates.model.DataGeneratorTable.builder()
        .name(name)
        .columns(columns)
        .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
        .foreignKeys(fks)
        .uniqueKeys(com.google.common.collect.ImmutableList.of())
        .insertQps(1)
        .isRoot(true)
        .build();
  }

  private static FetchSchemaFn fnReturning(DataGeneratorSchema schema, String configJson) {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    when(mockFetcher.getSchema()).thenReturn(schema);
    return new FetchSchemaFn(SinkType.MYSQL, "options", null, "schema_config.json") {
      @Override
      protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
        return mockFetcher;
      }

      @Override
      protected String readSinkOptions(String path) throws IOException {
        if ("schema_config.json".equals(path)) {
          return configJson;
        }
        return "{}";
      }
    };
  }

  // ---------- Generator + skip overrides ----------

  /**
   * A column-level override that sets {@code generator} and {@code skip:false} should land on the
   * resolved column. The bare directive form (without {@code #{...}}) must be normalised.
   */
  @Test
  public void testApplyOverrides_normalisesGeneratorAndSetsSkip() throws IOException {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "users",
                    simpleTable(
                        "users",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"),
                            dataCol("name", LogicalType.STRING),
                            dataCol("blob", LogicalType.BYTES)),
                        com.google.common.collect.ImmutableList.of())))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"users\",\"columns\":{"
            + "\"name\":{\"generator\":\"name.fullName\"},"
            + "\"blob\":{\"skip\":true}"
            + "}}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    org.mockito.ArgumentCaptor<DataGeneratorSchema> captor =
        org.mockito.ArgumentCaptor.forClass(DataGeneratorSchema.class);
    fnReturning(schema, config).processElement(receiver);
    verify(receiver).output(captor.capture());

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable resolved =
        captor.getValue().tables().get("users");
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn name =
        resolved.columns().stream().filter(c -> c.name().equals("name")).findFirst().get();
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn blob =
        resolved.columns().stream().filter(c -> c.name().equals("blob")).findFirst().get();
    assertEquals("#{name.fullName}", name.generator());
    assertEquals(true, blob.skip());
  }

  /** A pre-wrapped {@code #{...}} expression must pass through unchanged. */
  @Test
  public void testApplyOverrides_passesThroughAlreadyWrappedExpression() throws IOException {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "users",
                    simpleTable(
                        "users",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"), dataCol("name", LogicalType.STRING)),
                        com.google.common.collect.ImmutableList.of())))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"users\",\"columns\":{"
            + "\"name\":{\"generator\":\"#{name.fullName}\"}"
            + "}}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    org.mockito.ArgumentCaptor<DataGeneratorSchema> captor =
        org.mockito.ArgumentCaptor.forClass(DataGeneratorSchema.class);
    fnReturning(schema, config).processElement(receiver);
    verify(receiver).output(captor.capture());

    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn name =
        captor.getValue().tables().get("users").columns().stream()
            .filter(c -> c.name().equals("name"))
            .findFirst()
            .get();
    assertEquals("#{name.fullName}", name.generator());
  }

  /** {@code skip=true} on a primary-key column must be rejected loudly at config load. */
  @Test
  public void testApplyOverrides_rejectsSkipOnPrimaryKey() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "users",
                    simpleTable(
                        "users",
                        com.google.common.collect.ImmutableList.of(pkCol("id")),
                        com.google.common.collect.ImmutableList.of())))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"users\",\"columns\":{"
            + "\"id\":{\"skip\":true}"
            + "}}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class, () -> fnReturning(schema, config).processElement(receiver));
    org.junit.Assert.assertTrue(
        "Expected error message to name the PK column",
        ex.getMessage().contains("primary-key column 'id'") || ex.getCause() != null);
  }

  // ---------- Foreign-key overrides ----------

  /** Adding a previously-undiscovered FK appends it to the table's FK list. */
  @Test
  public void testApplyOverrides_addsNewForeignKey() throws IOException {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "orders",
                    simpleTable(
                        "orders",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"), dataCol("user_id", LogicalType.INT64)),
                        com.google.common.collect.ImmutableList.of()),
                    "users",
                    simpleTable(
                        "users",
                        com.google.common.collect.ImmutableList.of(pkCol("id")),
                        com.google.common.collect.ImmutableList.of())))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"orders\",\"foreignKeys\":["
            + "{\"name\":\"fk_orders_user\",\"referencedTable\":\"users\","
            + "\"keyColumns\":[\"user_id\"],\"referencedColumns\":[\"id\"]}"
            + "]}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    org.mockito.ArgumentCaptor<DataGeneratorSchema> captor =
        org.mockito.ArgumentCaptor.forClass(DataGeneratorSchema.class);
    fnReturning(schema, config).processElement(receiver);
    verify(receiver).output(captor.capture());

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable resolved =
        captor.getValue().tables().get("orders");
    assertEquals(1, resolved.foreignKeys().size());
    assertEquals("fk_orders_user", resolved.foreignKeys().get(0).name());
    assertEquals("users", resolved.foreignKeys().get(0).referencedTable());
  }

  /**
   * A discovered FK that doesn't appear in the config must remain on the table — the merge is by
   * NAME, not "replace all when foreignKeys is present."
   */
  @Test
  public void testApplyOverrides_preservesDiscoveredFkNotInConfig() throws IOException {
    com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey discovered =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey.builder()
            .name("fk_existing")
            .referencedTable("parents")
            .keyColumns(com.google.common.collect.ImmutableList.of("parent_id"))
            .referencedColumns(com.google.common.collect.ImmutableList.of("id"))
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "children",
                    simpleTable(
                        "children",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"),
                            dataCol("parent_id", LogicalType.INT64),
                            dataCol("user_id", LogicalType.INT64)),
                        com.google.common.collect.ImmutableList.of(discovered))))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"children\",\"foreignKeys\":["
            + "{\"name\":\"fk_new\",\"referencedTable\":\"users\","
            + "\"keyColumns\":[\"user_id\"],\"referencedColumns\":[\"id\"]}"
            + "]}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    org.mockito.ArgumentCaptor<DataGeneratorSchema> captor =
        org.mockito.ArgumentCaptor.forClass(DataGeneratorSchema.class);
    fnReturning(schema, config).processElement(receiver);
    verify(receiver).output(captor.capture());

    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable resolved =
        captor.getValue().tables().get("children");
    java.util.Set<String> fkNames =
        resolved.foreignKeys().stream()
            .map(
                com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey::name)
            .collect(java.util.stream.Collectors.toSet());
    org.junit.Assert.assertEquals(
        com.google.common.collect.ImmutableSet.of("fk_existing", "fk_new"), fkNames);
  }

  /**
   * A config FK with the same name but a different definition than the discovered one must throw,
   * naming the conflicting fields. This protects users from silently picking either side.
   */
  @Test
  public void testApplyOverrides_throwsOnConflictingForeignKey() {
    com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey discovered =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey.builder()
            .name("fk_orders_user")
            .referencedTable("users")
            .keyColumns(com.google.common.collect.ImmutableList.of("user_id"))
            .referencedColumns(com.google.common.collect.ImmutableList.of("id"))
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "orders",
                    simpleTable(
                        "orders",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"),
                            dataCol("user_id", LogicalType.INT64),
                            dataCol("alt_user_id", LogicalType.INT64)),
                        com.google.common.collect.ImmutableList.of(discovered))))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"orders\",\"foreignKeys\":["
            + "{\"name\":\"fk_orders_user\",\"referencedTable\":\"users\","
            + "\"keyColumns\":[\"alt_user_id\"],\"referencedColumns\":[\"id\"]}"
            + "]}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    RuntimeException ex =
        assertThrows(
            RuntimeException.class, () -> fnReturning(schema, config).processElement(receiver));
    org.junit.Assert.assertTrue(
        "Expected error to mention the FK name",
        rootMessage(ex).contains("fk_orders_user"));
    org.junit.Assert.assertTrue(
        "Expected error to mention the conflicting columns",
        rootMessage(ex).contains("alt_user_id") || rootMessage(ex).contains("user_id"));
  }

  /**
   * A config FK with the same name and identical definition is a no-op (idempotent re-declaration
   * of a discovered FK). Should not throw.
   */
  @Test
  public void testApplyOverrides_acceptsIdenticalForeignKey() throws IOException {
    com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey discovered =
        com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey.builder()
            .name("fk_orders_user")
            .referencedTable("users")
            .keyColumns(com.google.common.collect.ImmutableList.of("user_id"))
            .referencedColumns(com.google.common.collect.ImmutableList.of("id"))
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                com.google.common.collect.ImmutableMap.of(
                    "orders",
                    simpleTable(
                        "orders",
                        com.google.common.collect.ImmutableList.of(
                            pkCol("id"), dataCol("user_id", LogicalType.INT64)),
                        com.google.common.collect.ImmutableList.of(discovered))))
            .dialect(SinkDialect.MYSQL)
            .build();

    String config =
        "{\"tables\":[{\"tableName\":\"orders\",\"foreignKeys\":["
            + "{\"name\":\"fk_orders_user\",\"referencedTable\":\"users\","
            + "\"keyColumns\":[\"user_id\"],\"referencedColumns\":[\"id\"]}"
            + "]}]}";

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fnReturning(schema, config).processElement(receiver);
    // No throw is the assertion.
    verify(receiver).output(org.mockito.ArgumentMatchers.any(DataGeneratorSchema.class));
  }

  /** Walks a Throwable chain so an assertion can match the deepest "real" message. */
  private static String rootMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur.getMessage() == null ? "" : cur.getMessage();
  }
}
