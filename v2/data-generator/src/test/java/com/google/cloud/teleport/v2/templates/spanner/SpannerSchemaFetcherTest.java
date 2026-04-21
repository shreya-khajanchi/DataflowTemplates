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
package com.google.cloud.teleport.v2.templates.spanner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SpannerSchemaFetcherTest {

  @Mock private Ddl ddl;

  @Spy private SpannerSchemaFetcher fetcher;

  @Before
  public void setUp() {
    String json = "{\"projectId\": \"p\", \"instanceId\": \"i\", \"databaseId\": \"d\"}";
    fetcher.init("dummy.json", json);
  }

  @Test
  public void testGetSchema() throws IOException {

    com.google.cloud.teleport.v2.spanner.ddl.Table table =
        com.google.cloud.teleport.v2.spanner.ddl.Table.builder()
            .name("t")
            .column("id")
            .parseType("INT64")
            .endColumn()
            .primaryKeys(ImmutableList.of())
            .build();

    Ddl.Builder builder = Ddl.builder();
    builder.addTable(table);
    Ddl ddl = builder.build();
    doReturn(ddl).when(fetcher).fetchDdl(any(SpannerConfig.class));

    DataGeneratorSchema result = fetcher.getSchema();

    assertEquals(1, result.tables().size());
    assertEquals("t", result.tables().get("t").name());
    assertEquals("id", result.tables().get("t").columns().get(0).name());
  }

  @Test
  public void testGetSchemaWithForeignKeysAndUniqueKeys() throws IOException {

    com.google.cloud.teleport.v2.spanner.ddl.Table parentTable =
        com.google.cloud.teleport.v2.spanner.ddl.Table.builder()
            .name("Parent")
            .column("id")
            .parseType("INT64")
            .endColumn()
            .primaryKeys(ImmutableList.of())
            .build();

    com.google.cloud.teleport.v2.spanner.ddl.ForeignKey.Builder fkBuilder =
        com.google.cloud.teleport.v2.spanner.ddl.ForeignKey.builder()
            .name("fk_parent")
            .table("Child")
            .referencedTable("Parent");
    fkBuilder.columnsBuilder().add("parentId");
    fkBuilder.referencedColumnsBuilder().add("id");

    com.google.cloud.teleport.v2.spanner.ddl.Index.Builder ukBuilder =
        com.google.cloud.teleport.v2.spanner.ddl.Index.builder()
            .name("uk_uniqueVal")
            .table("Child")
            .unique(true);
    ukBuilder
        .columns()
        .set(
            com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.create(
                "uniqueVal", com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order.ASC));
    com.google.cloud.teleport.v2.spanner.ddl.Index uniqueIndex = ukBuilder.build();

    com.google.cloud.teleport.v2.spanner.ddl.Table childTable =
        com.google.cloud.teleport.v2.spanner.ddl.Table.builder()
            .name("Child")
            .column("id")
            .parseType("INT64")
            .endColumn()
            .column("parentId")
            .parseType("INT64")
            .endColumn()
            .column("uniqueVal")
            .parseType("STRING(MAX)")
            .endColumn()
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of(fkBuilder.build()))
            .indexes(ImmutableList.of("uk_uniqueVal"))
            .indexObjects(ImmutableList.of(uniqueIndex))
            .build();

    System.out.println("Child Table FKs: " + childTable.foreignKeys());
    System.out.println("Child Table Indexes: " + childTable.indexes());

    Ddl.Builder builder = Ddl.builder();
    builder.addTable(parentTable);
    builder.addTable(childTable);
    Ddl ddl = builder.build();
    doReturn(ddl).when(fetcher).fetchDdl(any(SpannerConfig.class));

    DataGeneratorSchema result = fetcher.getSchema();

    assertEquals(2, result.tables().size());
    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable childResult =
        result.tables().get("Child");
    assertEquals(1, childResult.foreignKeys().size());
    assertEquals("fk_parent", childResult.foreignKeys().get(0).name());
    assertEquals("Parent", childResult.foreignKeys().get(0).referencedTable());
    assertEquals(1, childResult.uniqueKeys().size());
    assertEquals("uk_uniqueVal", childResult.uniqueKeys().get(0).name());
  }

  @Test
  public void testGetSchemaWithPrimaryKeyAndStoringColumns() throws IOException {

    com.google.cloud.teleport.v2.spanner.ddl.Index.Builder pkIndexBuilder =
        com.google.cloud.teleport.v2.spanner.ddl.Index.builder()
            .name("PRIMARY_KEY")
            .table("Table")
            .unique(true);
    pkIndexBuilder
        .columns()
        .set(
            com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.create(
                "id", com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order.ASC));

    com.google.cloud.teleport.v2.spanner.ddl.Index.Builder storingIndexBuilder =
        com.google.cloud.teleport.v2.spanner.ddl.Index.builder()
            .name("idx_unique_storing")
            .table("Table")
            .unique(true);
    storingIndexBuilder
        .columns()
        .set(
            com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.create(
                "uniqueCol", com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order.ASC))
        .set(
            com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.create(
                "storedCol", com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order.STORING));

    com.google.cloud.teleport.v2.spanner.ddl.Table table =
        com.google.cloud.teleport.v2.spanner.ddl.Table.builder()
            .name("Table")
            .column("id")
            .parseType("INT64")
            .endColumn()
            .column("uniqueCol")
            .parseType("STRING(MAX)")
            .endColumn()
            .column("storedCol")
            .parseType("STRING(MAX)")
            .endColumn()
            .primaryKeys(ImmutableList.of())
            .indexes(ImmutableList.of("PRIMARY_KEY", "idx_unique_storing"))
            .indexObjects(ImmutableList.of(pkIndexBuilder.build(), storingIndexBuilder.build()))
            .build();

    Ddl.Builder builder = Ddl.builder();
    builder.addTable(table);
    Ddl ddl = builder.build();
    doReturn(ddl).when(fetcher).fetchDdl(any(SpannerConfig.class));

    DataGeneratorSchema result = fetcher.getSchema();

    assertEquals(1, result.tables().size());
    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable tableResult =
        result.tables().get("Table");

    // PRIMARY_KEY index should be ignored, so only 1 unique key should be present
    assertEquals(1, tableResult.uniqueKeys().size());
    assertEquals("idx_unique_storing", tableResult.uniqueKeys().get(0).name());

    // content of the unique key should NOT contain storedCol
    assertEquals(1, tableResult.uniqueKeys().get(0).keyColumns().size());
    assertEquals("uniqueCol", tableResult.uniqueKeys().get(0).keyColumns().get(0));
  }

  @Test
  public void testGetSchemaWithUuidColumn() throws IOException {
    // Covers the full UUID chain: Ddl Column.parseType("UUID") → Type.uuid() →
    // SpannerSchemaFetcher.mapColumn → SpannerTypeMapper → LogicalType.UUID.
    com.google.cloud.teleport.v2.spanner.ddl.Table table =
        com.google.cloud.teleport.v2.spanner.ddl.Table.builder()
            .name("t")
            .column("id")
            .parseType("UUID")
            .endColumn()
            .column("tags")
            .parseType("ARRAY<UUID>")
            .endColumn()
            .primaryKeys(ImmutableList.of())
            .build();

    Ddl.Builder builder = Ddl.builder();
    builder.addTable(table);
    Ddl ddl = builder.build();
    doReturn(ddl).when(fetcher).fetchDdl(any(SpannerConfig.class));

    DataGeneratorSchema result = fetcher.getSchema();

    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn idCol =
        result.tables().get("t").columns().get(0);
    assertEquals("id", idCol.name());
    assertEquals(
        com.google.cloud.teleport.v2.templates.model.LogicalType.UUID, idCol.logicalType());

    // ARRAY<UUID> should map to ARRAY logical type with UUID element type so the
    // writer emits a proper Value.stringArray of UUID hex strings (not lorem text).
    com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn tagsCol =
        result.tables().get("t").columns().get(1);
    assertEquals(
        com.google.cloud.teleport.v2.templates.model.LogicalType.ARRAY, tagsCol.logicalType());
    assertEquals(
        com.google.cloud.teleport.v2.templates.model.LogicalType.UUID, tagsCol.elementType());
  }
}
