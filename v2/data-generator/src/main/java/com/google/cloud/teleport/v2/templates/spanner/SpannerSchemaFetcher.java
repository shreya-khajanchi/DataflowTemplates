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

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.json.JSONObject;

public class SpannerSchemaFetcher implements SinkSchemaFetcher {

  private String projectId;
  private String instanceId;
  private String databaseId;
  private int qps;
  private final SpannerTypeMapper typeMapper = new SpannerTypeMapper();

  @Override
  public void init(String optionsFilePath, String jsonData) {
    JSONObject json = new JSONObject(jsonData);
    this.projectId = json.getString("projectId");
    this.instanceId = json.getString("instanceId");
    this.databaseId = json.getString("databaseId");
  }

  @Override
  public void setQps(int qps) {
    this.qps = qps;
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(StaticValueProvider.of(projectId))
            .withInstanceId(StaticValueProvider.of(instanceId))
            .withDatabaseId(StaticValueProvider.of(databaseId));
    Ddl ddl = fetchDdl(spannerConfig);
    return mapToDataGeneratorSchema(ddl);
  }

  private DataGeneratorSchema mapToDataGeneratorSchema(Ddl ddl) {
    SinkDialect dialect =
        ddl.dialect() == com.google.cloud.spanner.Dialect.POSTGRESQL
            ? SinkDialect.POSTGRESQL
            : SinkDialect.GOOGLE_STANDARD_SQL;

    Map<String, DataGeneratorTable> tables =
        ddl.allTables().stream()
            .collect(
                Collectors.toMap(
                    com.google.cloud.teleport.v2.spanner.ddl.Table::name,
                    table -> mapTable(table, dialect)));

    return DataGeneratorSchema.builder()
        .tables(ImmutableMap.copyOf(tables))
        .dialect(dialect)
        .build();
  }

  private DataGeneratorTable mapTable(
      com.google.cloud.teleport.v2.spanner.ddl.Table table, SinkDialect dialect) {
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (com.google.cloud.teleport.v2.spanner.ddl.Column column : table.columns()) {
      columnsBuilder.add(mapColumn(column, table, dialect));
    }

    ImmutableList.Builder<DataGeneratorForeignKey> fksBuilder = ImmutableList.builder();
    if (table.foreignKeys() != null) {
      for (com.google.cloud.teleport.v2.spanner.ddl.ForeignKey fk : table.foreignKeys()) {
        fksBuilder.add(
            DataGeneratorForeignKey.builder()
                .name(fk.name())
                .referencedTable(fk.referencedTable())
                .keyColumns(fk.columns())
                .referencedColumns(fk.referencedColumns())
                .build());
      }
    }

    ImmutableList.Builder<DataGeneratorUniqueKey> uniqueKeysBuilder = ImmutableList.builder();
    if (table.indexes() != null) {
      for (String indexName : table.indexes()) {
        com.google.cloud.teleport.v2.spanner.ddl.Index index =
            table.indexObjects().stream()
                .filter(i -> i.name().equals(indexName))
                .findFirst()
                .orElse(null);
        // Ignore PRIMARY_KEY and only include unique indices
        if (index != null && index.unique() && !"PRIMARY_KEY".equalsIgnoreCase(index.name())) {
          uniqueKeysBuilder.add(
              DataGeneratorUniqueKey.builder()
                  .name(index.name())
                  .keyColumns(
                      index.indexColumns().stream()
                          .filter(
                              c ->
                                  c.order()
                                      != com.google.cloud.teleport.v2.spanner.ddl.IndexColumn.Order
                                          .STORING)
                          .map(com.google.cloud.teleport.v2.spanner.ddl.IndexColumn::name)
                          .collect(ImmutableList.toImmutableList()))
                  .build());
        }
      }
    }

    return DataGeneratorTable.builder()
        .name(table.name())
        .columns(columnsBuilder.build())
        .primaryKeys(
            table.primaryKeys().stream()
                .map(com.google.cloud.teleport.v2.spanner.ddl.IndexColumn::name)
                .collect(ImmutableList.toImmutableList()))
        .interleavedInTable(table.interleavingParent())
        .foreignKeys(fksBuilder.build())
        .uniqueKeys(uniqueKeysBuilder.build())
        .isRoot(table.interleavingParent() == null)
        .insertQps(qps)
        .build();
  }

  private DataGeneratorColumn mapColumn(
      com.google.cloud.teleport.v2.spanner.ddl.Column column,
      com.google.cloud.teleport.v2.spanner.ddl.Table table,
      SinkDialect dialect) {
    boolean isPrimaryKey =
        table.primaryKeys().stream().anyMatch(pk -> pk.name().equals(column.name()));

    DataGeneratorColumn.Builder builder =
        DataGeneratorColumn.builder()
            .name(column.name())
            .logicalType(typeMapper.getLogicalType(column.typeString(), dialect))
            .isNullable(!column.notNull())
            .isPrimaryKey(isPrimaryKey)
            .isGenerated(column.isGenerated())
            .originalType(column.typeString())
            .size(column.size() != null ? Long.valueOf(column.size()) : null)
            .precision(null)
            .scale(null);

    // For ARRAY columns, propagate the element type so the generator and writer
    // emit values of the correct type (otherwise everything degrades to STRING).
    Type ddlType = column.type();
    if (ddlType != null
        && (ddlType.getCode() == Type.Code.ARRAY || ddlType.getCode() == Type.Code.PG_ARRAY)) {
      Type elementDdlType = ddlType.getArrayElementType();
      if (elementDdlType != null && elementDdlType.getCode() != null) {
        // Code.getName() returns the dialect-appropriate name (e.g. "INT64" for GoogleSQL,
        // "bigint" for PG), which is what the type mapper's switches expect.
        LogicalType elementLogical =
            typeMapper.getLogicalType(elementDdlType.getCode().getName(), dialect);
        builder.elementType(elementLogical);
      }
    }

    return builder.build();
  }

  protected SpannerAccessor getSpannerAccessor(SpannerConfig spannerConfig) {
    return SpannerAccessor.getOrCreate(spannerConfig);
  }

  protected Ddl fetchDdl(SpannerConfig spannerConfig) {
    return SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
  }
}
