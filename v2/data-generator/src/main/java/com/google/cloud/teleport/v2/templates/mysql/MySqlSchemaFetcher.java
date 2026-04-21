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
package com.google.cloud.teleport.v2.templates.mysql;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceUniqueKey;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MySqlSchemaFetcher implements SinkSchemaFetcher {

  private String driverClassName;
  private String connectionUrl;
  private String username;
  private String password;
  private int qps;
  private final MySqlTypeMapper typeMapper = new MySqlTypeMapper();

  @Override
  public void init(String optionsFilePath, String jsonData) {
    if (optionsFilePath == null || optionsFilePath.isEmpty()) {
      throw new IllegalArgumentException(
          "MySQL sink requires a valid shard configuration file path.");
    }

    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails(optionsFilePath);
    if (shards == null || shards.isEmpty()) {
      throw new RuntimeException("No shards found in the provided shard file: " + optionsFilePath);
    }
    // Use the first shard to extract schema
    Shard firstShard = shards.get(0);
    this.username = firstShard.getUserName();
    this.password = firstShard.getPassword();

    String host = firstShard.getHost();
    String port = firstShard.getPort();
    // Grab the first dbName for the connection if mapped, otherwise it connects at
    // root
    String dbName = "";
    if (!firstShard.getDbName().isEmpty()) {
      dbName = firstShard.getDbName();
    }

    this.connectionUrl = String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);
    if (firstShard.getConnectionProperties() != null
        && !firstShard.getConnectionProperties().isEmpty()) {
      this.connectionUrl += "?" + firstShard.getConnectionProperties();
    }
    this.driverClassName = "com.mysql.cj.jdbc.Driver";
  }

  @Override
  public void setQps(int qps) {
    this.qps = qps;
  }

  @Override
  public DataGeneratorSchema getSchema() throws IOException {
    try {
      if (driverClassName != null && !driverClassName.isEmpty()) {
        Class.forName(driverClassName);
      }
      try (Connection connection = getConnection()) {
        return scanMySqlSchema(connection);
      }
    } catch (ClassNotFoundException | SQLException e) {
      throw new IOException("Failed to fetch MySQL schema", e);
    }
  }

  private DataGeneratorSchema scanMySqlSchema(Connection connection) throws SQLException {
    String databaseName = connection.getCatalog();
    MySqlInformationSchemaScanner scanner = createMySqlScanner(connection, databaseName);
    SourceSchema sourceSchema = scanner.scan();

    Map<String, DataGeneratorTable> tables =
        sourceSchema.tables().values().stream()
            .collect(
                Collectors.toMap(
                    SourceTable::name, table -> mapSourceTable(table, SinkDialect.MYSQL)));

    return DataGeneratorSchema.builder()
        .tables(ImmutableMap.copyOf(tables))
        .dialect(SinkDialect.MYSQL)
        .build();
  }

  protected MySqlInformationSchemaScanner createMySqlScanner(
      Connection connection, String databaseName) {
    return new MySqlInformationSchemaScanner(connection, databaseName);
  }

  private DataGeneratorTable mapSourceTable(SourceTable table, SinkDialect dialect) {
    ImmutableList.Builder<DataGeneratorColumn> columnsBuilder = ImmutableList.builder();
    for (SourceColumn column : table.columns()) {
      columnsBuilder.add(mapSourceColumn(column, dialect));
    }

    ImmutableList.Builder<DataGeneratorForeignKey> foreignKeysBuilder = ImmutableList.builder();
    if (table.foreignKeys() != null) {
      for (SourceForeignKey fk : table.foreignKeys()) {
        foreignKeysBuilder.add(
            DataGeneratorForeignKey.builder()
                .name(fk.name())
                .referencedTable(fk.referencedTable())
                .keyColumns(fk.keyColumns())
                .referencedColumns(fk.referencedColumns())
                .build());
      }
    }

    ImmutableList.Builder<DataGeneratorUniqueKey> uniqueKeysBuilder = ImmutableList.builder();
    if (table.uniqueKeys() != null) {
      for (SourceUniqueKey uk : table.uniqueKeys()) {
        uniqueKeysBuilder.add(
            DataGeneratorUniqueKey.builder().name(uk.name()).keyColumns(uk.keyColumns()).build());
      }
    }

    return DataGeneratorTable.builder()
        .name(table.name())
        .columns(columnsBuilder.build())
        .primaryKeys(table.primaryKeyColumns())
        .foreignKeys(foreignKeysBuilder.build())
        .uniqueKeys(uniqueKeysBuilder.build())
        .isRoot(true)
        .insertQps(qps)
        .build();
  }

  private DataGeneratorColumn mapSourceColumn(SourceColumn column, SinkDialect dialect) {
    DataGeneratorColumn.Builder builder =
        DataGeneratorColumn.builder()
            .name(column.name())
            .logicalType(typeMapper.getLogicalType(column.type(), dialect, column.size()))
            .isNullable(column.isNullable())
            .isPrimaryKey(column.isPrimaryKey())
            .isGenerated(column.isGenerated())
            .originalType(column.type())
            .size(column.size())
            .precision(column.precision())
            .scale(column.scale());

    // ENUM and SET need their member-value list, which lives in column_type (e.g.
    // "enum('a','b','c')") — not data_type ("enum"). Prefer fullType() and fall back to type()
    // for safety in case an upstream caller built a SourceColumn without the full string.
    String typeForEnumParsing = column.fullType() != null ? column.fullType() : column.type();
    if (typeForEnumParsing != null
        && (typeForEnumParsing.toUpperCase().startsWith("ENUM")
            || typeForEnumParsing.toUpperCase().startsWith("SET"))) {
      builder.enumValues(parseEnumValues(typeForEnumParsing));
    }

    return builder.build();
  }

  private java.util.List<String> parseEnumValues(String type) {
    java.util.List<String> values = new java.util.ArrayList<>();
    if (type == null || !type.contains("(")) {
      return values;
    }
    String content = type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
    // Split by comma, assuming standard MySQL format 'a','b','c'
    String[] parts = content.split(",");
    for (String part : parts) {
      values.add(part.trim().replace("'", ""));
    }
    return values;
  }

  protected Connection getConnection() throws SQLException {
    return DriverManager.getConnection(connectionUrl, username, password);
  }
}
