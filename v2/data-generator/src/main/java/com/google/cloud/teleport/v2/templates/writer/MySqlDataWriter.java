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
package com.google.cloud.teleport.v2.templates.writer;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link DataWriter}. */
public class MySqlDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlDataWriter.class);
  private final String sinkOptionsJson;
  private transient JdbcConnectionHelper connectionHelper;
  private transient org.json.JSONArray parsedShards;

  public MySqlDataWriter(String sinkOptionsJson) {
    this.sinkOptionsJson = sinkOptionsJson;
  }

  private org.json.JSONArray getParsedShards() {
    if (parsedShards == null) {
      parsedShards = new org.json.JSONArray(sinkOptionsJson);
    }
    return parsedShards;
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table) {
    write(rows, table, "", Constants.MUTATION_INSERT);
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table, String shardId, String operation) {
    if (rows.isEmpty()) {
      return;
    }
    if (connectionHelper == null) {
      connectionHelper =
          new com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper();
    }
    try {
      if (!connectionHelper.isConnectionPoolInitialized()) {
        ConnectionHelperRequest request = parseConnectionRequest(sinkOptionsJson);
        connectionHelper.init(request);
      }

      org.json.JSONArray jsonArray = getParsedShards();
      org.json.JSONObject targetShardJson = null;

      if (!shardId.isEmpty()) {
        for (int i = 0; i < jsonArray.length(); i++) {
          org.json.JSONObject shardJson = jsonArray.getJSONObject(i);
          if (shardId.equals(shardJson.optString("logicalShardId", ""))) {
            targetShardJson = shardJson;
            break;
          }
        }
      }

      if (targetShardJson == null) {
        // Fallback to first shard if not found or shardId is empty
        if (!shardId.isEmpty()) {
          LOG.warn(
              "Shard ID '{}' not found in sink options. Falling back to first shard.", shardId);
        }
        targetShardJson = jsonArray.getJSONObject(0);
      }

      String user = targetShardJson.optString("user", "");
      String host = targetShardJson.optString("host", "");
      String port = targetShardJson.optString("port", "3306");
      String dbName = targetShardJson.optString("dbName", "");
      String connectionProperties = targetShardJson.optString("connectionProperties", "");
      String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);
      if (!connectionProperties.isEmpty()) {
        jdbcUrl += "?" + connectionProperties;
      }
      String connectionKey = jdbcUrl + "/" + user;

      try (Connection connection = connectionHelper.getConnection(connectionKey)) {
        writeRowsToConnection(connection, rows, table, operation);
      }
    } catch (ConnectionException | SQLException e) {
      LOG.error("Failed to write to MySQL", e);
      throw new RuntimeException("Failed to write to MySQL", e);
    }
  }

  private void writeRowsToConnection(
      Connection connection, List<Row> rows, DataGeneratorTable table, String operation)
      throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    String sql;
    if (Constants.MUTATION_UPDATE.equalsIgnoreCase(operation)) {
      sql = buildUpdateSql(table);
    } else if (Constants.MUTATION_DELETE.equalsIgnoreCase(operation)) {
      sql = buildDeleteSql(table);
    } else {
      sql = buildInsertSql(table);
    }
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (Row row : rows) {
        setStatementParameters(statement, row, table, operation);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private String buildInsertSql(DataGeneratorTable table) {
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(table.name()).append(" (");

    // skip=true columns are intentionally absent from the row (see SchemaLoader / BatchAndWrite),
    // so they must also be absent from the INSERT column list. The sink applies its DEFAULT (or
    // NULL) for them. Filtering on the writer side keeps the SQL aligned with the row schema
    // exactly — relying on the row's getSchema() instead would mix in shard_id and other
    // synthetic fields.
    List<DataGeneratorColumn> columns =
        table.columns().stream()
            .filter(c -> !c.skip())
            .collect(java.util.stream.Collectors.toList());
    for (int i = 0; i < columns.size(); i++) {
      sql.append(columns.get(i).name());
      if (i < columns.size() - 1) {
        sql.append(", ");
      }
    }
    sql.append(") VALUES (");

    for (int i = 0; i < columns.size(); i++) {
      sql.append("?");
      if (i < columns.size() - 1) {
        sql.append(", ");
      }
    }
    sql.append(")");
    return sql.toString();
  }

  private String buildUpdateSql(DataGeneratorTable table) {
    StringBuilder sql = new StringBuilder("UPDATE ");
    sql.append(table.name()).append(" SET ");

    List<DataGeneratorColumn> columns = table.columns();
    boolean first = true;
    for (DataGeneratorColumn col : columns) {
      if (col.skip()) {
        continue;
      }
      if (!col.isPrimaryKey()) {
        if (!first) {
          sql.append(", ");
        }
        sql.append(col.name()).append(" = ?");
        first = false;
      }
    }
    sql.append(" WHERE ");
    first = true;
    for (DataGeneratorColumn col : columns) {
      // PK columns are never skipped (SchemaLoader rejects that at config time), so the WHERE
      // clause iteration doesn't need a skip filter — but an explicit one keeps this method
      // resilient if that invariant ever changes.
      if (col.skip()) {
        continue;
      }
      if (col.isPrimaryKey()) {
        if (!first) {
          sql.append(" AND ");
        }
        sql.append(col.name()).append(" = ?");
        first = false;
      }
    }
    return sql.toString();
  }

  private String buildDeleteSql(DataGeneratorTable table) {
    StringBuilder sql = new StringBuilder("DELETE FROM ");
    sql.append(table.name()).append(" WHERE ");

    List<DataGeneratorColumn> columns = table.columns();
    boolean first = true;
    for (DataGeneratorColumn col : columns) {
      if (col.skip()) {
        continue;
      }
      if (col.isPrimaryKey()) {
        if (!first) {
          sql.append(" AND ");
        }
        sql.append(col.name()).append(" = ?");
        first = false;
      }
    }
    return sql.toString();
  }

  private void setStatementParameters(
      PreparedStatement statement, Row row, DataGeneratorTable table, String operation)
      throws SQLException {
    // Iterate in the same skip-aware order as the SQL builders so positional `?` placeholders
    // line up with the values being bound.
    List<DataGeneratorColumn> columns =
        table.columns().stream()
            .filter(c -> !c.skip())
            .collect(java.util.stream.Collectors.toList());
    if (Constants.MUTATION_UPDATE.equalsIgnoreCase(operation)) {
      int idx = 1;
      // Set non-PK columns first
      for (DataGeneratorColumn col : columns) {
        if (!col.isPrimaryKey()) {
          Object val = row.getSchema().hasField(col.name()) ? row.getValue(col.name()) : null;
          statement.setObject(idx++, val);
        }
      }
      // Set PK columns for WHERE clause
      for (DataGeneratorColumn col : columns) {
        if (col.isPrimaryKey()) {
          Object val = row.getSchema().hasField(col.name()) ? row.getValue(col.name()) : null;
          statement.setObject(idx++, val);
        }
      }
    } else if (Constants.MUTATION_DELETE.equalsIgnoreCase(operation)) {
      int idx = 1;
      // Set PK columns for WHERE clause
      for (DataGeneratorColumn col : columns) {
        if (col.isPrimaryKey()) {
          Object val = row.getSchema().hasField(col.name()) ? row.getValue(col.name()) : null;
          statement.setObject(idx++, val);
        }
      }
    } else {
      // INSERT
      for (int i = 0; i < columns.size(); i++) {
        DataGeneratorColumn col = columns.get(i);
        Object val = row.getSchema().hasField(col.name()) ? row.getValue(col.name()) : null;
        statement.setObject(i + 1, val);
      }
    }
  }

  ConnectionHelperRequest parseConnectionRequest(String sinkOptionsJson) {
    try {
      org.json.JSONArray jsonArray = new org.json.JSONArray(sinkOptionsJson);
      if (jsonArray.length() == 0) {
        throw new RuntimeException("No shards found in sink options");
      }

      List<com.google.cloud.teleport.v2.spanner.migrations.shard.Shard> shards =
          new java.util.ArrayList<>();

      for (int i = 0; i < jsonArray.length(); i++) {
        org.json.JSONObject shardJson = jsonArray.getJSONObject(i);
        String user = shardJson.optString("user", "");
        String password = shardJson.optString("password", "");
        String host = shardJson.optString("host", "");
        String port = shardJson.optString("port", "3306");
        String dbName = shardJson.optString("dbName", "");
        String connectionProperties = shardJson.optString("connectionProperties", "");

        com.google.cloud.teleport.v2.spanner.migrations.shard.Shard shard =
            new com.google.cloud.teleport.v2.spanner.migrations.shard.Shard();
        shard.setLogicalShardId(shardJson.optString("logicalShardId", "shard" + i));
        shard.setUser(user);
        shard.setPassword(password);
        shard.setHost(host);
        shard.setPort(port);
        shard.setDbName(dbName);
        shard.setConnectionProperties(connectionProperties);
        shards.add(shard);
      }

      // Default to 10 connections per pool for now. Can be configured later.
      return new ConnectionHelperRequest(shards, "", 10, "com.mysql.cj.jdbc.Driver", "");
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse connection request", e);
    }
  }

  @Override
  public void close() {
    // HikariCP connectionHelper is a singleton managed per worker. We do not explicitly close it
    // per writer.
  }
}
