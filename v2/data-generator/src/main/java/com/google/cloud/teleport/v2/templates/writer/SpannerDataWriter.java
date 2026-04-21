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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spanner implementation of {@link DataWriter}. */
public class SpannerDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerDataWriter.class);
  private final SpannerConfig spannerConfig;
  private transient SpannerAccessor spannerAccessor;

  public SpannerDataWriter(DataGeneratorOptions options, String sinkOptionsJson) {
    this(sinkOptionsJson);
  }

  public SpannerDataWriter(String sinkOptionsJson) {
    this.spannerConfig = parseSpannerConfig(sinkOptionsJson);
  }

  private SpannerConfig parseSpannerConfig(String sinkOptionsJson) {
    org.json.JSONObject json = new org.json.JSONObject(sinkOptionsJson);
    String instanceId = json.getString("instanceId");
    String databaseId = json.getString("databaseId");
    String projectId = json.getString("projectId");

    return SpannerConfig.create()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId);
  }

  public SpannerDataWriter(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table) {
    write(rows, table, "", "");
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table, String shardId, String operation) {
    if (spannerAccessor == null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    List<Mutation> mutations = new ArrayList<>();
    for (Row row : rows) {
      if (Constants.MUTATION_DELETE.equalsIgnoreCase(operation)) {
        mutations.add(rowToDeleteMutation(table, row));
      } else if (Constants.MUTATION_INSERT.equalsIgnoreCase(operation)) {
        mutations.add(rowToInsertMutation(table, row));
      } else if (Constants.MUTATION_UPDATE.equalsIgnoreCase(operation)) {
        mutations.add(rowToUpdateMutation(table, row));
      } else {
        mutations.add(rowToMutation(table, row));
      }
    }
    spannerAccessor.getDatabaseClient().writeAtLeastOnce(mutations);
  }

  Mutation rowToMutation(DataGeneratorTable table, Row row) {
    return rowToMutation(table, row, Mutation.newInsertOrUpdateBuilder(table.name()));
  }

  Mutation rowToMutation(DataGeneratorTable table, Row row, Mutation.WriteBuilder builder) {
    for (DataGeneratorColumn col : table.columns()) {
      Object val = getFieldFromRow(row, col.name());
      setColumnValueFromLogicalType(builder, col, val);
    }
    return builder.build();
  }

  Mutation rowToInsertMutation(DataGeneratorTable table, Row row) {
    return rowToMutation(table, row, Mutation.newInsertBuilder(table.name()));
  }

  Mutation rowToUpdateMutation(DataGeneratorTable table, Row row) {
    return rowToMutation(table, row, Mutation.newUpdateBuilder(table.name()));
  }

  Mutation rowToDeleteMutation(DataGeneratorTable table, Row row) {
    com.google.cloud.spanner.Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
    for (String pkName : table.primaryKeys()) {
      Object val = getFieldFromRow(row, pkName);
      if (val == null) {
        throw new IllegalStateException(
            "Primary key value missing for column: " + pkName + " in table: " + table.name());
      }
      com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn col =
          getColumn(table, pkName);
      if (col != null) {
        addValueToKeyBuilder(keyBuilder, val, col.logicalType());
      } else {
        keyBuilder.append(val.toString());
      }
    }
    return Mutation.delete(table.name(), keyBuilder.build());
  }

  private com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn getColumn(
      DataGeneratorTable table, String columnName) {
    return table.columns().stream()
        .filter(c -> c.name().equals(columnName))
        .findFirst()
        .orElse(null);
  }

  private void addValueToKeyBuilder(
      com.google.cloud.spanner.Key.Builder builder, Object val, LogicalType type) {
    switch (type) {
      case INT64:
        builder.append((Long) val);
        break;
      case FLOAT64:
        builder.append((Double) val);
        break;
      case BOOLEAN:
        builder.append((Boolean) val);
        break;
      case STRING:
      case JSON:
      case UUID:
      case ENUM:
        builder.append((String) val);
        break;
      case NUMERIC:
        builder.append((BigDecimal) val);
        break;
      case BYTES:
        builder.append(com.google.cloud.ByteArray.copyFrom((byte[]) val));
        break;
      default:
        builder.append(val.toString());
    }
  }

  private Object getFieldFromRow(Row row, String fieldName) {
    if (row.getSchema().hasField(fieldName)) {
      return row.getValue(fieldName);
    }
    return null; // Handle missing fields if any
  }

  private void setColumnValueFromLogicalType(
      Mutation.WriteBuilder builder, DataGeneratorColumn column, Object value) {
    if (value == null) {
      return;
    }
    String columnName = column.name();
    LogicalType logicalType = column.logicalType();

    switch (logicalType) {
      case STRING:
      case JSON:
      case UUID:
      case ENUM:
        builder.set(columnName).to((String) value);
        break;
      case INT64:
        builder.set(columnName).to((Long) value);
        break;
      case FLOAT64:
        builder.set(columnName).to((Double) value);
        break;
      case NUMERIC:
        builder.set(columnName).to((BigDecimal) value);
        break;
      case BOOLEAN:
        builder.set(columnName).to((Boolean) value);
        break;
      case BYTES:
        builder.set(columnName).to(Value.bytes(ByteArray.copyFrom((byte[]) value)));
        break;
      case TIMESTAMP:
        if (value instanceof Instant) {
          builder
              .set(columnName)
              .to(
                  com.google.cloud.Timestamp.ofTimeMicroseconds(
                      ((Instant) value).getMillis() * 1000));
        } else {
          builder.set(columnName).to(value.toString());
        }
        break;
      case DATE:
        if (value instanceof Instant) {
          builder
              .set(columnName)
              .to(
                  com.google.cloud.Date.fromJavaUtilDate(
                      new java.util.Date(((Instant) value).getMillis())));
        } else {
          builder.set(columnName).to(value.toString());
        }
        break;
      case ARRAY:
        LogicalType elementType = column.elementType();
        if (elementType == null) {
          elementType = LogicalType.STRING;
        }
        java.util.List<?> list = (java.util.List<?>) value;
        switch (elementType) {
          case STRING:
          case UUID:
          case ENUM:
            builder
                .set(columnName)
                .to(
                    Value.stringArray(
                        list.stream()
                            .map(v -> (String) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case JSON:
            builder
                .set(columnName)
                .to(
                    Value.jsonArray(
                        list.stream()
                            .map(v -> (String) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case INT64:
            builder
                .set(columnName)
                .to(
                    Value.int64Array(
                        list.stream()
                            .map(v -> (Long) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case FLOAT64:
            builder
                .set(columnName)
                .to(
                    Value.float64Array(
                        list.stream()
                            .map(v -> (Double) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case BOOLEAN:
            builder
                .set(columnName)
                .to(
                    Value.boolArray(
                        list.stream()
                            .map(v -> (Boolean) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case NUMERIC:
            builder
                .set(columnName)
                .to(
                    Value.numericArray(
                        list.stream()
                            .map(v -> (BigDecimal) v)
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case BYTES:
            builder
                .set(columnName)
                .to(
                    Value.bytesArray(
                        list.stream()
                            .map(v -> ByteArray.copyFrom((byte[]) v))
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case TIMESTAMP:
            builder
                .set(columnName)
                .to(
                    Value.timestampArray(
                        list.stream()
                            .map(
                                v ->
                                    com.google.cloud.Timestamp.ofTimeMicroseconds(
                                        ((Instant) v).getMillis() * 1000))
                            .collect(java.util.stream.Collectors.toList())));
            break;
          case DATE:
            builder
                .set(columnName)
                .to(
                    Value.dateArray(
                        list.stream()
                            .map(
                                v ->
                                    com.google.cloud.Date.fromJavaUtilDate(
                                        new java.util.Date(((Instant) v).getMillis())))
                            .collect(java.util.stream.Collectors.toList())));
            break;
          default:
            LOG.warn(
                "ARRAY column '{}' has unsupported element type {}; falling back to stringArray.",
                columnName,
                elementType);
            builder
                .set(columnName)
                .to(
                    Value.stringArray(
                        list.stream()
                            .map(v -> v == null ? null : v.toString())
                            .collect(java.util.stream.Collectors.toList())));
        }
        break;
      default:
        builder.set(columnName).to(value.toString());
    }
  }

  @Override
  public void close() {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
  }
}
