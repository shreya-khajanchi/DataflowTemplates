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

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that loads the {@link DataGeneratorSchema} from the sink as a side input.
 */
public class SchemaLoader extends PTransform<PBegin, PCollectionView<DataGeneratorSchema>> {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaLoader.class);
  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final Integer qps;
  private final String schemaConfigPath;

  public SchemaLoader(
      SinkType sinkType, String sinkOptionsPath, Integer qps, String schemaConfigPath) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.qps = qps;
    this.schemaConfigPath = schemaConfigPath;
  }

  @Override
  public PCollectionView<DataGeneratorSchema> expand(PBegin input) {
    return input
        .apply("CreateSinkType", Create.of(sinkType))
        .apply(
            "FetchSchema",
            ParDo.of(new FetchSchemaFn(sinkType, sinkOptionsPath, qps, schemaConfigPath)))
        .apply("ViewAsSingleton", View.asSingleton());
  }

  static class FetchSchemaFn extends DoFn<SinkType, DataGeneratorSchema> {
    private final SinkType sinkType;
    private final String sinkOptionsPath;
    private final int qps;
    private final String schemaConfigPath;

    FetchSchemaFn(SinkType sinkType, String sinkOptionsPath, Integer qps, String schemaConfigPath) {
      this.sinkType = sinkType;
      this.sinkOptionsPath = sinkOptionsPath;
      this.qps = qps != null ? qps : 1000;
      this.schemaConfigPath = schemaConfigPath;
    }

    @ProcessElement
    public void processElement(OutputReceiver<DataGeneratorSchema> receiver) {
      try {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        SinkSchemaFetcher fetcher = createFetcher(sinkType);

        fetcher.init(sinkOptionsPath, sinkOptionsJson);
        fetcher.setQps(qps);
        DataGeneratorSchema schema = fetcher.getSchema();

        if (schemaConfigPath != null && !schemaConfigPath.isEmpty()) {
          String schemaConfigJson = readSinkOptions(schemaConfigPath);
          schema = applyOverrides(schema, schemaConfigJson);
        }

        SchemaUtils.validateNoDuplicateFkTargets(schema);
        schema = SchemaUtils.setSchemaDAG(schema);
        LOG.info("Fetched Schema: {}", schema);

        java.util.List<String> rootTables =
            schema.tables().values().stream()
                .filter(com.google.cloud.teleport.v2.templates.model.DataGeneratorTable::isRoot)
                .map(com.google.cloud.teleport.v2.templates.model.DataGeneratorTable::name)
                .collect(java.util.stream.Collectors.toList());
        LOG.info("Root tables in the job: {}", rootTables);

        receiver.output(schema);
      } catch (IOException e) {
        throw new RuntimeException("Failed to fetch schema", e);
      }
    }

    private DataGeneratorSchema applyOverrides(
        DataGeneratorSchema schema, String schemaConfigJson) {
      JSONObject config = new JSONObject(schemaConfigJson);
      if (!config.has("tables")) {
        return schema;
      }
      JSONArray tablesArray = config.getJSONArray("tables");

      java.util.Map<String, DataGeneratorTable> tableMap = new java.util.HashMap<>(schema.tables());

      for (int i = 0; i < tablesArray.length(); i++) {
        JSONObject tableConfig = tablesArray.getJSONObject(i);
        String tableName = tableConfig.getString("tableName");

        DataGeneratorTable existingTable = tableMap.get(tableName);
        if (existingTable == null) {
          LOG.warn("Override specified for unknown table: {}", tableName);
          continue;
        }

        DataGeneratorTable.Builder tableBuilder = existingTable.toBuilder();

        if (tableConfig.has("insertQps")) {
          tableBuilder.insertQps(tableConfig.getInt("insertQps"));
        }
        if (tableConfig.has("updateQps")) {
          tableBuilder.updateQps(tableConfig.getInt("updateQps"));
        }
        if (tableConfig.has("deleteQps")) {
          tableBuilder.deleteQps(tableConfig.getInt("deleteQps"));
        }

        if (tableConfig.has("columns")) {
          JSONObject columnsConfig = tableConfig.getJSONObject("columns");
          java.util.List<DataGeneratorColumn> updatedColumns = new java.util.ArrayList<>();

          for (DataGeneratorColumn col : existingTable.columns()) {
            if (columnsConfig.has(col.name())) {
              JSONObject colConfig = columnsConfig.getJSONObject(col.name());
              DataGeneratorColumn.Builder colBuilder = col.toBuilder();

              if (colConfig.has("generator")) {
                String gen = colConfig.getString("generator");
                if (!"inherited".equals(gen)) {
                  colBuilder.generator(gen);
                }
              }
              if (colConfig.has("skip")) {
                colBuilder.skip(colConfig.getBoolean("skip"));
              }
              updatedColumns.add(colBuilder.build());
            } else {
              updatedColumns.add(col);
            }
          }
          tableBuilder.columns(com.google.common.collect.ImmutableList.copyOf(updatedColumns));
        }

        tableMap.put(tableName, tableBuilder.build());
      }

      return DataGeneratorSchema.builder()
          .tables(com.google.common.collect.ImmutableMap.copyOf(tableMap))
          .dialect(schema.dialect())
          .build();
    }

    /**
     * Creates a {@link SinkSchemaFetcher} based on the sink type. This method is protected to allow
     * overriding in tests.
     *
     * @param sinkType The sink type.
     * @return The {@link SinkSchemaFetcher}.
     */
    protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
      if (sinkType == SinkType.SPANNER) {
        return new SpannerSchemaFetcher();
      } else if (sinkType == SinkType.MYSQL) {
        return new MySqlSchemaFetcher();
      } else {
        throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
      }
    }

    protected String readSinkOptions(String path) throws IOException {
      try (ReadableByteChannel channel =
          FileSystems.open(FileSystems.matchNewResource(path, false))) {
        try (Reader reader =
            new InputStreamReader(Channels.newInputStream(channel), StandardCharsets.UTF_8)) {
          return CharStreams.toString(reader);
        }
      }
    }
  }
}
