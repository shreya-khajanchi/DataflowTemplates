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
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.utils.SeedUtils;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * A {@link PTransform} that generates Primary Key values for the selected table. It uses {@link
 * Faker} to generate realistic fake data based on logical types.
 */
public class GeneratePrimaryKey
    extends PTransform<PCollection<DataGeneratorTable>, PCollection<KV<String, Row>>> {

  private final int maxShards;
  private final String sinkOptionsPath;
  private final String sinkType;

  public GeneratePrimaryKey(int maxShards, String sinkOptionsPath, String sinkType) {
    this.maxShards = maxShards;
    this.sinkOptionsPath = sinkOptionsPath;
    this.sinkType = sinkType;
  }

  @Override
  public PCollection<KV<String, Row>> expand(PCollection<DataGeneratorTable> input) {
    return input
        .apply(
            "GeneratePrimaryKey",
            ParDo.of(new GeneratePrimaryKeyFn(maxShards, sinkOptionsPath, sinkType)))
        .setCoder(
            KvCoder.of(
                org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                org.apache.beam.sdk.coders.SerializableCoder.of(Row.class)));
  }

  static class GeneratePrimaryKeyFn extends DoFn<DataGeneratorTable, KV<String, Row>> {
    private final int maxShards;
    private final String sinkOptionsPath;
    private final String sinkType;
    private transient Faker faker;
    private transient Random random;
    private transient List<String> logicalShardIds;

    public GeneratePrimaryKeyFn(int maxShards, String sinkOptionsPath, String sinkType) {
      this.maxShards = maxShards;
      this.sinkOptionsPath = sinkOptionsPath;
      this.sinkType = sinkType;
    }

    @Setup
    public void setup() {
      // Seed from multiple independent entropy sources (see SeedUtils javadoc). Using only
      // `new SecureRandom().nextLong()` is unsafe on Dataflow: autoscaled worker VMs are cloned
      // from the same image and can have correlated /dev/urandom state at the moment @Setup
      // runs, which previously produced identical Faker sequences across workers and caused
      // "PK already exists" errors during scale-up.
      faker = new Faker(new java.util.Random(SeedUtils.generate()));
      random = new java.util.Random(SeedUtils.generate());

      if (Constants.SINK_TYPE_MYSQL.equalsIgnoreCase(sinkType) && sinkOptionsPath != null) {
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

    @ProcessElement
    public void processElement(
        @Element DataGeneratorTable table, OutputReceiver<KV<String, Row>> out) {
      List<DataGeneratorColumn> pkColumns =
          table.columns().stream()
              .filter(DataGeneratorColumn::isPrimaryKey)
              .collect(Collectors.toList());

      if (pkColumns.isEmpty()) {
        // Fallback: If no PK, maybe return empty Row or log warning?
        // Ideally every table has a PK in this generator.
        return;
      }

      Schema schema = buildSchema(pkColumns);
      Row.Builder rowBuilder = Row.withSchema(schema);

      for (DataGeneratorColumn column : pkColumns) {
        rowBuilder.addValue(generateValue(column));
      }

      // Add logical shard ID
      String shardId = "";
      if (logicalShardIds != null && !logicalShardIds.isEmpty()) {
        shardId = logicalShardIds.get(random.nextInt(logicalShardIds.size()));
      } else {
        shardId = "shard" + random.nextInt(maxShards);
      }
      rowBuilder.addValue(shardId);

      out.output(KV.of(table.name(), rowBuilder.build()));
    }

    private Schema buildSchema(List<DataGeneratorColumn> columns) {
      Schema.Builder builder = Schema.builder();
      for (DataGeneratorColumn col : columns) {
        builder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      }
      builder.addField(Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      return builder.build();
    }

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }
  }
}
