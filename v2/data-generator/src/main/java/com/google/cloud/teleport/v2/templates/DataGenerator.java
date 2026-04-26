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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite;
import com.google.cloud.teleport.v2.templates.transforms.GeneratePrimaryKey;
import com.google.cloud.teleport.v2.templates.transforms.GenerateTicks;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader;
import com.google.cloud.teleport.v2.templates.transforms.SelectTable;
import com.google.cloud.teleport.v2.templates.transforms.WriteFailuresToGcs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;

@Template(
    name = "Data_Generator",
    category = TemplateCategory.STREAMING,
    displayName = "Data Generator",
    description = "A template to generate synthetic data based on a source schema.",
    optionsClass = DataGeneratorOptions.class,
    flexContainerName = "data-generator",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
public class DataGenerator {

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    DataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataGeneratorOptions.class);
    options.setStreaming(true);
    run(options);
  }

  public static PipelineResult run(DataGeneratorOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    // Register SerializableCoder for Row class globally since we use dynamically constructed
    // schemas at runtime
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            Row.class, org.apache.beam.sdk.coders.SerializableCoder.of(Row.class));

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply(
            "LoadSchema",
            new SchemaLoader(
                options.getSinkType(),
                options.getSinkOptions(),
                options.getInsertQps(),
                options.getSchemaConfig()));

    // Generate ticks based on schema QPS.
    //
    // Default baseTickRate to the user-declared total insertQps so that in the common case
    // totalRootQps (computed at runtime from the side input) ≈ baseTickRate, which keeps the
    // scaling multiplier near 1 and avoids scale-up bursts. Explicit --baseTickRate still wins
    // if the operator wants to pin it independently of --insertQps.
    int baseTickRate =
        options.getBaseTickRate() != null
            ? options.getBaseTickRate()
            : (options.getInsertQps() != null ? options.getInsertQps() : 1000);
    PCollection<DataGeneratorTable> ticks =
        pipeline
            .apply(
                "TriggerTick",
                org.apache.beam.sdk.io.GenerateSequence.from(0)
                    .withRate(baseTickRate, org.joda.time.Duration.standardSeconds(1)))
            // Reshuffle BEFORE the scale-up multiplier so the fan-out parallelizes across
            // workers. Without this, GenerateSequence fuses with ScaleTicksFn and a single
            // worker handles all scaling, which caps throughput at very high totalRootQps.
            .apply("ReshuffleForParallelScale", Reshuffle.viaRandomKey())
            .apply("GenerateTicks", new GenerateTicks(options, schemaView))
            .apply("ReshuffleProvider", Reshuffle.viaRandomKey())
            .apply("SelectTable", new SelectTable(schemaView));

    // Generate Primary Keys
    int maxShards = options.getMaxShards() != null ? options.getMaxShards() : 1;
    PCollection<KV<String, Row>> pendingRows =
        ticks.apply(
            "GeneratePrimaryKey",
            new GeneratePrimaryKey(
                maxShards, options.getSinkOptions(), options.getSinkType().name()));

    // Reshuffle based on Hash(TableName + PK) to ensure same PK goes to same worker
    // Key = Hash(TableName + PK) % 5000
    PCollection<KV<String, Row>> reshuffledRows =
        pendingRows
            .apply(
                "MapToReshuffleKey",
                ParDo.of(
                    new DoFn<KV<String, Row>, KV<String, Row>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String tableName = c.element().getKey();
                        Row pkValues = c.element().getValue();
                        int hash = (tableName + pkValues.toString()).hashCode();
                        int shard = Math.abs(hash % 50);
                        c.output(KV.of(tableName + "#" + shard, pkValues));
                      }
                    }))
            .apply("Reshuffle", Reshuffle.of());

    PCollection<String> dlqRecords =
        reshuffledRows.apply(
            "BatchAndWrite",
            new BatchAndWrite(
                options.getSinkType().name(),
                options.getSinkOptions(),
                options.getBatchSize(),
                schemaView));

    // Route generation/sink-write failures to GCS when a DLQ directory is configured. When unset,
    // the failure records are still emitted on the DLQ PCollection but nothing consumes them, so
    // they're silently dropped. Operators get visibility through the writeFailures /
    // generationFailures Counters either way.
    String dlqDirectory = options.getDeadLetterQueueDirectory();
    if (dlqDirectory != null && !dlqDirectory.isEmpty()) {
      dlqRecords.apply("WriteFailuresToGcs", new WriteFailuresToGcs(dlqDirectory));
    }

    return pipeline.run();
  }
}
