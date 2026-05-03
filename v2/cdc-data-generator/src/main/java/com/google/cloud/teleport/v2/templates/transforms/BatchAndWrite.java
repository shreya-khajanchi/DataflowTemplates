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

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.dofn.BatchAndWriteFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;

/**
 * {@link PTransform} that takes a stream of partially-generated keyed rows, completes them,
 * recursively materialises child rows from the schema DAG, batches them by {@code (table, shard,
 * op)}, and writes to the sink configured by {@code sinkType}/{@code sinkOptionsPath}.
 *
 * <p>Output is a {@code PCollection<String>} of JSON-encoded {@link FailureRecord}s — one per row
 * that could not be generated or that the sink rejected. Callers should pipe this output to a DLQ
 * sink (e.g. {@code WriteFailuresToGcs}) when a dead-letter directory is configured. The output
 * is empty when no failures occur.
 *
 * <p>The actual work is delegated to {@link BatchAndWriteFn}; this transform exists so callers can
 * apply one named PTransform without having to wire up the {@link ParDo} and side input.
 */
public class BatchAndWrite extends PTransform<PCollection<KV<String, Row>>, PCollection<String>> {

  private final SinkType sinkType;
  private final String sinkOptionsPath;
  private final Integer batchSize;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  /**
   * @param sinkType which sink writer the underlying {@link BatchAndWriteFn} should instantiate
   * @param sinkOptionsPath path/URI to the sink-specific configuration document
   * @param batchSize maximum rows buffered per {@code (table, shard, op)} before flush; {@code
   *     null} or non-positive falls back to {@link BatchAndWriteFn#DEFAULT_BATCH_SIZE}
   * @param schemaView side input carrying the {@link DataGeneratorSchema}
   */
  public BatchAndWrite(
      SinkType sinkType,
      String sinkOptionsPath,
      Integer batchSize,
      PCollectionView<DataGeneratorSchema> schemaView) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = batchSize;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<String> expand(PCollection<KV<String, Row>> input) {
    return input
        .apply(
            "BatchAndWriteFn",
            ParDo.of(new BatchAndWriteFn(sinkType, sinkOptionsPath, batchSize, schemaView))
                .withSideInputs(schemaView))
        .setCoder(StringUtf8Coder.of());
  }
}
