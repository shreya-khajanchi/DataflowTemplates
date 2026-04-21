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

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PTransform} that takes a fixed baseline tick rate (e.g., 1000/s) and scales or filters
 * it dynamically based on the total QPS required by the root tables in the schema side input.
 */
public class GenerateTicks extends PTransform<PCollection<Long>, PCollection<Long>> {

  private final DataGeneratorOptions options;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(
      DataGeneratorOptions options, PCollectionView<DataGeneratorSchema> schemaView) {
    this.options = options;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<Long> input) {
    // Mirrors the fallback in DataGenerator.run(): prefer --baseTickRate, else fall back to
    // --insertQps (the user-declared total), else 1000. Keeping the fallback consistent across
    // both sites avoids silent rate drift if one is edited without the other.
    int baseTickRate =
        options.getBaseTickRate() != null
            ? options.getBaseTickRate()
            : (options.getInsertQps() != null ? options.getInsertQps() : 1000);
    if (baseTickRate <= 0) {
      throw new IllegalArgumentException(
          "baseTickRate must be > 0; got " + baseTickRate + ". Set --baseTickRate or --insertQps.");
    }
    return input.apply(
        "ScaleTicks",
        ParDo.of(new ScaleTicksFn(schemaView, baseTickRate)).withSideInputs(schemaView));
  }

  static class ScaleTicksFn extends DoFn<Long, Long> {
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int baseTickRate;
    private transient DataGeneratorSchema cachedSchema;
    private transient int cachedTotalQps;

    ScaleTicksFn(PCollectionView<DataGeneratorSchema> schemaView, int baseTickRate) {
      this.schemaView = schemaView;
      this.baseTickRate = baseTickRate;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DataGeneratorSchema schema = c.sideInput(schemaView);

      // Calculate total QPS only if schema changes (usually it's static)
      if (cachedSchema != schema) {
        int totalQps = 0;
        for (DataGeneratorTable table : schema.tables().values()) {
          if (table.isRoot()) {
            totalQps += table.insertQps();
          }
        }
        cachedTotalQps = totalQps;
        cachedSchema = schema;
      }

      if (cachedTotalQps <= 0) {
        return; // No generation
      }

      if (cachedTotalQps < baseTickRate) {
        // Filter down: emit each input with probability totalQps/baseTickRate. Expected output
        // rate = baseTickRate * (totalQps/baseTickRate) = totalQps.
        double probability = (double) cachedTotalQps / baseTickRate;
        if (ThreadLocalRandom.current().nextDouble() < probability) {
          c.output(c.element());
        }
      } else {
        // Scale up (or exact match — multiplier=1, remainder=0 handles totalQps==baseTickRate).
        // Emit `multiplier` outputs deterministically, then probabilistically emit one more to
        // cover the sub-tick remainder. Expected output per input = totalQps/baseTickRate.
        int multiplier = cachedTotalQps / baseTickRate;
        int remainder = cachedTotalQps % baseTickRate;

        for (int i = 0; i < multiplier; i++) {
          c.output(c.element());
        }
        if (remainder > 0) {
          double probability = (double) remainder / baseTickRate;
          if (ThreadLocalRandom.current().nextDouble() < probability) {
            c.output(c.element());
          }
        }
      }
    }
  }
}
