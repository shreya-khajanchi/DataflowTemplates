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

import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Writes data-generator failure records to a GCS dead-letter queue.
 *
 * <p>This is a thin wrapper around {@link DLQWriteTransform.WriteDLQ} from {@code v2/common} that
 * fixes the file-name prefix and a sensible tmp directory layout so the data-generator template
 * only has to surface a single {@code deadLetterQueueDirectory} option to operators. Each input
 * record is a JSON string built by {@link
 * com.google.cloud.teleport.v2.templates.utils.FailureRecord}; output is one or more shard files
 * per minute under {@code <dlqDirectory>/error-*.json}.
 */
public class WriteFailuresToGcs extends PTransform<PCollection<String>, PDone> {

  private final String dlqDirectory;

  public WriteFailuresToGcs(String dlqDirectory) {
    if (dlqDirectory == null || dlqDirectory.isEmpty()) {
      throw new IllegalArgumentException("dlqDirectory must be a non-empty GCS path");
    }
    this.dlqDirectory = dlqDirectory;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    return input.apply(
        "WriteDLQToGcs",
        DLQWriteTransform.WriteDLQ.newBuilder()
            .withDlqDirectory(dlqDirectory)
            .withTmpDirectory(trailingSlash(dlqDirectory) + "tmp/")
            .setIncludePaneInfo(true)
            .build());
  }

  private static String trailingSlash(String path) {
    return path.endsWith("/") ? path : path + "/";
  }
}
