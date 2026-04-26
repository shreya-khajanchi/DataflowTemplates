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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end smoke test for {@link WriteFailuresToGcs}. Routes a small in-memory PCollection of
 * JSON failure strings through the wrapper into a temp directory and verifies that at least one
 * file lands in the configured DLQ folder. The full {@code DLQWriteTransform} behavior (windowing,
 * shard templates) is exercised by its own tests in {@code v2/common}; here we only verify the
 * wiring.
 */
@RunWith(JUnit4.class)
public class WriteFailuresToGcsTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private File dlqDir;

  @Before
  public void setUp() throws IOException {
    dlqDir = tmp.newFolder("dlq");
  }

  @After
  public void tearDown() throws IOException {
    // TemporaryFolder rule already deletes; this is just defensive in case TestPipeline left
    // something tangled.
    if (dlqDir != null && dlqDir.exists()) {
      try (Stream<Path> walk = Files.walk(dlqDir.toPath())) {
        walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  @Test
  public void testRejectsEmptyDirectory() {
    try {
      new WriteFailuresToGcs("");
      org.junit.Assert.fail("Expected IllegalArgumentException for empty path");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testRejectsNullDirectory() {
    try {
      new WriteFailuresToGcs(null);
      org.junit.Assert.fail("Expected IllegalArgumentException for null path");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testTrailingSlashHandling() {
    // Both forms should construct without error; the underlying transform accepts both.
    new WriteFailuresToGcs("file:///tmp/dlq/");
    new WriteFailuresToGcs("file:///tmp/dlq");
  }

  @Test
  public void testWritesRecordsToConfiguredDirectory() throws Exception {
    PCollection<String> input =
        pipeline.apply(Create.of("{\"table\":\"Users\",\"error\":\"boom\"}"));

    input.apply(new WriteFailuresToGcs("file://" + dlqDir.getAbsolutePath() + "/"));

    pipeline.run().waitUntilFinish();

    // Walk the dlq directory tree and assert at least one .json shard landed somewhere.
    long jsonFiles;
    try (Stream<Path> walk = Files.walk(dlqDir.toPath())) {
      jsonFiles =
          walk.filter(p -> p.toFile().isFile() && p.toString().endsWith(".json")).count();
    }
    org.junit.Assert.assertTrue(
        "Expected at least one .json shard under " + dlqDir + ", found " + jsonFiles,
        jsonFiles >= 1);
  }
}
