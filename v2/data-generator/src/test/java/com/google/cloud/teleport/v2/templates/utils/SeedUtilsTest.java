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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.github.javafaker.Faker;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that {@link SeedUtils} produces unique seeds under the exact condition where the original
 * seeding (a single {@code new SecureRandom().nextLong()}) can fail: many DoFn instances starting
 * simultaneously on autoscaled workers.
 */
@RunWith(JUnit4.class)
public class SeedUtilsTest {

  @Test
  public void generate_returnsDifferentValueOnEachCall_singleThread() {
    Set<Long> seeds = new HashSet<>();
    int n = 10_000;
    for (int i = 0; i < n; i++) {
      seeds.add(SeedUtils.generate());
    }
    // No collisions across 10k sequential calls.
    assertEquals("Unexpected seed collision across " + n + " calls", n, seeds.size());
  }

  /**
   * Simulates a Dataflow scale-up burst: many threads all call {@code SeedUtils.generate()} at
   * the same wall-clock moment. With the old seeding (single SecureRandom.nextLong on cloned VMs
   * with correlated /dev/urandom state), this scenario produced seed collisions and therefore
   * identical Faker PK sequences. With the new seeding, the JVM-scoped monotonic counter +
   * per-thread id + per-call identity hash guarantee uniqueness even within a single JVM.
   */
  @Test
  public void generate_noCollisions_underConcurrentBurst() throws InterruptedException {
    int workers = 1024;
    ExecutorService pool = Executors.newFixedThreadPool(64);
    CountDownLatch startLine = new CountDownLatch(1);
    CountDownLatch finishLine = new CountDownLatch(workers);
    Set<Long> seeds = ConcurrentHashMap.newKeySet();
    AtomicInteger collisions = new AtomicInteger();

    for (int i = 0; i < workers; i++) {
      pool.submit(
          () -> {
            try {
              startLine.await();
              long s = SeedUtils.generate();
              if (!seeds.add(s)) {
                collisions.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLine.countDown();
            }
          });
    }

    startLine.countDown(); // release all workers at once
    finishLine.await(30, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertEquals("SeedUtils produced a duplicate seed under concurrent burst", 0, collisions.get());
    assertEquals("Not all workers produced a seed", workers, seeds.size());
  }

  /**
   * End-to-end: simulate exactly what happens in GeneratePrimaryKeyFn.setup() when hundreds of
   * workers come up at once. Build a Faker from a SeedUtils seed, generate a 60-char string PK,
   * and assert all PKs are unique.
   *
   * <p>This is the regression guard for the user-reported bug: "primary key already exists"
   * during worker scaling on a 60-char string PK column.
   */
  @Test
  public void faker60CharStringPk_allUnique_underConcurrentBurst() throws InterruptedException {
    int workers = 1024;
    ExecutorService pool = Executors.newFixedThreadPool(64);
    CountDownLatch startLine = new CountDownLatch(1);
    CountDownLatch finishLine = new CountDownLatch(workers);
    Set<String> pks = ConcurrentHashMap.newKeySet();
    AtomicInteger collisions = new AtomicInteger();

    for (int i = 0; i < workers; i++) {
      pool.submit(
          () -> {
            try {
              startLine.await();
              Faker faker = new Faker(new Random(SeedUtils.generate()));
              String pk = faker.lorem().characters(60);
              if (!pks.add(pk)) {
                collisions.incrementAndGet();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLine.countDown();
            }
          });
    }

    startLine.countDown();
    finishLine.await(60, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertEquals(
        "Duplicate 60-char PK produced across concurrently-seeded Faker instances",
        0,
        collisions.get());
    assertEquals("Not all workers produced a PK", workers, pks.size());
  }

  /**
   * Demonstrates that SplitMix64 decorrelates near-identical inputs. If two workers somehow
   * got seeds differing by only one bit (which could happen with weakly-seeded SecureRandom),
   * the final mixed output should still be wildly different — preventing "near-collision"
   * Faker outputs.
   */
  @Test
  public void splitMix64_decorrelatesCloseInputs() {
    long a = SeedUtils.splitMix64(0L);
    long b = SeedUtils.splitMix64(1L);
    long c = SeedUtils.splitMix64(2L);
    assertNotEquals(a, b);
    assertNotEquals(b, c);
    // Avalanche: flipping one bit of input flips ~half the bits of output. Require at least 20.
    int diffBits = Long.bitCount(a ^ b);
    if (diffBits < 20) {
      throw new AssertionError("splitMix64 avalanche too weak: only " + diffBits + " bits flipped");
    }
  }
}
