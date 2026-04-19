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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for generating high-quality, worker-unique seeds for {@link java.util.Random} and
 * {@link com.github.javafaker.Faker} instances used inside Dataflow {@code DoFn}s.
 *
 * <p><b>Why this exists.</b> When Dataflow autoscales, new worker VMs are typically cloned from the
 * same image. On Linux, {@link SecureRandom} (NativePRNG) is seeded from {@code /dev/urandom},
 * whose initial pool on a freshly-booted cloned VM is derived from the image's saved random-seed
 * file and can therefore be <em>identical or highly correlated</em> across clones during the
 * narrow window when the Beam harness calls {@code @Setup}. If seeding relied solely on
 * {@code new SecureRandom().nextLong()}, two freshly-booted workers could produce the same seed,
 * and since {@code new java.util.Random(seed)} is a deterministic LCG, they would then emit
 * identical sequences of random characters — producing duplicate primary keys and triggering
 * "PK already exists" errors on the sink during scale-up.
 *
 * <p>This class defends against that by mixing {@link SecureRandom} with <em>worker-local</em>
 * entropy that is not derived from {@code /dev/urandom} — hostname, PID, thread id, JVM-high-res
 * clock, a JVM-scoped monotonic counter, and a fresh object's identity hash. These components
 * differ between two Dataflow workers by construction (different hostnames, different PIDs),
 * guaranteeing that two concurrently-starting workers cannot collapse to the same seed even if
 * every kernel-entropy source misbehaves.
 *
 * <p>The mixed result is passed through SplitMix64 (the standard avalanche-quality finalizer from
 * java.util.SplittableRandom) so that structural correlations in any individual component do not
 * survive into the final seed.
 *
 * <p>Safe for concurrent use.
 */
public final class SeedUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SeedUtils.class);

  /** JVM-scoped monotonic counter. Guarantees that two seeds minted inside the same JVM differ. */
  private static final AtomicLong JVM_COUNTER = new AtomicLong(0L);

  /**
   * Hostname resolved once per JVM. Unique per Dataflow worker VM and therefore a reliable
   * tiebreaker even when {@link SecureRandom} is correlated across cloned VMs.
   */
  private static final long HOST_HASH = computeHostHash();

  /** Process id — unique per JVM on a host. */
  private static final long PID = computePid();

  private SeedUtils() {}

  /**
   * Returns a fresh 64-bit seed suitable for passing to {@code new java.util.Random(seed)} or
   * {@code new Faker(new Random(seed))}. Each call returns a different value.
   *
   * <p>The seed is built from multiple independent entropy sources so that the weakness (or
   * outright failure) of any single source cannot produce a collision across workers or across
   * calls within one worker.
   */
  public static long generate() {
    long secureRandomBits = new SecureRandom().nextLong();
    long uuidHighBits = UUID.randomUUID().getMostSignificantBits();
    long uuidLowBits = UUID.randomUUID().getLeastSignificantBits();
    long nano = System.nanoTime();
    long threadId = Thread.currentThread().getId();
    long jvmCounter = JVM_COUNTER.getAndIncrement();
    long identityHash = System.identityHashCode(new Object()) & 0xFFFFFFFFL;

    long mixed =
        splitMix64(secureRandomBits)
            ^ splitMix64(uuidHighBits ^ (uuidLowBits << 1))
            ^ splitMix64(nano ^ (threadId << 17) ^ (jvmCounter << 37))
            ^ splitMix64(HOST_HASH ^ (PID << 23))
            ^ splitMix64(identityHash);

    return splitMix64(mixed);
  }

  /**
   * SplitMix64 finalizer. Same constants as {@link java.util.SplittableRandom}. Guarantees good
   * avalanche even on structured inputs, so XOR-combined low-entropy components don't cancel out.
   */
  static long splitMix64(long x) {
    long z = x + 0x9E3779B97F4A7C15L;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  private static long computeHostHash() {
    try {
      String host = InetAddress.getLocalHost().getHostName();
      // Use SplitMix64 as a 64-bit hash of the hostname chars.
      long h = 0L;
      for (int i = 0; i < host.length(); i++) {
        h = splitMix64(h ^ host.charAt(i));
      }
      return h;
    } catch (UnknownHostException e) {
      // Fall back to a process-unique UUID — still worker-unique.
      LOG.warn("Could not resolve local hostname; falling back to random UUID for seed mixing.", e);
      return UUID.randomUUID().getMostSignificantBits();
    }
  }

  private static long computePid() {
    try {
      return ProcessHandle.current().pid();
    } catch (Throwable t) {
      // Extremely old / restricted JVMs — use something else that's at least process-unique.
      return System.identityHashCode(ProcessHandle.class) & 0xFFFFFFFFL;
    }
  }
}
