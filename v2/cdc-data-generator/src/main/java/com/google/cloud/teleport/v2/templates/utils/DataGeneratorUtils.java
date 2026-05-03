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

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

/**
 * Common utilities for data generation — value synthesis and Beam type mapping.
 *
 * <p>Kept separate from the transform classes so that the generation rules can be unit-tested in
 * isolation and shared across any DoFn that needs to emit a synthetic value for a {@link
 * DataGeneratorColumn}.
 *
 * <p>Logical types currently supported: {@code STRING, INT64, FLOAT64, NUMERIC, BOOLEAN, BYTES,
 * DATE, TIMESTAMP, JSON}. Future logical types (UUID, ENUM, ARRAY) and user-supplied Faker
 * expressions are introduced by separate PRs that extend the model first.
 */
public final class DataGeneratorUtils {

  /** Default string length when the column doesn't declare a {@code size}. */
  @VisibleForTesting static final int DEFAULT_STRING_LENGTH = 20;

  /** Default precision/scale when the column doesn't declare them. */
  @VisibleForTesting static final int DEFAULT_NUMERIC_PRECISION = 10;

  @VisibleForTesting static final int DEFAULT_NUMERIC_SCALE = 2;

  private DataGeneratorUtils() {}

  /**
   * Synthesise a value for the given column using {@code faker}. The caller is responsible for
   * seeding the Faker instance.
   *
   * <p>The returned Java type matches what {@link #mapToBeamFieldType(LogicalType)} declares for
   * the same logical type, so the value can be added directly to a {@code Row.Builder}.
   */
  public static Object generateValue(DataGeneratorColumn column, Faker faker) {
    LogicalType type = column.logicalType();
    Long size = column.size();

    switch (type) {
      case STRING:
        return faker.lorem().characters(clampStringLength(size));
      case JSON:
        return generateJson(faker);
      case INT64:
        return (long) ThreadLocalRandom.current().nextInt();
      case FLOAT64:
        {
          int scale = column.scale() != null ? column.scale() : DEFAULT_NUMERIC_SCALE;
          int precision =
              column.precision() != null ? column.precision() : DEFAULT_NUMERIC_PRECISION + 5;
          double maxVal = Math.pow(10, precision - scale) - 1.0 / Math.pow(10, scale);
          double minVal = -maxVal;
          return faker.number().randomDouble(scale, (long) minVal, (long) maxVal);
        }
      case NUMERIC:
        return generateNumeric(column, faker);
      case BOOLEAN:
        return faker.bool().bool();
      case BYTES:
        return faker.lorem().characters(clampStringLength(size)).getBytes(StandardCharsets.UTF_8);
      case DATE:
        return generateDate();
      case TIMESTAMP:
        return generateTimestamp();
      default:
        return "unknown";
    }
  }

  /** Map a logical type to the matching Beam {@link Schema.FieldType}. */
  public static Schema.FieldType mapToBeamFieldType(LogicalType logicalType) {
    switch (logicalType) {
      case STRING:
      case JSON:
        return Schema.FieldType.STRING;
      case INT64:
        return Schema.FieldType.INT64;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case NUMERIC:
        return Schema.FieldType.DECIMAL;
      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;
      case BYTES:
        return Schema.FieldType.BYTES;
      case DATE:
      case TIMESTAMP:
        return Schema.FieldType.DATETIME;
      default:
        return Schema.FieldType.STRING;
    }
  }

  /**
   * Generate a {@link BigDecimal} that fits the column's declared precision and scale. Avoids
   * {@code new BigDecimal(faker.number().randomNumber())} which returns an unbounded integer
   * value and overflows most DECIMAL/NUMERIC columns.
   */
  @VisibleForTesting
  static BigDecimal generateNumeric(DataGeneratorColumn column, Faker faker) {
    int prec =
        (column.precision() != null && column.precision() > 0)
            ? column.precision()
            : DEFAULT_NUMERIC_PRECISION;
    int sc =
        (column.scale() != null && column.scale() >= 0) ? column.scale() : DEFAULT_NUMERIC_SCALE;
    if (sc > prec) {
      sc = prec;
    }
    String randomDigits = faker.number().digits(prec);
    BigDecimal value = new BigDecimal(randomDigits).movePointLeft(sc);
    return value.setScale(sc, RoundingMode.HALF_UP);
  }

  /**
   * Clamp user-supplied string lengths into a safe range. Unset or zero values fall back to a
   * reasonable default; values larger than {@link Integer#MAX_VALUE} are capped so Faker doesn't
   * receive an invalid input.
   */
  @VisibleForTesting
  static int clampStringLength(Long size) {
    if (size == null || size <= 0) {
      return DEFAULT_STRING_LENGTH;
    }
    if (size > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return size.intValue();
  }

  private static String generateJson(Faker faker) {
    return "{"
        + "\"id\": "
        + faker.number().randomNumber()
        + ", "
        + "\"name\": \""
        + faker.name().fullName()
        + "\", "
        + "\"isActive\": "
        + faker.bool().bool()
        + ", "
        + "\"score\": "
        + faker.number().randomDouble(2, 0, 100)
        + ", "
        + "\"tags\": [\""
        + faker.lorem().word()
        + "\", \""
        + faker.lorem().word()
        + "\"], "
        + "\"address\": {\"city\": \""
        + faker.address().city()
        + "\", \"zip\": \""
        + faker.address().zipCode()
        + "\"}"
        + "}";
  }

  private static Instant generateDate() {
    long minMillis = -30610224000000L; // Year 1000
    long maxMillis = 253402300799000L; // Year 9999
    long randomMillis = ThreadLocalRandom.current().nextLong(minMillis, maxMillis);
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(randomMillis);
    // Zero the time part — DATE is day-granular; TIMESTAMP uses the TIMESTAMP branch.
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return new Instant(cal.getTimeInMillis());
  }

  private static Instant generateTimestamp() {
    long minMillis = -30610224000000L; // Year 1000
    long maxMillis = 253402300799000L; // Year 9999
    return new Instant(ThreadLocalRandom.current().nextLong(minMillis, maxMillis));
  }
}
