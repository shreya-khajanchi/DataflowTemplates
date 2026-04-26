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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

/** Common utilities for data generation. */
public class DataGeneratorUtils {

  /**
   * Generates a random value for the given column using the provided Faker instance.
   *
   * <p>If the column carries a custom {@code generator} (a normalised Faker expression), it is
   * evaluated via {@link Faker#expression(String)} and the resulting String is coerced into the
   * column's logical type. {@link Faker#expression(String)} always returns a String regardless of
   * the underlying directive, so coercion is mandatory for non-string types. Coercion failure
   * (e.g. an INT64 column with a generator that emits non-numeric output) throws a {@link
   * RuntimeException} which the caller is expected to route to the dead-letter queue rather than
   * crashing the bundle.
   *
   * @param column The column definition.
   * @param faker The Faker instance to use.
   * @return The generated value.
   */
  public static Object generateValue(DataGeneratorColumn column, Faker faker) {
    LogicalType type = column.logicalType();
    Long size = column.size();

    if (column.generator() != null && !column.generator().isEmpty()) {
      return generateFromExpression(column, faker);
    }

    switch (type) {
      case STRING:
        int len = (size != null && size > 0 && size < 1000) ? size.intValue() : 20;
        return faker.lorem().characters(len);
      case INT64:
        {
          long[] range = int64RangeFor(column.originalType());
          return faker.number().numberBetween(range[0], range[1]);
        }
      case FLOAT64:
        int scale = column.scale() != null ? column.scale() : 2;
        int precision = column.precision() != null ? column.precision() : 15;
        double maxVal = Math.pow(10, precision - scale) - 1.0 / Math.pow(10, scale);
        double minVal = -maxVal;
        return faker.number().randomDouble(scale, (long) minVal, (long) maxVal);
      case NUMERIC:
        return generateNumeric(column, faker);
      case BOOLEAN:
        return faker.bool().bool();
      case BYTES:
        int byteLen = (size != null && size > 0 && size < 1000) ? size.intValue() : 20;
        return faker.lorem().characters(byteLen).getBytes(StandardCharsets.UTF_8);
      case DATE:
        java.util.Date pastDate = faker.date().past(365 * 2, TimeUnit.DAYS);
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(pastDate);
        cal.set(java.util.Calendar.HOUR_OF_DAY, 0);
        cal.set(java.util.Calendar.MINUTE, 0);
        cal.set(java.util.Calendar.SECOND, 0);
        cal.set(java.util.Calendar.MILLISECOND, 0);
        return new Instant(cal.getTimeInMillis());
      case TIMESTAMP:
        return new Instant(faker.date().past(365 * 2, TimeUnit.DAYS).getTime());
      case JSON:
        return "{\"id\": " + faker.number().randomNumber() + "}";
      case UUID:
        return java.util.UUID.randomUUID().toString();
      case ENUM:
        java.util.List<String> enumVals = column.enumValues();
        if (enumVals != null && !enumVals.isEmpty()) {
          return faker.options().option(enumVals.toArray(new String[0]));
        }
        return "UNKNOWN_ENUM";
      case ARRAY:
        int arraySize = faker.number().numberBetween(1, 5);
        java.util.List<Object> arrayList = new java.util.ArrayList<>();
        LogicalType elementType = column.elementType();
        if (elementType == null) {
          elementType = LogicalType.STRING;
        }
        DataGeneratorColumn elementCol =
            DataGeneratorColumn.builder()
                .name(column.name() + "_elem")
                .logicalType(elementType)
                .isNullable(true)
                .isPrimaryKey(false)
                .isGenerated(false)
                .originalType("")
                .build();
        for (int i = 0; i < arraySize; i++) {
          arrayList.add(generateValue(elementCol, faker));
        }
        return arrayList;
      default:
        return "unknown";
    }
  }

  /**
   * Evaluates a user-provided Faker expression and coerces the resulting String into the column's
   * logical type.
   *
   * <p>{@link Faker#expression(String)} returns a String even when the directive is conceptually
   * typed (e.g. {@code number.numberBetween} returns a long internally but is surfaced as a
   * decimal-string). For columns whose logical type isn't STRING, parsing is the only way to
   * recover the typed value.
   *
   * <p>For ENUM and ARRAY logical types we fall back to the random generator — the user-facing
   * coercion contract for those is fuzzier and the randomised path is type-safe by construction.
   */
  static Object generateFromExpression(DataGeneratorColumn column, Faker faker) {
    String expression = column.generator();
    String raw;
    try {
      raw = faker.expression(expression);
    } catch (Exception e) {
      throw new RuntimeException(
          "Faker expression for column '"
              + column.name()
              + "' failed to evaluate: "
              + expression,
          e);
    }
    if (raw == null) {
      return null;
    }
    return coerceToLogicalType(raw, column);
  }

  private static Object coerceToLogicalType(String raw, DataGeneratorColumn column) {
    LogicalType type = column.logicalType();
    try {
      switch (type) {
        case STRING:
        case JSON:
        case UUID:
        case ENUM:
          return raw;
        case INT64:
          return Long.parseLong(raw.trim());
        case FLOAT64:
          return Double.parseDouble(raw.trim());
        case NUMERIC:
          {
            BigDecimal bd = new BigDecimal(raw.trim());
            int sc = (column.scale() != null && column.scale() >= 0) ? column.scale() : 2;
            return bd.setScale(sc, java.math.RoundingMode.HALF_UP);
          }
        case BOOLEAN:
          {
            String lower = raw.trim().toLowerCase();
            if ("true".equals(lower) || "false".equals(lower)) {
              return Boolean.parseBoolean(lower);
            }
            throw new IllegalArgumentException("not a boolean: " + raw);
          }
        case BYTES:
          return raw.getBytes(StandardCharsets.UTF_8);
        case DATE:
          return new Instant(java.time.LocalDate.parse(raw.trim()).toEpochDay() * 86400000L);
        case TIMESTAMP:
          return new Instant(java.time.Instant.parse(raw.trim()).toEpochMilli());
        case ARRAY:
          // Array generators are not supported; element-level expressions would require a
          // different config shape. Fall back to the random array path.
          return null;
        default:
          return raw;
      }
    } catch (NumberFormatException | IllegalArgumentException | java.time.DateTimeException e) {
      throw new RuntimeException(
          "Generator output for column '"
              + column.name()
              + "' did not parse as "
              + type
              + ": got '"
              + raw
              + "' from expression '"
              + column.generator()
              + "'",
          e);
    }
  }

  /**
   * Returns a {@code [min, max)} range for INT64 generation that fits the declared source type.
   *
   * <p>MySQL maps TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT and YEAR all to {@link LogicalType#INT64},
   * but the underlying column widths differ dramatically — a ~1B value overflows everything
   * smaller than INT and all YEAR columns. This helper picks a conservative positive sub-range
   * per declared type. Spanner's INT64 and unknown types fall through to the wide range.
   *
   * <p>Returned as a two-element array {@code [minInclusive, maxExclusive]} matching the contract
   * of {@code faker.number().numberBetween(long, long)}.
   */
  static long[] int64RangeFor(String originalType) {
    if (originalType == null) {
      return new long[] {1_000_000_000L, 2_147_483_647L};
    }
    String t = originalType.trim().toUpperCase();
    // Strip parameters like "(10)" and any trailing modifiers ("UNSIGNED", "ZEROFILL").
    int paren = t.indexOf('(');
    if (paren >= 0) {
      t = t.substring(0, paren);
    }
    t = t.trim();
    // The MySQL YEAR column only accepts 1901..2155 (and 0).
    if (t.equals("YEAR")) {
      return new long[] {1901L, 2156L};
    }
    if (t.equals("TINYINT")) {
      return new long[] {0L, 128L};
    }
    if (t.equals("SMALLINT")) {
      return new long[] {0L, 32_768L};
    }
    if (t.equals("MEDIUMINT")) {
      return new long[] {0L, 8_388_608L};
    }
    if (t.equals("INT") || t.equals("INTEGER")) {
      return new long[] {0L, 2_147_483_647L};
    }
    // BIGINT, Spanner INT64, or anything unrecognized.
    return new long[] {1_000_000_000L, 2_147_483_647L};
  }

  /**
   * Generates a {@link BigDecimal} that fits the column's declared precision and scale. Avoids
   * {@code new BigDecimal(faker.number().randomNumber())} which returns an unbounded integer
   * value and overflows most DECIMAL/NUMERIC columns (e.g. {@code DECIMAL(5,2)} tops out at
   * {@code 999.99}).
   *
   * <p>Defaults are precision 10 / scale 2 when the fetcher did not populate them — safe for
   * MySQL {@code DECIMAL} (default precision is 10) and Spanner {@code NUMERIC} (up to 38
   * precision, 9 scale).
   */
  static BigDecimal generateNumeric(DataGeneratorColumn column, Faker faker) {
    int prec = (column.precision() != null && column.precision() > 0) ? column.precision() : 10;
    int sc = (column.scale() != null && column.scale() >= 0) ? column.scale() : 2;
    if (sc > prec) {
      sc = prec;
    }
    int intDigits = prec - sc;
    // Cap to 18 digits so the randomised upper bound stays within {@code long}.
    int capIntDigits = Math.min(intDigits, 18);
    long maxIntPart = 1L;
    for (int i = 0; i < capIntDigits; i++) {
      maxIntPart *= 10L;
    }
    // numberBetween(a, b) is [a, b) and requires b > a.
    long intPart = maxIntPart <= 1L ? 0L : faker.number().numberBetween(0L, maxIntPart);
    BigDecimal value = BigDecimal.valueOf(intPart);
    if (sc > 0) {
      int capScaleDigits = Math.min(sc, 18);
      long maxFrac = 1L;
      for (int i = 0; i < capScaleDigits; i++) {
        maxFrac *= 10L;
      }
      long fracPart = maxFrac <= 1L ? 0L : faker.number().numberBetween(0L, maxFrac);
      value = value.add(BigDecimal.valueOf(fracPart, capScaleDigits));
    }
    // Normalise the trailing scale so the returned value always reports the declared scale.
    return value.setScale(sc, java.math.RoundingMode.HALF_UP);
  }

  public static Schema.FieldType mapToBeamFieldType(LogicalType logicalType) {
    return mapToBeamFieldType(logicalType, null);
  }

  /**
   * Column-aware overload — preferred when the caller has the column in hand because it preserves
   * ARRAY element type information so the generated Beam Row matches the actual element type
   * (instead of degrading to {@code iterable<STRING>}).
   */
  public static Schema.FieldType mapToBeamFieldType(DataGeneratorColumn col) {
    return mapToBeamFieldType(col.logicalType(), col.elementType());
  }

  public static Schema.FieldType mapToBeamFieldType(
      LogicalType logicalType, LogicalType elementType) {
    switch (logicalType) {
      case STRING:
      case JSON:
      case UUID:
      case ENUM:
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
      case ARRAY:
        if (elementType == null) {
          elementType = LogicalType.STRING;
        }
        return Schema.FieldType.iterable(mapToBeamFieldType(elementType, null));
      default:
        return Schema.FieldType.STRING;
    }
  }
}
