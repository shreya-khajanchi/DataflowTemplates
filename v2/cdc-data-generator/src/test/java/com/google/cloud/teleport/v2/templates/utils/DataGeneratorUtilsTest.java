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

import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.math.BigDecimal;
import java.util.Random;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataGeneratorUtils}. */
@RunWith(JUnit4.class)
public class DataGeneratorUtilsTest {

  /** Deterministic Faker so generated values don't make tests flaky. */
  private final Faker faker = new Faker(new Random(42L));

  // ===========================================================================
  // mapToBeamFieldType
  // ===========================================================================

  @Test
  public void mapToBeamFieldType_string() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.STRING))
        .isEqualTo(Schema.FieldType.STRING);
  }

  @Test
  public void mapToBeamFieldType_jsonMapsToString() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.JSON))
        .isEqualTo(Schema.FieldType.STRING);
  }

  @Test
  public void mapToBeamFieldType_int64() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.INT64))
        .isEqualTo(Schema.FieldType.INT64);
  }

  @Test
  public void mapToBeamFieldType_float64() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.FLOAT64))
        .isEqualTo(Schema.FieldType.DOUBLE);
  }

  @Test
  public void mapToBeamFieldType_numeric() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.NUMERIC))
        .isEqualTo(Schema.FieldType.DECIMAL);
  }

  @Test
  public void mapToBeamFieldType_boolean() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.BOOLEAN))
        .isEqualTo(Schema.FieldType.BOOLEAN);
  }

  @Test
  public void mapToBeamFieldType_bytes() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.BYTES))
        .isEqualTo(Schema.FieldType.BYTES);
  }

  @Test
  public void mapToBeamFieldType_dateAndTimestampUseDateTime() {
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.DATE))
        .isEqualTo(Schema.FieldType.DATETIME);
    assertThat(DataGeneratorUtils.mapToBeamFieldType(LogicalType.TIMESTAMP))
        .isEqualTo(Schema.FieldType.DATETIME);
  }

  // ===========================================================================
  // generateValue — type contract
  // ===========================================================================

  @Test
  public void generateValue_stringRespectsSize() {
    DataGeneratorColumn col = stringColumn("name", 5L);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(String.class);
    assertThat(((String) v).length()).isEqualTo(5);
  }

  @Test
  public void generateValue_stringNullSizeFallsBackToDefault() {
    DataGeneratorColumn col = stringColumn("name", null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(String.class);
    assertThat(((String) v).length()).isEqualTo(DataGeneratorUtils.DEFAULT_STRING_LENGTH);
  }

  @Test
  public void generateValue_int64ReturnsLong() {
    DataGeneratorColumn col = column("id", LogicalType.INT64, null, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(Long.class);
  }

  @Test
  public void generateValue_float64ReturnsDouble() {
    DataGeneratorColumn col = column("score", LogicalType.FLOAT64, null, 8, 2);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(Double.class);
  }

  @Test
  public void generateValue_booleanReturnsBoolean() {
    DataGeneratorColumn col = column("flag", LogicalType.BOOLEAN, null, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(Boolean.class);
  }

  @Test
  public void generateValue_bytesReturnsByteArrayOfRequestedSize() {
    DataGeneratorColumn col = column("payload", LogicalType.BYTES, 8L, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(byte[].class);
    assertThat(((byte[]) v).length).isEqualTo(8);
  }

  @Test
  public void generateValue_dateReturnsInstantWithZeroedTimePart() {
    DataGeneratorColumn col = column("d", LogicalType.DATE, null, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(Instant.class);
    long millis = ((Instant) v).getMillis();
    // DATE rounds to day boundary — milliseconds within the day must be zero.
    assertThat(millis % 1000L).isEqualTo(0);
  }

  @Test
  public void generateValue_timestampReturnsInstant() {
    DataGeneratorColumn col = column("ts", LogicalType.TIMESTAMP, null, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(Instant.class);
  }

  @Test
  public void generateValue_jsonReturnsParseableLookingString() {
    DataGeneratorColumn col = column("payload", LogicalType.JSON, null, null, null);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(String.class);
    String s = (String) v;
    assertThat(s).startsWith("{");
    assertThat(s).endsWith("}");
    assertThat(s).contains("\"id\"");
  }

  @Test
  public void generateValue_numericRespectsPrecisionAndScale() {
    DataGeneratorColumn col = column("amount", LogicalType.NUMERIC, null, 5, 2);
    Object v = DataGeneratorUtils.generateValue(col, faker);
    assertThat(v).isInstanceOf(BigDecimal.class);
    BigDecimal bd = (BigDecimal) v;
    assertThat(bd.scale()).isEqualTo(2);
    assertThat(bd.abs().toPlainString().replace(".", "").replaceAll("^0+", "").length())
        .isAtMost(5);
  }

  // ===========================================================================
  // generateNumeric — defaults + edge cases
  // ===========================================================================

  @Test
  public void generateNumeric_appliesDefaultsWhenColumnHasNonePopulated() {
    DataGeneratorColumn col = column("amount", LogicalType.NUMERIC, null, null, null);
    BigDecimal v = DataGeneratorUtils.generateNumeric(col, faker);
    assertThat(v.scale()).isEqualTo(DataGeneratorUtils.DEFAULT_NUMERIC_SCALE);
  }

  @Test
  public void generateNumeric_capsScaleAtPrecision() {
    // scale > precision is nonsense; helper should clamp scale to precision.
    DataGeneratorColumn col = column("amount", LogicalType.NUMERIC, null, 3, 5);
    BigDecimal v = DataGeneratorUtils.generateNumeric(col, faker);
    assertThat(v.scale()).isEqualTo(3);
  }

  // ===========================================================================
  // clampStringLength
  // ===========================================================================

  @Test
  public void clampStringLength_nullFallsBackToDefault() {
    assertThat(DataGeneratorUtils.clampStringLength(null))
        .isEqualTo(DataGeneratorUtils.DEFAULT_STRING_LENGTH);
  }

  @Test
  public void clampStringLength_zeroOrNegativeFallsBackToDefault() {
    assertThat(DataGeneratorUtils.clampStringLength(0L))
        .isEqualTo(DataGeneratorUtils.DEFAULT_STRING_LENGTH);
    assertThat(DataGeneratorUtils.clampStringLength(-7L))
        .isEqualTo(DataGeneratorUtils.DEFAULT_STRING_LENGTH);
  }

  @Test
  public void clampStringLength_passesThroughReasonableValues() {
    assertThat(DataGeneratorUtils.clampStringLength(42L)).isEqualTo(42);
  }

  @Test
  public void clampStringLength_capsAtIntMax() {
    assertThat(DataGeneratorUtils.clampStringLength((long) Integer.MAX_VALUE + 1L))
        .isEqualTo(Integer.MAX_VALUE);
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private static DataGeneratorColumn stringColumn(String name, Long size) {
    return column(name, LogicalType.STRING, size, null, null);
  }

  private static DataGeneratorColumn column(
      String name, LogicalType type, Long size, Integer precision, Integer scale) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isNullable(false)
        .isSkipped(false)
        .isGenerated(false)
        .size(size)
        .precision(precision)
        .scale(scale)
        .build();
  }
}
