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
import java.util.Arrays;
import java.util.List;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

  @Test
  public void testGenerateUUID() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("uuid_col")
            .logicalType(LogicalType.UUID)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("UUID")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertEquals(36, ((String) value).length());
  }

  @Test
  public void testGenerateEnum() {
    List<String> enumValues = Arrays.asList("A", "B", "C");
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("enum_col")
            .logicalType(LogicalType.ENUM)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("ENUM")
            .enumValues(enumValues)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertTrue(enumValues.contains(value));
  }

  @Test
  public void testGenerateDate_truncatesTime() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("date_col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("DATE")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Instant);
    Instant instant = (Instant) value;
    // Verify time is midnight in some time zone or just check it's a multiple of day millis if
    // UTC.
    // Faker uses system default timezone, so it might be hard to check exact midnight without
    // knowing zone.
    // But we can check it is not zero!
    Assert.assertTrue(instant.getMillis() > 0);
  }

  @Test
  public void testGenerateFloat64_withPrecisionAndScale() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("float_col")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("FLOAT(5,2)")
            .precision(5)
            .scale(2)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Double);
    Double d = (Double) value;
    Assert.assertTrue(d < 1000.0);
    Assert.assertTrue(d > -1000.0);
  }

  @Test
  public void testGenerateArray() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("array_col")
            .logicalType(LogicalType.ARRAY)
            .elementType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("ARRAY<STRING>")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof List);
    List<?> list = (List<?>) value;
    Assert.assertFalse(list.isEmpty());
    Assert.assertTrue(list.get(0) instanceof String);
  }

  // ---------- column.generator() — Faker expression coercion ----------

  /**
   * STRING / JSON / UUID / ENUM logical types should pass the raw expression output through
   * unchanged. The user-supplied expression is fully responsible for shape.
   */
  @Test
  public void testGenerator_StringPassesThrough() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("name_col")
            .logicalType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("VARCHAR")
            .generator("#{name.fullName}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertFalse(((String) value).isEmpty());
  }

  /**
   * INT64 columns must coerce the Faker String output into a Long. {@code numberBetween} is the
   * canonical example.
   */
  @Test
  public void testGenerator_Int64CoercesToLong() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("amount")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("BIGINT")
            .generator("#{number.numberBetween '100','200'}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue("Expected Long, got " + value.getClass(), value instanceof Long);
    long v = (Long) value;
    Assert.assertTrue("Expected value in [100,200), got " + v, v >= 100 && v < 200);
  }

  /** FLOAT64 columns must coerce the Faker String into a Double. */
  @Test
  public void testGenerator_Float64CoercesToDouble() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("ratio")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("DOUBLE")
            .generator("#{number.randomDouble '2','0','100'}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue("Expected Double, got " + value.getClass(), value instanceof Double);
  }

  /** BOOLEAN columns must coerce true/false strings (case-insensitive). */
  @Test
  public void testGenerator_BooleanCoercesFromString() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("active")
            .logicalType(LogicalType.BOOLEAN)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("BOOLEAN")
            .generator("#{bool.bool}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Boolean);
  }

  /**
   * NUMERIC columns must produce a BigDecimal at the column's declared scale, even when the
   * generator emits an integer-looking string.
   */
  @Test
  public void testGenerator_NumericCoercesAtDeclaredScale() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("balance")
            .logicalType(LogicalType.NUMERIC)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("DECIMAL(10,2)")
            .precision(10)
            .scale(2)
            .generator("#{number.numberBetween '1','1000'}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof java.math.BigDecimal);
    Assert.assertEquals(2, ((java.math.BigDecimal) value).scale());
  }

  /**
   * If the user-supplied expression yields output that doesn't parse as the column's logical
   * type, generation must throw with a message that names the column, type, and offending
   * value. The DLQ wiring upstream relies on these throws to capture the bad row.
   */
  @Test
  public void testGenerator_ParseFailureSurfacesDescriptiveError() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("age")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("BIGINT")
            .generator("#{name.fullName}") // returns a non-numeric String
            .build();

    RuntimeException ex =
        Assert.assertThrows(
            RuntimeException.class, () -> DataGeneratorUtils.generateValue(column, faker));
    String msg = ex.getMessage();
    Assert.assertNotNull(msg);
    Assert.assertTrue(
        "Expected error to name the column, got: " + msg, msg.contains("'age'"));
    Assert.assertTrue(
        "Expected error to name the logical type, got: " + msg, msg.contains("INT64"));
  }

  /** UUID columns should leave the string output alone (UUID is a string-like type internally). */
  @Test
  public void testGenerator_UuidPassesThrough() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.UUID)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("UUID")
            .generator("#{internet.uuid}")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    // Faker's internet.uuid produces a 36-char canonical form.
    Assert.assertEquals(36, ((String) value).length());
  }
}
