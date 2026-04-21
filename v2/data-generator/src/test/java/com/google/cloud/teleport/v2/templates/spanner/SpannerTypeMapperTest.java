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
package com.google.cloud.teleport.v2.templates.spanner;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import org.junit.Test;

public class SpannerTypeMapperTest {

  private final SpannerTypeMapper mapper = new SpannerTypeMapper();

  @Test
  public void testGoogleStandardSqlMapping() {
    // Basic types
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "BOOL", LogicalType.BOOLEAN);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "INT64", LogicalType.INT64);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "FLOAT64", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "FLOAT32", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "STRING", LogicalType.STRING);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "BYTES", LogicalType.BYTES);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "DATE", LogicalType.DATE);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "TIMESTAMP", LogicalType.TIMESTAMP);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "NUMERIC", LogicalType.NUMERIC);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "BIGNUMERIC", LogicalType.NUMERIC);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "JSON", LogicalType.JSON);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "UUID", LogicalType.UUID);

    // Parameterized types
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "STRING(MAX)", LogicalType.STRING);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "STRING(100)", LogicalType.STRING);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "BYTES(MAX)", LogicalType.BYTES);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "BYTES(100)", LogicalType.BYTES);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "NUMERIC(10,2)", LogicalType.NUMERIC);

    // Complex types (sanitization strips parameters)
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "ARRAY<INT64>", LogicalType.ARRAY);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "ARRAY<STRING(MAX)>", LogicalType.ARRAY);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "STRUCT<a INT64>", LogicalType.STRUCT);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "STRUCT<a INT64, b STRING>", LogicalType.STRUCT);

    // Case insensitivity
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "bool", LogicalType.BOOLEAN);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "Int64", LogicalType.INT64);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "String(Max)", LogicalType.STRING);

    // Unknown/Null
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, "UNKNOWN", LogicalType.STRING);
    verifyMapping(SinkDialect.GOOGLE_STANDARD_SQL, null, LogicalType.STRING);
  }

  @Test
  public void testPostgreSqlMapping() {
    verifyMapping(SinkDialect.POSTGRESQL, "boolean", LogicalType.BOOLEAN);
    verifyMapping(SinkDialect.POSTGRESQL, "bool", LogicalType.BOOLEAN);
    verifyMapping(SinkDialect.POSTGRESQL, "bigint", LogicalType.INT64);
    verifyMapping(SinkDialect.POSTGRESQL, "int8", LogicalType.INT64);
    verifyMapping(SinkDialect.POSTGRESQL, "real", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.POSTGRESQL, "double precision", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.POSTGRESQL, "float4", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.POSTGRESQL, "float8", LogicalType.FLOAT64);
    verifyMapping(SinkDialect.POSTGRESQL, "varchar", LogicalType.STRING);
    verifyMapping(SinkDialect.POSTGRESQL, "text", LogicalType.STRING);
    verifyMapping(SinkDialect.POSTGRESQL, "character varying", LogicalType.STRING);
    verifyMapping(SinkDialect.POSTGRESQL, "bytea", LogicalType.BYTES);
    verifyMapping(SinkDialect.POSTGRESQL, "date", LogicalType.DATE);
    verifyMapping(SinkDialect.POSTGRESQL, "timestamp with time zone", LogicalType.TIMESTAMP);
    verifyMapping(SinkDialect.POSTGRESQL, "timestamptz", LogicalType.TIMESTAMP);
    verifyMapping(SinkDialect.POSTGRESQL, "numeric", LogicalType.NUMERIC);
    verifyMapping(SinkDialect.POSTGRESQL, "decimal", LogicalType.NUMERIC);
    verifyMapping(SinkDialect.POSTGRESQL, "jsonb", LogicalType.JSON);
    verifyMapping(SinkDialect.POSTGRESQL, "uuid", LogicalType.UUID);

    // Parameterized PG types
    verifyMapping(SinkDialect.POSTGRESQL, "varchar(255)", LogicalType.STRING);
    verifyMapping(SinkDialect.POSTGRESQL, "numeric(10,2)", LogicalType.NUMERIC);

    // Case insensitivity
    verifyMapping(SinkDialect.POSTGRESQL, "VARCHAR", LogicalType.STRING);
    verifyMapping(SinkDialect.POSTGRESQL, "BigInt", LogicalType.INT64);

    verifyMapping(SinkDialect.POSTGRESQL, "unknown", LogicalType.STRING);
  }

  private void verifyMapping(SinkDialect dialect, String inputType, LogicalType expected) {
    assertEquals(
        "Failed for type: " + inputType, expected, mapper.getLogicalType(inputType, dialect));
  }
}
