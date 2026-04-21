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
package com.google.cloud.teleport.v2.templates.mysql;

import com.google.cloud.teleport.v2.templates.common.TypeMapper;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import java.util.Locale;

/** TypeMapper implementation for JDBC types. */
public class MySqlTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(String typeName, SinkDialect dialect) {
    // This method signature is kept for interface compatibility,
    // but the size is needed for TINYINT handling.
    // Call the overloaded method with a default size.
    return getLogicalType(typeName, dialect, null);
  }

  public LogicalType getLogicalType(String typeName, SinkDialect dialect, Long size) {
    if (typeName == null) {
      return LogicalType.STRING;
    }

    String upperType = typeName.toUpperCase(Locale.ROOT);
    // Remove parameters
    if (upperType.contains("(")) {
      upperType = upperType.substring(0, upperType.indexOf("("));
    }

    switch (upperType) {
      case "BIT":
      case "BOOLEAN":
      case "BOOL":
        return LogicalType.BOOLEAN;

      case "TINYINT":
        // Tip: If type == "TINYINT" AND length == 1, return LogicalType.BOOLEAN.
        return (size != null && size == 1) ? LogicalType.BOOLEAN : LogicalType.INT64;
      case "SMALLINT":
      case "MEDIUMINT": // Added
      case "INTEGER":
      case "INT":
      case "BIGINT":
      case "YEAR": // Added - safer to treat as INT64 for generation
        return LogicalType.INT64;

      case "FLOAT":
      case "REAL":
      case "DOUBLE":
        return LogicalType.FLOAT64; // Separate from Fixed-Point

      case "NUMERIC":
      case "DECIMAL":
        return LogicalType.NUMERIC;

      case "CHAR":
      case "VARCHAR":
      case "LONGVARCHAR":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
        return LogicalType.STRING;
      case "ENUM":
      case "SET":
        // SET stores a comma-separated subset of an enum-like value list. A *single* member is
        // always a valid SET value, so we reuse the ENUM generator (pick-one-from-list) to emit
        // safe SET values without needing a dedicated logical type.
        return LogicalType.ENUM;

      case "BINARY":
      case "VARBINARY":
      case "LONGVARBINARY":
      case "BLOB":
      case "MEDIUMBLOB": // Added
      case "LONGBLOB": // Added
      case "TINYBLOB": // Added
        return LogicalType.BYTES;

      case "DATE":
        return LogicalType.DATE;

      case "TIME": // Note: May need a specific TIME type if engine supports it
      case "TIMESTAMP":
      case "DATETIME":
        return LogicalType.TIMESTAMP;

      case "JSON":
        return LogicalType.JSON;

      default:
        return LogicalType.STRING;
    }
  }
}
