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
package com.google.cloud.teleport.v2.templates.model;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/** Represents a table in the data generator schema. */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class DataGeneratorTable implements Serializable {

  /** The name of the table. */
  public abstract String name();

  /** The columns of the table. */
  public abstract ImmutableList<DataGeneratorColumn> columns();

  /** The primary key column names. */
  public abstract ImmutableList<String> primaryKeys();

  /** The name of the table this table is interleaved in, if any (Spanner specific). */
  @Nullable
  public abstract String interleavedInTable();

  /** Foreign keys defined on this table. */
  public abstract ImmutableList<DataGeneratorForeignKey> foreignKeys();

  /** Unique keys/indexes defined on this table. */
  public abstract ImmutableList<DataGeneratorUniqueKey> uniqueKeys();

  /** The target Insert QPS for this table. */
  public abstract int insertQps();

  /**
   * The target Update QPS for this table, or {@code null} to inherit the pipeline-global update
   * QPS. An explicit {@code 0} means "never generate updates for this table" and must NOT be
   * treated as "unset".
   */
  @Nullable
  public abstract Integer updateQps();

  /**
   * The target Delete QPS for this table, or {@code null} to inherit the pipeline-global delete
   * QPS. An explicit {@code 0} means "never generate deletes for this table" and must NOT be
   * treated as "unset".
   */
  @Nullable
  public abstract Integer deleteQps();

  /** Whether this table is a root table. */
  public abstract boolean isRoot();

  /** The list of child tables. */
  public abstract ImmutableList<String> childTables();

  /**
   * Depth of this table in the parent→child DAG produced by {@code SchemaUtils.setSchemaDAG}. A
   * root has depth {@code 0}; direct children have depth {@code 1}; grandchildren {@code 2}; etc.
   * When a table has multiple ancestors, its depth is the longest chain length from any root.
   */
  public abstract int depth();

  public static Builder builder() {
    return new AutoValue_DataGeneratorTable.Builder()
        .insertQps(1)
        .updateQps(null)
        .deleteQps(null)
        .isRoot(true)
        .depth(0)
        .childTables(ImmutableList.of());
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder columns(ImmutableList<DataGeneratorColumn> columns);

    public abstract Builder primaryKeys(ImmutableList<String> primaryKeys);

    public abstract Builder interleavedInTable(@Nullable String interleavedInTable);

    public abstract Builder foreignKeys(ImmutableList<DataGeneratorForeignKey> foreignKeys);

    public abstract Builder uniqueKeys(ImmutableList<DataGeneratorUniqueKey> uniqueKeys);

    public abstract Builder insertQps(int insertQps);

    public abstract Builder updateQps(@Nullable Integer updateQps);

    public abstract Builder deleteQps(@Nullable Integer deleteQps);

    public abstract Builder isRoot(boolean isRoot);

    public abstract Builder childTables(ImmutableList<String> childTables);

    public abstract Builder depth(int depth);

    public abstract DataGeneratorTable build();
  }
}
