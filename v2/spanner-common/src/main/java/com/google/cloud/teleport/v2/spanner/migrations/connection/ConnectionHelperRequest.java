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
package com.google.cloud.teleport.v2.spanner.migrations.connection;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.List;

/**
 * Represents a request to initialize a connection helper with the necessary parameters.
 *
 * <p>This class encapsulates the essential information required for establishing connections to a
 * database or a data source. It includes:
 *
 * <ul>
 *   <li>A list of {@link Shard} objects representing the database shards.
 *   <li>Optional connection properties as a {@link String}.
 *   <li>The maximum number of connections allowed per shard pool.
 *   <li>The JDBC driver class name.
 *   <li>Optional connection initialisation query as a {@link String}.
 *   <li>Optional JDBC URL prefix (for example {@code "jdbc:mysql://"} or {@code
 *       "jdbc:postgresql://"}) used to construct the connection URL. Defaults to {@code
 *       "jdbc:mysql://"} when unset.
 * </ul>
 */
public class ConnectionHelperRequest {
  private final List<Shard> shards;
  private final String properties;
  private final int maxConnections;
  private final String driver;
  private final String connectionInitQuery;
  private final String jdbcUrlPrefix;

  public ConnectionHelperRequest(
      List<Shard> shards,
      String properties,
      int maxConnections,
      String driver,
      String connectionInitQuery) {
    this(shards, properties, maxConnections, driver, connectionInitQuery, null);
  }

  public ConnectionHelperRequest(
      List<Shard> shards,
      String properties,
      int maxConnections,
      String driver,
      String connectionInitQuery,
      String jdbcUrlPrefix) {
    this.shards = shards;
    this.properties = properties;
    this.maxConnections = maxConnections;
    this.driver = driver;
    this.connectionInitQuery = connectionInitQuery;
    this.jdbcUrlPrefix = jdbcUrlPrefix;
  }

  public List<Shard> getShards() {
    return shards;
  }

  public String getProperties() {
    return properties;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public String getDriver() {
    return driver;
  }

  public String getConnectionInitQuery() {
    return connectionInitQuery;
  }

  /**
   * Returns the JDBC URL prefix (e.g. {@code "jdbc:mysql://"} or {@code "jdbc:postgresql://"}). May
   * be {@code null}, in which case callers should default to {@code "jdbc:mysql://"}.
   */
  public String getJdbcUrlPrefix() {
    return jdbcUrlPrefix;
  }
}
