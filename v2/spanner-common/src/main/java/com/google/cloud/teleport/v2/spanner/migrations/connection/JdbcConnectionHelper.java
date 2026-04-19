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

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-Dataflow-worker singleton that maintains HikariCP connection pools for one or more JDBC
 * shards.
 *
 * <p>Pools are keyed by {@code <jdbcUrl>/<userName>} and are created once on first {@link
 * #init(ConnectionHelperRequest)}; subsequent {@code init} calls are no-ops.
 */
public class JdbcConnectionHelper implements IConnectionHelper<Connection> {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionHelper.class);
  private static final String DEFAULT_JDBC_URL_PREFIX = "jdbc:mysql://";
  private static Map<String, HikariDataSource> connectionPoolMap = null;

  @Override
  public synchronized boolean isConnectionPoolInitialized() {
    return connectionPoolMap != null;
  }

  @Override
  public synchronized void init(ConnectionHelperRequest connectionHelperRequest) {
    if (connectionPoolMap != null) {
      return;
    }
    LOG.info(
        "Initializing connection pool with size: {}", connectionHelperRequest.getMaxConnections());
    connectionPoolMap = new HashMap<>();
    String urlPrefix =
        connectionHelperRequest.getJdbcUrlPrefix() != null
            ? connectionHelperRequest.getJdbcUrlPrefix()
            : DEFAULT_JDBC_URL_PREFIX;
    for (Shard shard : connectionHelperRequest.getShards()) {
      String sourceConnectionUrl =
          urlPrefix + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(sourceConnectionUrl);
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName(connectionHelperRequest.getDriver());
      config.setMaximumPoolSize(connectionHelperRequest.getMaxConnections());
      config.setConnectionInitSql(connectionHelperRequest.getConnectionInitQuery());
      config.setInitializationFailTimeout(-1); // do not fail during pool construction
      config.setMinimumIdle(0); // avoid pre-filling connections
      Properties jdbcProperties = new Properties();
      if (shard.getConnectionProperties() != null && !shard.getConnectionProperties().isEmpty()) {
        try (StringReader reader = new StringReader(shard.getConnectionProperties())) {
          jdbcProperties.load(reader);
        } catch (IOException e) {
          LOG.error("Error converting string to properties: {}", e.getMessage());
        }
      }

      for (String key : jdbcProperties.stringPropertyNames()) {
        String value = jdbcProperties.getProperty(key);
        config.addDataSourceProperty(key, value);
      }
      HikariDataSource ds = new HikariDataSource(config);
      connectionPoolMap.put(sourceConnectionUrl + "/" + shard.getUserName(), ds);
    }
  }

  @Override
  public Connection getConnection(String connectionRequestKey) throws ConnectionException {
    try {
      if (connectionPoolMap == null) {
        LOG.warn("Connection pool not initialized");
        return null;
      }
      HikariDataSource ds = connectionPoolMap.get(connectionRequestKey);
      if (ds == null) {
        LOG.warn("Connection pool not found for source connection : {}", connectionRequestKey);
        return null;
      }
      return ds.getConnection();
    } catch (Exception e) {
      throw new ConnectionException(e);
    }
  }

  /** Visible for unit testing only - allows tests to inject a mocked pool map. */
  @VisibleForTesting
  public void setConnectionPoolMap(Map<String, HikariDataSource> inputMap) {
    connectionPoolMap = inputMap;
  }
}
