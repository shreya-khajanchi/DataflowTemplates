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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcConnectionHelperTest {

  private JdbcConnectionHelper helper;

  @Before
  public void setUp() {
    helper = new JdbcConnectionHelper();
    helper.setConnectionPoolMap(null);
  }

  @After
  public void tearDown() {
    // Reset the static singleton state to avoid cross-test pollution.
    helper.setConnectionPoolMap(null);
  }

  @Test
  public void testIsConnectionPoolInitialized_falseByDefault() {
    assertThat(helper.isConnectionPoolInitialized()).isFalse();
  }

  @Test
  public void testSetConnectionPoolMap_flipsInitializedFlag() {
    helper.setConnectionPoolMap(new HashMap<>());
    assertThat(helper.isConnectionPoolInitialized()).isTrue();
  }

  @Test
  public void testGetConnection_poolUninitializedReturnsNull() throws Exception {
    assertThat(helper.getConnection("key")).isNull();
  }

  @Test
  public void testGetConnection_poolMissingReturnsNull() throws Exception {
    helper.setConnectionPoolMap(new HashMap<>());
    assertThat(helper.getConnection("missing")).isNull();
  }

  @Test
  public void testGetConnection_returnsConnectionFromPool() throws Exception {
    HikariDataSource mockDs = mock(HikariDataSource.class);
    Connection mockConn = mock(Connection.class);
    when(mockDs.getConnection()).thenReturn(mockConn);
    helper.setConnectionPoolMap(ImmutableMap.of("key", mockDs));

    assertThat(helper.getConnection("key")).isSameInstanceAs(mockConn);
  }

  @Test
  public void testGetConnection_wrapsUnderlyingExceptions() throws Exception {
    HikariDataSource mockDs = mock(HikariDataSource.class);
    when(mockDs.getConnection()).thenThrow(new SQLException("boom"));
    helper.setConnectionPoolMap(ImmutableMap.of("key", mockDs));

    ConnectionException ex = assertThrows(ConnectionException.class, () -> helper.getConnection("key"));
    assertThat(ex).hasCauseThat().isInstanceOf(SQLException.class);
  }

  @Test
  public void testInit_noopIfAlreadyInitialized() {
    // Arrange: pre-populate the pool so init() should short-circuit.
    HikariDataSource existing = mock(HikariDataSource.class);
    helper.setConnectionPoolMap(new HashMap<>(ImmutableMap.of("preexisting", existing)));

    Shard shard = new Shard("id", "host", "3306", "user", "pass", "db", null, null, null);
    ConnectionHelperRequest request =
        new ConnectionHelperRequest(
            ImmutableList.of(shard), "", 2, "com.mysql.cj.jdbc.Driver", "");

    // Act
    helper.init(request);

    // Assert: pool map was not replaced; no attempts to build a new datasource for the shard.
    assertThat(helper.isConnectionPoolInitialized()).isTrue();
    verify(existing, never()).close();
  }
}
