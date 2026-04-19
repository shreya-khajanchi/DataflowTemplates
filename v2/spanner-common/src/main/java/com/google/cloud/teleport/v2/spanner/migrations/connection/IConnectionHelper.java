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

/**
 * Contract for a per-worker singleton connection helper that exposes pooled connections keyed by a
 * {@code connectionRequestKey}.
 *
 * @param <T> the connection type handed out by {@link #getConnection(String)}.
 */
public interface IConnectionHelper<T> {

  /** Initializes the underlying pool(s) from the given request. */
  void init(ConnectionHelperRequest connectionHelperRequest);

  /**
   * Returns a connection for the pool identified by {@code connectionRequestKey}.
   *
   * @throws ConnectionException if the pool is uninitialized, missing, or connection acquisition
   *     fails.
   */
  T getConnection(String connectionRequestKey) throws ConnectionException;

  /** Returns whether the underlying pool(s) have been initialized. */
  boolean isConnectionPoolInitialized();
}
