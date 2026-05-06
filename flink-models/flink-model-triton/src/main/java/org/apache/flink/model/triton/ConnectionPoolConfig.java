/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import java.util.Objects;

/**
 * Configuration holder for HTTP connection pool settings used by Triton HTTP clients.
 *
 * <p>This class encapsulates all connection pool related parameters including maximum connections,
 * keep-alive duration, timeouts, and monitoring settings.
 */
public class ConnectionPoolConfig {
    public final int maxIdleConnections;
    public final long keepAliveDurationMs;
    public final int maxTotalConnections;
    public final long connectionTimeoutMs;
    public final boolean reuseEnabled;
    public final boolean monitoringEnabled;

    public ConnectionPoolConfig(
            int maxIdleConnections,
            long keepAliveDurationMs,
            int maxTotalConnections,
            long connectionTimeoutMs,
            boolean reuseEnabled,
            boolean monitoringEnabled) {
        this.maxIdleConnections = maxIdleConnections;
        this.keepAliveDurationMs = keepAliveDurationMs;
        this.maxTotalConnections = maxTotalConnections;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.reuseEnabled = reuseEnabled;
        this.monitoringEnabled = monitoringEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionPoolConfig that = (ConnectionPoolConfig) o;
        return maxIdleConnections == that.maxIdleConnections
                && keepAliveDurationMs == that.keepAliveDurationMs
                && maxTotalConnections == that.maxTotalConnections
                && connectionTimeoutMs == that.connectionTimeoutMs
                && reuseEnabled == that.reuseEnabled
                && monitoringEnabled == that.monitoringEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxIdleConnections,
                keepAliveDurationMs,
                maxTotalConnections,
                connectionTimeoutMs,
                reuseEnabled,
                monitoringEnabled);
    }
}
