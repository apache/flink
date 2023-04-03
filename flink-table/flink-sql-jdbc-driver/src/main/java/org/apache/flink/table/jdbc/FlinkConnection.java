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

package org.apache.flink.table.jdbc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.jdbc.utils.DriverUtils;

import java.sql.DatabaseMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/** Connection to flink sql gateway for jdbc driver. */
public class FlinkConnection extends BaseConnection {
    private final Executor executor;
    private volatile boolean closed = false;

    public FlinkConnection(DriverUri driverUri) {
        // TODO Support default context from map to get gid of flink core for jdbc driver in
        // https://issues.apache.org/jira/browse/FLINK-31687.
        this.executor =
                Executor.create(
                        new DefaultContext(
                                Configuration.fromMap(
                                        DriverUtils.fromProperties(driverUri.getProperties())),
                                Collections.emptyList()),
                        driverUri.getAddress(),
                        UUID.randomUUID().toString());
        driverUri.getCatalog().ifPresent(this::setSessionCatalog);
        driverUri.getDatabase().ifPresent(this::setSessionSchema);
    }

    @Override
    public Statement createStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @VisibleForTesting
    Executor getExecutor() {
        return this.executor;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            this.executor.close();
        } catch (Exception e) {
            throw new SQLException("Close connection fail", e);
        }
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        try {
            setSessionCatalog(catalog);
        } catch (Exception e) {
            throw new SQLException(String.format("Set catalog[%s] fail", catalog), e);
        }
    }

    private void setSessionCatalog(String catalog) {
        executor.configureSession(String.format("USE CATALOG %s;", catalog));
    }

    @Override
    public String getCatalog() throws SQLException {
        try (StatementResult result = executor.executeStatement("SHOW CURRENT CATALOG;")) {
            if (result.hasNext()) {
                return result.next().getString(0).toString();
            } else {
                throw new SQLException("No catalog");
            }
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        executor.configureSession(String.format("SET '%s'='%s';", name, value));
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        for (Object key : properties.keySet()) {
            setClientInfo(key.toString(), properties.getProperty(key.toString()));
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // TODO Executor should return Map<String, String> here to get rid of flink core for jdbc
        // driver in https://issues.apache.org/jira/browse/FLINK-31687.
        Configuration configuration = (Configuration) executor.getSessionConfig();
        return configuration.toMap().get(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        Properties properties = new Properties();
        // TODO Executor should return Map<String, String> here to get rid of flink core for jdbc
        // driver in https://issues.apache.org/jira/browse/FLINK-31687.
        Configuration configuration = (Configuration) executor.getSessionConfig();
        configuration.toMap().forEach(properties::setProperty);
        return properties;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        try {
            setSessionSchema(schema);
        } catch (Exception e) {
            throw new SQLException(String.format("Set schema[%s] fail", schema), e);
        }
    }

    private void setSessionSchema(String schema) {
        executor.configureSession(String.format("USE %s;", schema));
    }

    @Override
    public String getSchema() throws SQLException {
        try (StatementResult result = executor.executeStatement("SHOW CURRENT DATABASE;")) {
            if (result.hasNext()) {
                return result.next().getString(0).toString();
            } else {
                throw new SQLException("No database");
            }
        }
    }
}
