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
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.jdbc.utils.DriverUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.table.jdbc.utils.DriverUtils.checkArgument;

/**
 * Connection to flink sql gateway for jdbc driver. Notice that the connection is not thread safe.
 */
@NotThreadSafe
public class FlinkConnection extends BaseConnection {
    private final String url;
    private final Executor executor;
    private final List<Statement> statements;
    private boolean closed = false;

    public FlinkConnection(DriverUri driverUri) {
        this.url = driverUri.getURL();
        this.statements = new ArrayList<>();
        this.executor =
                Executor.create(
                        new DefaultContext(
                                DriverUtils.fromProperties(driverUri.getProperties()),
                                Collections.emptyList()),
                        driverUri.getAddress(),
                        UUID.randomUUID().toString(),
                        RowFormat.JSON);
        driverUri.getCatalog().ifPresent(this::setSessionCatalog);
        driverUri.getDatabase().ifPresent(this::setSessionSchema);
    }

    @VisibleForTesting
    FlinkConnection(Executor executor) {
        this.url = null;
        this.statements = new ArrayList<>();
        this.executor = executor;
    }

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        final Statement statement = new FlinkStatement(this);
        statements.add(statement);
        return statement;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {}

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @VisibleForTesting
    Executor getExecutor() {
        return this.executor;
    }

    private void ensureOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("The current connection is closed.");
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        List<Statement> remainStatements = new ArrayList<>(statements);
        for (Statement statement : remainStatements) {
            statement.close();
        }
        remainStatements.clear();
        statements.clear();

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
        ensureOpen();
        return new FlinkDatabaseMetaData(url, this, createStatement());
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureOpen();
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
        ensureOpen();
        try (StatementResult result = executor.executeStatement("SHOW CURRENT CATALOG;")) {
            if (result.hasNext()) {
                String catalog = result.next().getString(0).toString();
                checkArgument(!result.hasNext(), "There are more than one current catalog.");
                return catalog;
            } else {
                throw new SQLException("No catalog");
            }
        }
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void setTransactionIsolation(int level) throws SQLException {}

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            ensureOpen();
        } catch (SQLException e) {
            throw new SQLClientInfoException("Connection is closed", new HashMap<>(), e);
        }
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
        ensureOpen();
        Map<String, String> configuration = executor.getSessionConfigMap();
        return configuration.get(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        ensureOpen();
        Properties properties = new Properties();
        Map<String, String> configuration = executor.getSessionConfigMap();
        configuration.forEach(properties::setProperty);
        return properties;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        ensureOpen();
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
        ensureOpen();
        try (StatementResult result = executor.executeStatement("SHOW CURRENT DATABASE;")) {
            if (result.hasNext()) {
                String schema = result.next().getString(0).toString();
                checkArgument(!result.hasNext(), "There are more than one current database.");
                return schema;
            } else {
                throw new SQLException("No database");
            }
        }
    }

    void removeStatement(FlinkStatement statement) {
        statements.remove(statement);
    }
}
