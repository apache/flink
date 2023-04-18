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

import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.jdbc.utils.StringDataConverter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicReference;

/* Statement in flink jdbc driver. */
public class FlinkStatement extends BaseStatement {
    private final Connection connection;
    private final Executor executor;
    private final AtomicReference<FlinkResultSet> currentResults;
    private volatile boolean closed;

    public FlinkStatement(FlinkConnection connection) {
        this.connection = connection;
        this.executor = connection.getExecutor();
        this.currentResults = new AtomicReference<>();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException(String.format("Statement[%s] is not a query.", sql));
        }

        return currentResults.get();
    }

    private void clearCurrentResults() throws SQLException {
        FlinkResultSet resultSet = currentResults.get();
        if (resultSet == null) {
            return;
        }
        resultSet.close();
        currentResults.set(null);
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        cancel();
        closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        clearCurrentResults();
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        clearCurrentResults();

        StatementResult result = executor.executeStatement(sql);
        FlinkResultSet resultSet = new FlinkResultSet(this, result, StringDataConverter.CONVERTER);
        currentResults.set(resultSet);

        return resultSet.isQuery() || result.getResultKind() == ResultKind.SUCCESS_WITH_CONTENT;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        FlinkResultSet resultSet = currentResults.get();
        if (resultSet == null) {
            throw new SQLException("No result set in the current statement.");
        }
        if (!resultSet.isQuery()) {
            throw new SQLException("Result set is not query");
        }
        if (resultSet.isClosed()) {
            throw new SQLException("Result set has been closed");
        }

        return resultSet;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();

        FlinkResultSet resultSet = currentResults.get();
        if (resultSet != null) {
            cancel();
            return false;
        }

        throw new SQLFeatureNotSupportedException("Multiple open results not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
}
