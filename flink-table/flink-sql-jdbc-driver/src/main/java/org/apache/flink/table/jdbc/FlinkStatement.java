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

import javax.annotation.concurrent.NotThreadSafe;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;

/** Statement for flink jdbc driver. Notice that the statement is not thread safe. */
@NotThreadSafe
public class FlinkStatement extends BaseStatement {
    private final FlinkConnection connection;
    private final Executor executor;
    private FlinkResultSet currentResults;
    private boolean hasResults;
    private boolean closed;

    public FlinkStatement(FlinkConnection connection) {
        this.connection = connection;
        this.executor = connection.getExecutor();
        this.currentResults = null;
    }

    /**
     * Execute a SELECT query.
     *
     * @param sql an SQL statement to be sent to the database, typically a static SQL <code>SELECT
     *     </code> statement
     * @return the select query result set.
     * @throws SQLException the thrown exception
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (!result.isQueryResult()) {
            result.close();
            throw new SQLException(String.format("Statement[%s] is not a query.", sql));
        }
        currentResults = new FlinkResultSet(this, result);
        hasResults = true;

        return currentResults;
    }

    private void clearCurrentResults() throws SQLException {
        if (currentResults == null) {
            return;
        }
        currentResults.close();
        currentResults = null;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        cancel();
        connection.removeStatement(this);
        closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        clearCurrentResults();
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    // TODO We currently do not support this, but we can't throw a SQLException here because we want
    // to support jdbc tools such as beeline and sqlline.
    @Override
    public void clearWarnings() throws SQLException {}

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("This result set is already closed");
        }
    }

    /**
     * Execute a sql statement. Notice that the <code>INSERT</code> statement in Flink would return
     * job id as result set.
     *
     * @param sql any SQL statement
     * @return True if there is result set for the statement.
     * @throws SQLException the thrown exception.
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        StatementResult result = executeInternal(sql);
        if (result.isQueryResult() || result.getResultKind() == ResultKind.SUCCESS_WITH_CONTENT) {
            currentResults = new FlinkResultSet(this, result);
            hasResults = true;
            return true;
        }

        hasResults = false;
        return false;
    }

    private StatementResult executeInternal(String sql) throws SQLException {
        checkClosed();
        clearCurrentResults();

        return executor.executeStatement(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();

        if (currentResults == null) {
            throw new SQLException("No result set in the current statement.");
        }
        if (currentResults.isClosed()) {
            throw new SQLException("Result set has been closed");
        }

        return currentResults;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();

        if (currentResults != null) {
            cancel();
            return false;
        }

        throw new SQLFeatureNotSupportedException("Multiple open results not supported");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (hasResults) {
            throw new SQLFeatureNotSupportedException(
                    "FlinkStatement#getUpdateCount is not supported for query");
        } else {
            return 0;
        }
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
