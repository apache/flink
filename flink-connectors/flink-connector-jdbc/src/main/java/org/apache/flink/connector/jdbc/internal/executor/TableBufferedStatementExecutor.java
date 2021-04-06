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

package org.apache.flink.connector.jdbc.internal.executor;

import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Currently, this statement executor is only used for table/sql to buffer records, because the
 * {@link PreparedStatement#executeBatch()} may fail and clear buffered records, so we have to
 * buffer the records and replay the records when retrying {@link #executeBatch()}.
 */
public final class TableBufferedStatementExecutor implements JdbcBatchStatementExecutor<RowData> {

    private final JdbcBatchStatementExecutor<RowData> statementExecutor;
    private final Function<RowData, RowData> valueTransform;
    private final List<RowData> buffer = new ArrayList<>();

    public TableBufferedStatementExecutor(
            JdbcBatchStatementExecutor<RowData> statementExecutor,
            Function<RowData, RowData> valueTransform) {
        this.statementExecutor = statementExecutor;
        this.valueTransform = valueTransform;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statementExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(RowData record) throws SQLException {
        RowData value = valueTransform.apply(record); // copy or not
        buffer.add(value);
    }

    @Override
    public void executeBatch() throws SQLException {
        for (RowData value : buffer) {
            statementExecutor.addToBatch(value);
        }
        statementExecutor.executeBatch();
        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        statementExecutor.closeStatements();
    }
}
