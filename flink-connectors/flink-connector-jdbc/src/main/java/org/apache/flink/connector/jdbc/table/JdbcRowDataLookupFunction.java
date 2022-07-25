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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A lookup function for {@link JdbcDynamicTableSource}. */
@Internal
public class JdbcRowDataLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupFunction.class);
    private static final long serialVersionUID = 2L;

    private final String query;
    private final JdbcConnectionProvider connectionProvider;
    private final String[] keyNames;
    private final int maxRetryTimes;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private transient FieldNamedPreparedStatement statement;

    public JdbcRowDataLookupFunction(
            JdbcConnectorOptions options,
            int maxRetryTimes,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.connectionProvider = new SimpleJdbcConnectionProvider(options);
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        DataType[] keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.maxRetryTimes = maxRetryTimes;
        this.query =
                options.getDialect()
                        .getSelectFromStatement(options.getTableName(), fieldNames, keyNames);
        String dbURL = options.getDbURL();
        JdbcDialect jdbcDialect = JdbcDialectLoader.load(dbURL);
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter =
                jdbcDialect.getRowConverter(
                        RowType.of(
                                Arrays.stream(keyTypes)
                                        .map(DataType::getLogicalType)
                                        .toArray(LogicalType[]::new)));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            establishConnectionAndStatement();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keyRow lookup keys
     */
    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        rows.add(row);
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error(
                            "JDBC connection is not valid, and reestablish connection failed",
                            exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection dbConn = connectionProvider.getOrEstablishConnection();
        statement = FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
    }

    @Override
    public void close() throws IOException {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        connectionProvider.closeConnection();
    }

    @VisibleForTesting
    public Connection getDbConnection() {
        return connectionProvider.getConnection();
    }
}
