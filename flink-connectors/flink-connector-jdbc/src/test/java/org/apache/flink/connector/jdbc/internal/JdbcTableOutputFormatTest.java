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

package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

/** Tests for the {@link JdbcOutputFormat}. */
public class JdbcTableOutputFormatTest extends JdbcDataTestBase {

    private TableJdbcUpsertOutputFormat format;
    private String[] fieldNames;
    private String[] keyFields;

    @Before
    public void setup() {
        fieldNames = new String[] {"id", "title", "author", "price", "qty"};
        keyFields = new String[] {"id"};
    }

    @Test
    public void testUpsertFormatCloseBeforeOpen() throws Exception {
        JdbcConnectorOptions options =
                JdbcConnectorOptions.builder()
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(fieldNames)
                        .withKeyFields(keyFields)
                        .build();
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(options),
                        dmlOptions,
                        JdbcExecutionOptions.defaults());
        // FLINK-17544: There should be no NPE thrown from this method
        format.close();
    }

    /**
     * Test that the delete executor in {@link TableJdbcUpsertOutputFormat} is updated when {@link
     * JdbcOutputFormat#attemptFlush()} fails.
     */
    @Test
    public void testDeleteExecutorUpdatedOnReconnect() throws Exception {
        // first fail flush from the main executor
        boolean[] exceptionThrown = {false};
        // then record whether the delete executor was updated
        // and check it on the next flush attempt
        boolean[] deleteExecutorPrepared = {false};
        boolean[] deleteExecuted = {false};
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(
                                JdbcConnectorOptions.builder()
                                        .setDBUrl(getDbMetadata().getUrl())
                                        .setTableName(OUTPUT_TABLE)
                                        .build()) {
                            @Override
                            public boolean isConnectionValid() throws SQLException {
                                return false; // trigger reconnect and re-prepare on flush failure
                            }
                        },
                        JdbcExecutionOptions.builder()
                                .withMaxRetries(1)
                                .withBatchIntervalMs(Long.MAX_VALUE) // disable periodic flush
                                .build(),
                        ctx ->
                                new JdbcBatchStatementExecutor<Row>() {

                                    @Override
                                    public void executeBatch() throws SQLException {
                                        if (!exceptionThrown[0]) {
                                            exceptionThrown[0] = true;
                                            throw new SQLException();
                                        }
                                    }

                                    @Override
                                    public void prepareStatements(Connection connection) {}

                                    @Override
                                    public void addToBatch(Row record) {}

                                    @Override
                                    public void closeStatements() {}
                                },
                        ctx ->
                                new JdbcBatchStatementExecutor<Row>() {
                                    @Override
                                    public void prepareStatements(Connection connection) {
                                        if (exceptionThrown[0]) {
                                            deleteExecutorPrepared[0] = true;
                                        }
                                    }

                                    @Override
                                    public void addToBatch(Row record) {}

                                    @Override
                                    public void executeBatch() {
                                        deleteExecuted[0] = true;
                                    }

                                    @Override
                                    public void closeStatements() {}
                                });
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
        doReturn(config).when(context).getExecutionConfig();
        doReturn(true).when(config).isObjectReuseEnabled();
        format.setRuntimeContext(context);
        format.open(0, 1);

        format.writeRecord(Tuple2.of(false /* false = delete*/, toRow(TEST_DATA[0])));
        format.flush();

        assertTrue("Delete should be executed", deleteExecuted[0]);
        assertTrue(
                "Delete executor should be prepared" + exceptionThrown[0],
                deleteExecutorPrepared[0]);
    }

    @Test
    public void testJdbcOutputFormat() throws Exception {
        JdbcConnectorOptions options =
                JdbcConnectorOptions.builder()
                        .setDBUrl(getDbMetadata().getUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(options.getTableName())
                        .withDialect(options.getDialect())
                        .withFieldNames(fieldNames)
                        .withKeyFields(keyFields)
                        .build();
        format =
                new TableJdbcUpsertOutputFormat(
                        new SimpleJdbcConnectionProvider(options),
                        dmlOptions,
                        JdbcExecutionOptions.defaults());
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        ExecutionConfig config = Mockito.mock(ExecutionConfig.class);
        doReturn(config).when(context).getExecutionConfig();
        doReturn(true).when(config).isObjectReuseEnabled();
        format.setRuntimeContext(context);
        format.open(0, 1);

        for (TestEntry entry : TEST_DATA) {
            format.writeRecord(Tuple2.of(true, toRow(entry)));
        }
        format.flush();
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // override
        for (TestEntry entry : TEST_DATA) {
            format.writeRecord(Tuple2.of(true, toRow(entry)));
        }
        format.flush();
        check(Arrays.stream(TEST_DATA).map(JdbcDataTestBase::toRow).toArray(Row[]::new));

        // delete
        for (int i = 0; i < TEST_DATA.length / 2; i++) {
            format.writeRecord(Tuple2.of(false, toRow(TEST_DATA[i])));
        }
        Row[] expected = new Row[TEST_DATA.length - TEST_DATA.length / 2];
        for (int i = TEST_DATA.length / 2; i < TEST_DATA.length; i++) {
            expected[i - TEST_DATA.length / 2] = toRow(TEST_DATA[i]);
        }
        format.flush();
        check(expected);
    }

    private void check(Row[] rows) throws SQLException {
        check(rows, getDbMetadata().getUrl(), OUTPUT_TABLE, fieldNames);
    }

    public static void check(Row[] rows, String url, String table, String[] fields)
            throws SQLException {
        try (Connection dbConn = DriverManager.getConnection(url);
                PreparedStatement statement = dbConn.prepareStatement("select * from " + table);
                ResultSet resultSet = statement.executeQuery()) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                Row row = new Row(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    row.setField(i, resultSet.getObject(fields[i]));
                }
                results.add(row.toString());
            }
            String[] sortedExpect = Arrays.stream(rows).map(Row::toString).toArray(String[]::new);
            String[] sortedResult = results.toArray(new String[0]);
            Arrays.sort(sortedExpect);
            Arrays.sort(sortedResult);
            assertArrayEquals(sortedExpect, sortedResult);
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        if (format != null) {
            format.close();
        }
        format = null;
        Class.forName(getDbMetadata().getDriverClass());
        try (Connection conn = DriverManager.getConnection(getDbMetadata().getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);

            stat.close();
            conn.close();
        }
    }
}
