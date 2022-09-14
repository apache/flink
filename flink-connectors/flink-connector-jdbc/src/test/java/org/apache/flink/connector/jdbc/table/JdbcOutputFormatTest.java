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

import org.apache.flink.connector.jdbc.JdbcDataTestBase;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suite for {@link JdbcOutputFormatBuilder}. */
public class JdbcOutputFormatTest extends JdbcDataTestBase {

    private static JdbcOutputFormat<RowData, ?, ?> outputFormat;
    private static String[] fieldNames = new String[] {"id", "title", "author", "price", "qty"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.DOUBLE(),
                DataTypes.INT()
            };
    private static RowType rowType =
            RowType.of(
                    Arrays.stream(fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new),
                    fieldNames);
    private static InternalTypeInfo<RowData> rowDataTypeInfo = InternalTypeInfo.of(rowType);

    @After
    public void tearDown() throws Exception {
        if (outputFormat != null) {
            outputFormat.close();
        }
        outputFormat = null;
    }

    @Test
    public void testInvalidDriver() {
        String expectedMsg = "unable to open JDBC writer";
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName("org.apache.derby.jdbc.idontexist")
                                            .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .setTableName(INPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .build();
                            outputFormat.open(0, 1);
                        })
                .isInstanceOf(IOException.class)
                .hasMessage(expectedMsg);
    }

    @Test
    public void testInvalidURL() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                                            .setTableName(INPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .build();
                            outputFormat.open(0, 1);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testIncompatibleTypes() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .setTableName(INPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();

                            setRuntimeContext(outputFormat, false);
                            outputFormat.open(0, 1);

                            RowData row =
                                    buildGenericData(4, "hello", "world", 0.99, "imthewrongtype");
                            outputFormat.writeRecord(row);
                            outputFormat.close();
                        })
                .rootCause()
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testExceptionOnInvalidType() {
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .setTableName(OUTPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();
                            setRuntimeContext(outputFormat, false);
                            outputFormat.open(0, 1);

                            TestEntry entry = TEST_DATA[0];
                            RowData row =
                                    buildGenericData(
                                            entry.id, entry.title, entry.author, 0L, entry.qty);
                            outputFormat.writeRecord(row);
                            outputFormat.close();
                        })
                .rootCause()
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testExceptionOnClose() {
        String expectedMsg = "Writing records to JDBC failed.";
        assertThatThrownBy(
                        () -> {
                            JdbcConnectorOptions jdbcOptions =
                                    JdbcConnectorOptions.builder()
                                            .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                                            .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                                            .setTableName(OUTPUT_TABLE)
                                            .build();
                            JdbcDmlOptions dmlOptions =
                                    JdbcDmlOptions.builder()
                                            .withTableName(jdbcOptions.getTableName())
                                            .withDialect(jdbcOptions.getDialect())
                                            .withFieldNames(fieldNames)
                                            .build();

                            outputFormat =
                                    new JdbcOutputFormatBuilder()
                                            .setJdbcOptions(jdbcOptions)
                                            .setFieldDataTypes(fieldDataTypes)
                                            .setJdbcDmlOptions(dmlOptions)
                                            .setJdbcExecutionOptions(
                                                    JdbcExecutionOptions.builder().build())
                                            .setRowDataTypeInfo(rowDataTypeInfo)
                                            .build();
                            setRuntimeContext(outputFormat, true);
                            outputFormat.open(0, 1);

                            TestEntry entry = TEST_DATA[0];
                            RowData row =
                                    buildGenericData(
                                            entry.id,
                                            entry.title,
                                            entry.author,
                                            entry.price,
                                            entry.qty);

                            outputFormat.writeRecord(row);
                            outputFormat.writeRecord(
                                    row); // writing the same record twice must yield a unique key
                            // violation.
                            outputFormat.close();
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage(expectedMsg);
    }

    @Test
    public void testJdbcOutputFormat() throws IOException, SQLException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(OUTPUT_TABLE)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        for (TestEntry entry : TEST_DATA) {
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }

    @Test
    public void testFlush() throws SQLException, IOException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(OUTPUT_TABLE_2)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(3).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                TEST_DATA[i].id,
                                TEST_DATA[i].title,
                                TEST_DATA[i].author,
                                TEST_DATA[i].price,
                                TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
            outputFormat.writeRecord(
                    buildGenericData(
                            TEST_DATA[2].id,
                            TEST_DATA[2].title,
                            TEST_DATA[2].author,
                            TEST_DATA[2].price,
                            TEST_DATA[2].qty));
            try (ResultSet resultSet = statement.executeQuery()) {
                int recordCount = 0;
                while (resultSet.next()) {
                    assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                    assertThat(resultSet.getObject("title"))
                            .isEqualTo(TEST_DATA[recordCount].title);
                    assertThat(resultSet.getObject("author"))
                            .isEqualTo(TEST_DATA[recordCount].author);
                    assertThat(resultSet.getObject("price"))
                            .isEqualTo(TEST_DATA[recordCount].price);
                    assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(3);
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    public void testFlushWithBatchSizeEqualsZero() throws SQLException, IOException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(OUTPUT_TABLE_2)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();
        JdbcExecutionOptions executionOptions =
                JdbcExecutionOptions.builder().withBatchSize(0).build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(executionOptions)
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)) {
            outputFormat.open(0, 1);
            for (int i = 0; i < 2; ++i) {
                outputFormat.writeRecord(
                        buildGenericData(
                                TEST_DATA[i].id,
                                TEST_DATA[i].title,
                                TEST_DATA[i].author,
                                TEST_DATA[i].price,
                                TEST_DATA[i].qty));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                assertThat(resultSet.next()).isFalse();
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    public void testInvalidConnectionInJdbcOutputFormat() throws IOException, SQLException {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setTableName(OUTPUT_TABLE_3)
                        .build();
        JdbcDmlOptions dmlOptions =
                JdbcDmlOptions.builder()
                        .withTableName(jdbcOptions.getTableName())
                        .withDialect(jdbcOptions.getDialect())
                        .withFieldNames(fieldNames)
                        .build();

        outputFormat =
                new JdbcOutputFormatBuilder()
                        .setJdbcOptions(jdbcOptions)
                        .setFieldDataTypes(fieldDataTypes)
                        .setJdbcDmlOptions(dmlOptions)
                        .setJdbcExecutionOptions(JdbcExecutionOptions.builder().build())
                        .setRowDataTypeInfo(rowDataTypeInfo)
                        .build();
        setRuntimeContext(outputFormat, true);
        outputFormat.open(0, 1);

        // write records
        for (int i = 0; i < 3; i++) {
            TestEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        // close connection
        outputFormat.getConnection().close();

        // continue to write rest records
        for (int i = 3; i < TEST_DATA.length; i++) {
            TestEntry entry = TEST_DATA[i];
            outputFormat.writeRecord(
                    buildGenericData(entry.id, entry.title, entry.author, entry.price, entry.qty));
        }

        outputFormat.close();

        try (Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_3);
                ResultSet resultSet = statement.executeQuery()) {
            int recordCount = 0;
            while (resultSet.next()) {
                assertThat(resultSet.getObject("id")).isEqualTo(TEST_DATA[recordCount].id);
                assertThat(resultSet.getObject("title")).isEqualTo(TEST_DATA[recordCount].title);
                assertThat(resultSet.getObject("author")).isEqualTo(TEST_DATA[recordCount].author);
                assertThat(resultSet.getObject("price")).isEqualTo(TEST_DATA[recordCount].price);
                assertThat(resultSet.getObject("qty")).isEqualTo(TEST_DATA[recordCount].qty);

                recordCount++;
            }
            assertThat(recordCount).isEqualTo(TEST_DATA.length);
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        try (Connection conn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
                Statement stat = conn.createStatement()) {
            stat.execute("DELETE FROM " + OUTPUT_TABLE);
        }
    }
}
