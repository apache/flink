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

package org.apache.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.ROW_TYPE_INFO;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_ID;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_EMPTY;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link JdbcInputFormat}. */
public class JdbcInputFormatTest extends JdbcDataTestBase {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private JdbcInputFormat jdbcInputFormat;

    @After
    public void tearDown() throws IOException {
        if (jdbcInputFormat != null) {
            jdbcInputFormat.close();
            jdbcInputFormat.closeInputFormat();
        }
        jdbcInputFormat = null;
    }

    @Test
    public void testUntypedRowInfo() throws IOException {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("No RowTypeInfo supplied");
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .finish();
        jdbcInputFormat.openInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDriver() throws IOException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername("org.apache.derby.jdbc.idontexist")
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
        jdbcInputFormat.openInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidURL() throws IOException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
        jdbcInputFormat.openInputFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidQuery() throws IOException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery("iamnotsql")
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
        jdbcInputFormat.openInputFormat();
    }

    @Test
    public void testNoUrl() throws IOException {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("jdbc url is empty");
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
    }

    @Test
    public void testNoQuery() throws IOException {
        thrown.expect(NullPointerException.class);
        thrown.expectMessage("No query supplied");
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFetchSize() {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setFetchSize(-7)
                        .finish();
    }

    @Test
    public void testValidFetchSizeIntegerMin() {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setFetchSize(Integer.MIN_VALUE)
                        .finish();
    }

    @Test
    public void testDefaultFetchSizeIsUsedIfNotConfiguredOtherwise()
            throws SQLException, ClassNotFoundException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
        jdbcInputFormat.openInputFormat();

        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        final int defaultFetchSize =
                DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl())
                        .createStatement()
                        .getFetchSize();

        assertThat(jdbcInputFormat.getStatement().getFetchSize()).isEqualTo(defaultFetchSize);
    }

    @Test
    public void testFetchSizeCanBeConfigured() throws SQLException {
        final int desiredFetchSize = 10_000;
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setFetchSize(desiredFetchSize)
                        .finish();
        jdbcInputFormat.openInputFormat();
        assertThat(jdbcInputFormat.getStatement().getFetchSize()).isEqualTo(desiredFetchSize);
    }

    @Test
    public void testDefaultAutoCommitIsUsedIfNotConfiguredOtherwise()
            throws SQLException, ClassNotFoundException {

        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .finish();
        jdbcInputFormat.openInputFormat();

        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        final boolean defaultAutoCommit =
                DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl()).getAutoCommit();

        assertThat(jdbcInputFormat.getDbConn().getAutoCommit()).isEqualTo(defaultAutoCommit);
    }

    @Test
    public void testAutoCommitCanBeConfigured() throws SQLException {

        final boolean desiredAutoCommit = false;
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setAutoCommit(desiredAutoCommit)
                        .finish();

        jdbcInputFormat.openInputFormat();
        assertThat(jdbcInputFormat.getDbConn().getAutoCommit()).isEqualTo(desiredAutoCommit);
    }

    @Test
    public void testJdbcInputFormatWithoutParallelism() throws IOException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                        .finish();
        // this query does not exploit parallelism
        assertThat(jdbcInputFormat.createInputSplits(1)).hasSize(1);
        jdbcInputFormat.openInputFormat();
        jdbcInputFormat.open(null);
        Row row = new Row(5);
        int recordCount = 0;
        while (!jdbcInputFormat.reachedEnd()) {
            Row next = jdbcInputFormat.nextRecord(row);

            assertEquals(TEST_DATA[recordCount], next);

            recordCount++;
        }
        jdbcInputFormat.close();
        jdbcInputFormat.closeInputFormat();
        assertThat(recordCount).isEqualTo(TEST_DATA.length);
    }

    @Test
    public void testJdbcInputFormatWithParallelismAndNumericColumnSplitting() throws IOException {
        final int fetchSize = 1;
        final long min = TEST_DATA[0].id;
        final long max = TEST_DATA[TEST_DATA.length - fetchSize].id;
        JdbcParameterValuesProvider pramProvider =
                new JdbcNumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize);
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setParametersProvider(pramProvider)
                        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                        .finish();

        jdbcInputFormat.openInputFormat();
        InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
        // this query exploit parallelism (1 split for every id)
        assertThat(splits).hasSameSizeAs(TEST_DATA);
        int recordCount = 0;
        Row row = new Row(5);
        for (InputSplit split : splits) {
            jdbcInputFormat.open(split);
            while (!jdbcInputFormat.reachedEnd()) {
                Row next = jdbcInputFormat.nextRecord(row);

                assertEquals(TEST_DATA[recordCount], next);

                recordCount++;
            }
            jdbcInputFormat.close();
        }
        jdbcInputFormat.closeInputFormat();
        assertThat(recordCount).isEqualTo(TEST_DATA.length);
    }

    @Test
    public void testJdbcInputFormatWithoutParallelismAndNumericColumnSplitting()
            throws IOException {
        final long min = TEST_DATA[0].id;
        final long max = TEST_DATA[TEST_DATA.length - 1].id;
        final long fetchSize = max + 1; // generate a single split
        JdbcParameterValuesProvider pramProvider =
                new JdbcNumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize);
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setParametersProvider(pramProvider)
                        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                        .finish();

        jdbcInputFormat.openInputFormat();
        InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
        // assert that a single split was generated
        assertThat(splits).hasSize(1);
        int recordCount = 0;
        Row row = new Row(5);
        for (InputSplit split : splits) {
            jdbcInputFormat.open(split);
            while (!jdbcInputFormat.reachedEnd()) {
                Row next = jdbcInputFormat.nextRecord(row);

                assertEquals(TEST_DATA[recordCount], next);

                recordCount++;
            }
            jdbcInputFormat.close();
        }
        jdbcInputFormat.closeInputFormat();
        assertThat(recordCount).isEqualTo(TEST_DATA.length);
    }

    @Test
    public void testJdbcInputFormatWithParallelismAndGenericSplitting() throws IOException {
        Serializable[][] queryParameters = new String[2][1];
        queryParameters[0] = new String[] {TEST_DATA[3].author};
        queryParameters[1] = new String[] {TEST_DATA[0].author};
        JdbcParameterValuesProvider paramProvider =
                new JdbcGenericParameterValuesProvider(queryParameters);
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setParametersProvider(paramProvider)
                        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                        .finish();

        jdbcInputFormat.openInputFormat();
        InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
        // this query exploit parallelism (1 split for every queryParameters row)
        assertThat(splits).hasSameSizeAs(queryParameters);

        verifySplit(splits[0], TEST_DATA[3].id);
        verifySplit(splits[1], TEST_DATA[0].id + TEST_DATA[1].id);

        jdbcInputFormat.closeInputFormat();
    }

    private void verifySplit(InputSplit split, int expectedIDSum) throws IOException {
        int sum = 0;

        Row row = new Row(5);
        jdbcInputFormat.open(split);
        while (!jdbcInputFormat.reachedEnd()) {
            row = jdbcInputFormat.nextRecord(row);

            int id = ((int) row.getField(0));
            int testDataIndex = id - 1001;

            assertEquals(TEST_DATA[testDataIndex], row);
            sum += id;
        }

        assertThat(sum).isEqualTo(expectedIDSum);
    }

    @Test
    public void testEmptyResults() throws IOException {
        jdbcInputFormat =
                JdbcInputFormat.buildJdbcInputFormat()
                        .setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
                        .setQuery(SELECT_EMPTY)
                        .setRowTypeInfo(ROW_TYPE_INFO)
                        .setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
                        .finish();
        try {
            jdbcInputFormat.openInputFormat();
            jdbcInputFormat.open(null);
            assertThat(jdbcInputFormat.reachedEnd()).isTrue();
        } finally {
            jdbcInputFormat.close();
            jdbcInputFormat.closeInputFormat();
        }
    }

    private static void assertEquals(TestEntry expected, Row actual) {
        assertThat(actual.getField(0)).isEqualTo(expected.id);
        assertThat(actual.getField(1)).isEqualTo(expected.title);
        assertThat(actual.getField(2)).isEqualTo(expected.author);
        assertThat(actual.getField(3)).isEqualTo(expected.price);
        assertThat(actual.getField(4)).isEqualTo(expected.qty);
    }
}
