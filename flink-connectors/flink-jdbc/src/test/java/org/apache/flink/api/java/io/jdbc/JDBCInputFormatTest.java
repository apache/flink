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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests for the {@link JDBCInputFormat}.
 */
public class JDBCInputFormatTest extends JDBCTestBase {

	private JDBCInputFormat jdbcInputFormat;

	@After
	public void tearDown() throws IOException {
		if (jdbcInputFormat != null) {
			jdbcInputFormat.close();
			jdbcInputFormat.closeInputFormat();
		}
		jdbcInputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUntypedRowInfo() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.setRowTypeInfo(ROW_TYPE_INFO)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.finish();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidFetchSize() {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(SELECT_ALL_BOOKS)
			.setRowTypeInfo(ROW_TYPE_INFO)
			.setFetchSize(-7)
			.finish();
	}

	@Test
	public void testValidFetchSizeIntegerMin() {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(SELECT_ALL_BOOKS)
			.setRowTypeInfo(ROW_TYPE_INFO)
			.setFetchSize(Integer.MIN_VALUE)
			.finish();
	}

	@Test
	public void testDefaultFetchSizeIsUsedIfNotConfiguredOtherwise() throws SQLException, ClassNotFoundException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(SELECT_ALL_BOOKS)
			.setRowTypeInfo(ROW_TYPE_INFO)
			.finish();
		jdbcInputFormat.openInputFormat();

		Class.forName(DRIVER_CLASS);
		final int defaultFetchSize = DriverManager.getConnection(DB_URL).createStatement().getFetchSize();

		Assert.assertEquals(defaultFetchSize, jdbcInputFormat.getStatement().getFetchSize());
	}

	@Test
	public void testFetchSizeCanBeConfigured() throws SQLException {
		final int desiredFetchSize = 10_000;
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(SELECT_ALL_BOOKS)
			.setRowTypeInfo(ROW_TYPE_INFO)
			.setFetchSize(desiredFetchSize)
			.finish();
		jdbcInputFormat.openInputFormat();
		Assert.assertEquals(desiredFetchSize, jdbcInputFormat.getStatement().getFetchSize());
	}

	@Test
	public void testJDBCInputFormatWithoutParallelism() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		//this query does not exploit parallelism
		Assert.assertEquals(1, jdbcInputFormat.createInputSplits(1).length);
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row =  new Row(5);
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			Row next = jdbcInputFormat.nextRecord(row);

			assertEquals(TEST_DATA[recordCount], next);

			recordCount++;
		}
		jdbcInputFormat.close();
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJDBCInputFormatWithParallelismAndNumericColumnSplitting() throws IOException {
		final int fetchSize = 1;
		final long min = TEST_DATA[0].id;
		final long max = TEST_DATA[TEST_DATA.length - fetchSize].id;
		ParameterValuesProvider pramProvider = new NumericBetweenParametersProvider(fetchSize, min, max);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.setParametersProvider(pramProvider)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();

		jdbcInputFormat.openInputFormat();
		InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every id)
		Assert.assertEquals(TEST_DATA.length, splits.length);
		int recordCount = 0;
		Row row =  new Row(5);
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
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJDBCInputFormatWithoutParallelismAndNumericColumnSplitting() throws IOException {
		final long min = TEST_DATA[0].id;
		final long max = TEST_DATA[TEST_DATA.length - 1].id;
		final long fetchSize = max + 1; //generate a single split
		ParameterValuesProvider pramProvider = new NumericBetweenParametersProvider(fetchSize, min, max);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.setParametersProvider(pramProvider)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();

		jdbcInputFormat.openInputFormat();
		InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
		//assert that a single split was generated
		Assert.assertEquals(1, splits.length);
		int recordCount = 0;
		Row row =  new Row(5);
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
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJDBCInputFormatWithParallelismAndGenericSplitting() throws IOException {
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{TEST_DATA[3].author};
		queryParameters[1] = new String[]{TEST_DATA[0].author};
		ParameterValuesProvider paramProvider = new GenericParameterValuesProvider(queryParameters);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.setParametersProvider(paramProvider)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();

		jdbcInputFormat.openInputFormat();
		InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every queryParameters row)
		Assert.assertEquals(queryParameters.length, splits.length);

		verifySplit(splits[0], TEST_DATA[3].id);
		verifySplit(splits[1], TEST_DATA[0].id + TEST_DATA[1].id);

		jdbcInputFormat.closeInputFormat();
	}

	private void verifySplit(InputSplit split, int expectedIDSum) throws IOException {
		int sum = 0;

		Row row =  new Row(5);
		jdbcInputFormat.open(split);
		while (!jdbcInputFormat.reachedEnd()) {
			row = jdbcInputFormat.nextRecord(row);

			int id = ((int) row.getField(0));
			int testDataIndex = id - 1001;

			assertEquals(TEST_DATA[testDataIndex], row);
			sum += id;
		}

		Assert.assertEquals(expectedIDSum, sum);
	}

	@Test
	public void testEmptyResults() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_EMPTY)
				.setRowTypeInfo(ROW_TYPE_INFO)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		try {
			jdbcInputFormat.openInputFormat();
			jdbcInputFormat.open(null);
			Assert.assertTrue(jdbcInputFormat.reachedEnd());
		} finally {
			jdbcInputFormat.close();
			jdbcInputFormat.closeInputFormat();
		}
	}

	private static void assertEquals(TestEntry expected, Row actual) {
		Assert.assertEquals(expected.id, actual.getField(0));
		Assert.assertEquals(expected.title, actual.getField(1));
		Assert.assertEquals(expected.author, actual.getField(2));
		Assert.assertEquals(expected.price, actual.getField(3));
		Assert.assertEquals(expected.qty, actual.getField(4));
	}

}
