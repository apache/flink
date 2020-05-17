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
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Arrays;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_BOOKS_SPLIT_BY_ID;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_EMPTY;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;

/**
 * Test suite for {@link JdbcRowDataInputFormat}.
 */
public class JdbcRowDataInputFormatTest extends JdbcDataTestBase {

	private JdbcRowDataInputFormat inputFormat;
	private static String[] fieldNames = new String[]{"id", "title", "author", "price", "qty"};
	private static DataType[] fieldDataTypes = new DataType[]{
		DataTypes.INT(),
		DataTypes.STRING(),
		DataTypes.STRING(),
		DataTypes.DOUBLE(),
		DataTypes.INT()};
	final JdbcDialect dialect = JdbcOptions.builder()
		.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
		.setTableName(INPUT_TABLE)
		.build()
		.getDialect();
	final RowType rowType = RowType.of(
		Arrays.stream(fieldDataTypes)
			.map(DataType::getLogicalType)
			.toArray(LogicalType[]::new),
		fieldNames);

	@After
	public void tearDown() throws IOException {
		if (inputFormat != null) {
			inputFormat.close();
			inputFormat.closeInputFormat();
		}
		inputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUntypedRowInfo() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername("org.apache.derby.jdbc.idontexist")
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS)
			.build();
		inputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername("org.apache.derby.jdbc.idontexist")
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS)
			.build();
		inputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
			.setQuery(SELECT_ALL_BOOKS)
			.build();
		inputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery("iamnotsql")
			.build();
		inputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setQuery(SELECT_ALL_BOOKS)
			.build();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidFetchSize() {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS)
			.setFetchSize(-7)
			.build();
	}

	@Test
	public void testValidFetchSizeIntegerMin() {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS)
			.setFetchSize(Integer.MIN_VALUE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();
	}

	@Test
	public void testJdbcInputFormatWithoutParallelism() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS)
			.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();
		//this query does not exploit parallelism
		Assert.assertEquals(1, inputFormat.createInputSplits(1).length);
		inputFormat.openInputFormat();
		inputFormat.open(null);
		RowData row = new GenericRowData(5);
		int recordCount = 0;
		while (!inputFormat.reachedEnd()) {
			RowData next = inputFormat.nextRecord(row);

			assertEquals(TEST_DATA[recordCount], next);

			recordCount++;
		}
		inputFormat.close();
		inputFormat.closeInputFormat();
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJdbcInputFormatWithParallelismAndNumericColumnSplitting() throws IOException {
		final int fetchSize = 1;
		final long min = TEST_DATA[0].id;
		final long max = TEST_DATA[TEST_DATA.length - fetchSize].id;
		JdbcParameterValuesProvider pramProvider = new JdbcNumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize);
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
			.setParametersProvider(pramProvider)
			.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();

		inputFormat.openInputFormat();
		InputSplit[] splits = inputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every id)
		Assert.assertEquals(TEST_DATA.length, splits.length);
		int recordCount = 0;
		RowData row = new GenericRowData(5);
		for (InputSplit split : splits) {
			inputFormat.open(split);
			while (!inputFormat.reachedEnd()) {
				RowData next = inputFormat.nextRecord(row);

				assertEquals(TEST_DATA[recordCount], next);

				recordCount++;
			}
			inputFormat.close();
		}
		inputFormat.closeInputFormat();
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJdbcInputFormatWithoutParallelismAndNumericColumnSplitting() throws IOException {
		final long min = TEST_DATA[0].id;
		final long max = TEST_DATA[TEST_DATA.length - 1].id;
		final long fetchSize = max + 1; //generate a single split
		JdbcParameterValuesProvider pramProvider = new JdbcNumericBetweenParametersProvider(min, max).ofBatchSize(fetchSize);
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID)
			.setParametersProvider(pramProvider)
			.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();

		inputFormat.openInputFormat();
		InputSplit[] splits = inputFormat.createInputSplits(1);
		//assert that a single split was generated
		Assert.assertEquals(1, splits.length);
		int recordCount = 0;
		RowData row = new GenericRowData(5);
		for (InputSplit split : splits) {
			inputFormat.open(split);
			while (!inputFormat.reachedEnd()) {
				RowData next = inputFormat.nextRecord(row);

				assertEquals(TEST_DATA[recordCount], next);

				recordCount++;
			}
			inputFormat.close();
		}
		inputFormat.closeInputFormat();
		Assert.assertEquals(TEST_DATA.length, recordCount);
	}

	@Test
	public void testJdbcInputFormatWithParallelismAndGenericSplitting() throws IOException {
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{TEST_DATA[3].author};
		queryParameters[1] = new String[]{TEST_DATA[0].author};
		JdbcParameterValuesProvider paramProvider = new JdbcGenericParameterValuesProvider(queryParameters);
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR)
			.setParametersProvider(paramProvider)
			.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();

		inputFormat.openInputFormat();
		InputSplit[] splits = inputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every queryParameters row)
		Assert.assertEquals(queryParameters.length, splits.length);

		verifySplit(splits[0], TEST_DATA[3].id);
		verifySplit(splits[1], TEST_DATA[0].id + TEST_DATA[1].id);

		inputFormat.closeInputFormat();
	}

	private void verifySplit(InputSplit split, int expectedIDSum) throws IOException {
		int sum = 0;

		RowData row = new GenericRowData(5);
		inputFormat.open(split);
		while (!inputFormat.reachedEnd()) {
			row = inputFormat.nextRecord(row);

			int id = ((int) RowData.get(row, 0, new IntType()));
			int testDataIndex = id - 1001;

			assertEquals(TEST_DATA[testDataIndex], row);
			sum += id;
		}

		Assert.assertEquals(expectedIDSum, sum);
	}

	@Test
	public void testEmptyResults() throws IOException {
		inputFormat = JdbcRowDataInputFormat.builder()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(SELECT_EMPTY)
			.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
			.setRowConverter(dialect.getRowConverter(rowType))
			.build();

		try {
			inputFormat.openInputFormat();
			inputFormat.open(null);
			Assert.assertTrue(inputFormat.reachedEnd());
		} finally {
			inputFormat.close();
			inputFormat.closeInputFormat();
		}
	}

	private static void assertEquals(JdbcTestFixture.TestEntry expected, RowData actual) {
		Assert.assertEquals(expected.id, actual.isNullAt(0) ? null : Integer.valueOf(actual.getInt(0)));
		Assert.assertEquals(expected.title, actual.isNullAt(1) ? null : actual.getString(1).toString());
		Assert.assertEquals(expected.author, actual.isNullAt(2) ? null : actual.getString(2).toString());
		Assert.assertEquals(expected.price, actual.isNullAt(3) ? null : Double.valueOf(actual.getDouble(3)));
		Assert.assertEquals(expected.qty, actual.isNullAt(4) ? null : Integer.valueOf(actual.getInt(4)));
	}
}
