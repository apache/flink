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

import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.OUTPUT_TABLE_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_2;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.SELECT_ALL_NEWBOOKS_3;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link JdbcOutputFormat}.
 */
public class JdbcOutputFormatTest extends JdbcDataTestBase {

	private JdbcOutputFormat jdbcOutputFormat;

	@After
	public void tearDown() throws IOException {
		if (jdbcOutputFormat != null) {
			jdbcOutputFormat.close();
		}
		jdbcOutputFormat = null;
	}

	@Test
	public void testInvalidDriver() {
		String expectedMsg = "unable to open JDBC writer";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
			jdbcOutputFormat.open(0, 1);
		} catch (Exception e) {
			assertTrue(findThrowable(e, IOException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testInvalidURL() {
		String expectedMsg = "No suitable driver found for jdbc:der:iamanerror:mory:ebookshop";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
			jdbcOutputFormat.open(0, 1);
		} catch (Exception e) {
			assertTrue(findThrowable(e, IOException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testInvalidQuery() {
		String expectedMsg = "unable to open JDBC writer";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery("iamnotsql")
				.finish();
			setRuntimeContext(jdbcOutputFormat, true);
			jdbcOutputFormat.open(0, 1);
		} catch (Exception e) {
			assertTrue(findThrowable(e, IOException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testIncompleteConfiguration() {
		String expectedMsg = "jdbc url is empty";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		} catch (Exception e) {
			assertTrue(findThrowable(e, NullPointerException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testIncompatibleTypes() {
		String expectedMsg = "Invalid character string format for type INTEGER.";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
			setRuntimeContext(jdbcOutputFormat, true);
			jdbcOutputFormat.open(0, 1);

			Row row = new Row(5);
			row.setField(0, 4);
			row.setField(1, "hello");
			row.setField(2, "world");
			row.setField(3, 0.99);
			row.setField(4, "imthewrongtype");

			jdbcOutputFormat.writeRecord(row);
			jdbcOutputFormat.close();
		} catch (Exception e) {
			assertTrue(findThrowable(e, SQLDataException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testExceptionOnInvalidType() {
		String expectedMsg = "field index: 3, field value: 0.";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.setSqlTypes(new int[]{
					Types.INTEGER,
					Types.VARCHAR,
					Types.VARCHAR,
					Types.DOUBLE,
					Types.INTEGER})
				.finish();
			setRuntimeContext(jdbcOutputFormat, true);
			jdbcOutputFormat.open(0, 1);

			TestEntry entry = TEST_DATA[0];
			Row row = new Row(5);
			row.setField(0, entry.id);
			row.setField(1, entry.title);
			row.setField(2, entry.author);
			row.setField(3, 0L); // use incompatible type (Long instead of Double)
			row.setField(4, entry.qty);
			jdbcOutputFormat.writeRecord(row);
			jdbcOutputFormat.close();
		} catch (Exception e) {
			assertTrue(findThrowable(e, ClassCastException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testExceptionOnClose() {
		String expectedMsg = "Writing records to JDBC failed.";
		try {
			jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.setSqlTypes(new int[]{
					Types.INTEGER,
					Types.VARCHAR,
					Types.VARCHAR,
					Types.DOUBLE,
					Types.INTEGER})
				.finish();
			setRuntimeContext(jdbcOutputFormat, true);
			jdbcOutputFormat.open(0, 1);

			TestEntry entry = TEST_DATA[0];
			Row row = new Row(5);
			row.setField(0, entry.id);
			row.setField(1, entry.title);
			row.setField(2, entry.author);
			row.setField(3, entry.price);
			row.setField(4, entry.qty);
			jdbcOutputFormat.writeRecord(row);
			jdbcOutputFormat.writeRecord(row); // writing the same record twice must yield a unique key violation.

			jdbcOutputFormat.close();
		} catch (Exception e) {
			assertTrue(findThrowable(e, RuntimeException.class).isPresent());
			assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
		}
	}

	@Test
	public void testJdbcOutputFormat() throws IOException, SQLException {
		jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
				.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
				.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.finish();
		setRuntimeContext(jdbcOutputFormat, true);
		jdbcOutputFormat.open(0, 1);

		for (TestEntry entry : TEST_DATA) {
			jdbcOutputFormat.writeRecord(toRow(entry));
		}

		jdbcOutputFormat.close();

		try (
			Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
			PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS);
			ResultSet resultSet = statement.executeQuery()
		) {
			int recordCount = 0;
			while (resultSet.next()) {
				assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
				assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
				assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
				assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
				assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

				recordCount++;
			}
			assertEquals(TEST_DATA.length, recordCount);
		}
	}

	@Test
	public void testFlush() throws SQLException, IOException {
		jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_2))
			.setBatchSize(3)
			.finish();
		setRuntimeContext(jdbcOutputFormat, true);
		try (
			Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
			PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_2)
		) {
			jdbcOutputFormat.open(0, 1);
			for (int i = 0; i < 2; ++i) {
				jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
			}
			try (ResultSet resultSet = statement.executeQuery()) {
				assertFalse(resultSet.next());
			}
			jdbcOutputFormat.writeRecord(toRow(TEST_DATA[2]));
			try (ResultSet resultSet = statement.executeQuery()) {
				int recordCount = 0;
				while (resultSet.next()) {
					assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
					assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
					assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
					assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
					assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));
					recordCount++;
				}
				assertEquals(3, recordCount);
			}
		} finally {
			jdbcOutputFormat.close();
		}
	}

	@Test
	public void testInvalidConnectionInJdbcOutputFormat() throws IOException, SQLException {
		jdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
			.setDrivername(DERBY_EBOOKSHOP_DB.getDriverClass())
			.setDBUrl(DERBY_EBOOKSHOP_DB.getUrl())
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_3))
			.finish();
		setRuntimeContext(jdbcOutputFormat, true);
		jdbcOutputFormat.open(0, 1);

		// write records
		for (int i = 0; i < 3; i++) {
			jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
		}

		// close connection
		jdbcOutputFormat.getConnection().close();

		for (int i = 3; i < TEST_DATA.length; i++) {
			jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
		}

		jdbcOutputFormat.close();

		try (
			Connection dbConn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
			PreparedStatement statement = dbConn.prepareStatement(SELECT_ALL_NEWBOOKS_3);
			ResultSet resultSet = statement.executeQuery()
		) {
			int recordCount = 0;
			while (resultSet.next()) {
				assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
				assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
				assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
				assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
				assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

				recordCount++;
			}
			assertEquals(TEST_DATA.length, recordCount);
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
		try (
			Connection conn = DriverManager.getConnection(DERBY_EBOOKSHOP_DB.getUrl());
			Statement stat = conn.createStatement()) {
			stat.execute("DELETE FROM " + OUTPUT_TABLE);

		}
	}
}
