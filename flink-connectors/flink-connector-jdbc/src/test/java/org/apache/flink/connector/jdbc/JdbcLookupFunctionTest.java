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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcLookupFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test suite for {@link JdbcLookupFunction}.
 */
public class JdbcLookupFunctionTest extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";
	public static final String DB_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

	private static String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
	private static TypeInformation[] fieldTypes = new TypeInformation[] {
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO
	};

	private static String[] lookupKeys = new String[] {"id1", "id2"};

	private TableFunction lookupFunction;

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DB_DRIVER);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + LOOKUP_TABLE + " (" +
				"id1 INT NOT NULL DEFAULT 0," +
				"id2 VARCHAR(20) NOT NULL," +
				"comment1 VARCHAR(1000)," +
				"comment2 VARCHAR(1000))");

			Object[][] data = new Object[][] {
				new Object[] {1, "1", "11-c1-v1", "11-c2-v1"},
				new Object[] {1, "1", "11-c1-v2", "11-c2-v2"},
				new Object[] {2, "3", null, "23-c2"},
				new Object[] {2, "5", "25-c1", "25-c2"},
				new Object[] {3, "8", "38-c1", "38-c2"}
			};
			boolean[] surroundedByQuotes = new boolean[] {
				false, true, true, true
			};

			StringBuilder sqlQueryBuilder = new StringBuilder(
				"INSERT INTO " + LOOKUP_TABLE + " (id1, id2, comment1, comment2) VALUES ");
			for (int i = 0; i < data.length; i++) {
				sqlQueryBuilder.append("(");
				for (int j = 0; j < data[i].length; j++) {
					if (data[i][j] == null) {
						sqlQueryBuilder.append("null");
					} else {
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
						sqlQueryBuilder.append(data[i][j]);
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
					}
					if (j < data[i].length - 1) {
						sqlQueryBuilder.append(", ");
					}
				}
				sqlQueryBuilder.append(")");
				if (i < data.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			stat.execute(sqlQueryBuilder.toString());
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DB_DRIVER);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + LOOKUP_TABLE);
		}
	}

	@Test
	public void testEval() throws Exception {

		JdbcLookupFunction lookupFunction = buildLookupFunction();

		lookupFunction.open(null);

		ListOutputCollector collector = new ListOutputCollector();
		lookupFunction.setCollector(collector);

		lookupFunction.eval(1, "1");

		List<String> result = Lists.newArrayList(collector.getOutputs()).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void testInvalidConnectionInEval() throws Exception {

		JdbcLookupFunction lookupFunction = buildLookupFunction();

		lookupFunction.open(null);

		// close connection
		lookupFunction.getDbConnection().close();

		ListOutputCollector collector = new ListOutputCollector();
		lookupFunction.setCollector(collector);

		lookupFunction.eval(1, "1");

		List<String> result = Lists.newArrayList(collector.getOutputs()).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	JdbcLookupFunction buildLookupFunction() {
		JdbcOptions jdbcOptions = JdbcOptions.builder()
			.setDriverName(DB_DRIVER)
			.setDBUrl(DB_URL)
			.setTableName(LOOKUP_TABLE)
			.build();

		JdbcLookupOptions lookupOptions = JdbcLookupOptions.builder().build();

		JdbcLookupFunction lookupFunction = JdbcLookupFunction.builder()
			.setOptions(jdbcOptions)
			.setLookupOptions(lookupOptions)
			.setFieldTypes(fieldTypes)
			.setFieldNames(fieldNames)
			.setKeyNames(lookupKeys)
			.build();

		return lookupFunction;
	}

	private static final class ListOutputCollector implements Collector<Row> {

		private final List<Row> output = new ArrayList<>();

		@Override
		public void collect(Row row) {
			this.output.add(row);
		}

		@Override
		public void close() {}

		public List<Row> getOutputs() {
			return output;
		}
	}
}
