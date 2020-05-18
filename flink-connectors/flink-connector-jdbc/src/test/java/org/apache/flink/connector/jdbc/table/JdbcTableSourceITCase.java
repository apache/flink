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

import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * ITCase for {@link JdbcTableSource}.
 */
public class JdbcTableSourceITCase extends AbstractTestBase {

	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:test";
	public static final String INPUT_TABLE = "jdbcSource";

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestBase.class.getCanonicalName() + ".DEV_NULL");
		Class.forName(DRIVER_CLASS);

		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement statement = conn.createStatement()) {
			statement.executeUpdate("CREATE TABLE " + INPUT_TABLE + " (" +
					"id BIGINT NOT NULL," +
					"timestamp6_col TIMESTAMP, " +
					"timestamp9_col TIMESTAMP, " +
					"time_col TIME, " +
					"real_col FLOAT(23), " +    // A precision of 23 or less makes FLOAT equivalent to REAL.
					"double_col FLOAT(24)," +   // A precision of 24 or greater makes FLOAT equivalent to DOUBLE PRECISION.
					"decimal_col DECIMAL(10, 4))");
			statement.executeUpdate("INSERT INTO " + INPUT_TABLE + " VALUES (" +
					"1, TIMESTAMP('2020-01-01 15:35:00.123456'), TIMESTAMP('2020-01-01 15:35:00.123456789'), " +
					"TIME('15:35:00'), 1.175E-37, 1.79769E+308, 100.1234)");
			statement.executeUpdate("INSERT INTO " + INPUT_TABLE + " VALUES (" +
					"2, TIMESTAMP('2020-01-01 15:36:01.123456'), TIMESTAMP('2020-01-01 15:36:01.123456789'), " +
					"TIME('15:36:01'), -1.175E-37, -1.79769E+308, 101.1234)");
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
		}
	}

	@Test
	public void testJdbcSource() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		tEnv.executeSql(
			"CREATE TABLE " + INPUT_TABLE + "(" +
				"id BIGINT," +
				"timestamp6_col TIMESTAMP(6)," +
				"timestamp9_col TIMESTAMP(9)," +
				"time_col TIME," +
				"real_col FLOAT," +
				"double_col DOUBLE," +
				"decimal_col DECIMAL(10, 4)" +
				") WITH (" +
				"  'connector.type'='jdbc'," +
				"  'connector.url'='" + DB_URL + "'," +
				"  'connector.table'='" + INPUT_TABLE + "'" +
				")"
		);

		TableResult tableResult = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE);

		List<String> results = manifestResults(tableResult);

		assertThat(
				results,
				containsInAnyOrder(
						"1,2020-01-01T15:35:00.123456,2020-01-01T15:35:00.123456789,15:35,1.175E-37,1.79769E308,100.1234",
						"2,2020-01-01T15:36:01.123456,2020-01-01T15:36:01.123456789,15:36:01,-1.175E-37,-1.79769E308,101.1234"));
	}

	@Test
	public void testProjectableJdbcSource() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		tEnv.executeSql(
			"CREATE TABLE " + INPUT_TABLE + "(" +
				"id BIGINT," +
				"timestamp6_col TIMESTAMP(6)," +
				"timestamp9_col TIMESTAMP(9)," +
				"time_col TIME," +
				"real_col FLOAT," +
				"decimal_col DECIMAL(10, 4)" +
				") WITH (" +
				"  'connector.type'='jdbc'," +
				"  'connector.url'='" + DB_URL + "'," +
				"  'connector.table'='" + INPUT_TABLE + "'" +
				")"
		);

		TableResult tableResult = tEnv.executeSql("SELECT timestamp6_col, decimal_col FROM " + INPUT_TABLE);

		List<String> results = manifestResults(tableResult);

		assertThat(
				results,
				containsInAnyOrder(
						"2020-01-01T15:35:00.123456,100.1234",
						"2020-01-01T15:36:01.123456,101.1234"));
	}

	@Test
	public void testScanQueryJDBCSource() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		final String testQuery = "SELECT id FROM " + INPUT_TABLE;
		tEnv.executeSql(
				"CREATE TABLE test(" +
						"id BIGINT" +
						") WITH (" +
						"  'connector.type'='jdbc'," +
						"  'connector.url'='" + DB_URL + "'," +
						"  'connector.table'='whatever'," +
						"  'connector.read.query'='" + testQuery + "'" +
						")"
		);

		TableResult tableResult = tEnv.executeSql("SELECT id FROM test");

		List<String> results = manifestResults(tableResult);

		assertThat(results, containsInAnyOrder("1", "2"));
	}

	private static List<String> manifestResults(TableResult result) {
		Iterator<Row> resultIterator = result.collect();
		return StreamSupport
				.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false)
				.map(Row::toString)
				.collect(Collectors.toList());
	}
}
