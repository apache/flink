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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.row;
import static org.junit.Assert.assertEquals;

/**
 * Test unsigned type conversion between Flink and JDBC driver mysql, the test underlying use
 * MariaDB to mock a DB which use mysql driver too.
 */
public class UnsignedTypeConversionITCase extends AbstractTestBase {

	private static final Logger logger = LoggerFactory.getLogger(UnsignedTypeConversionITCase.class);
	private static final String DEFAULT_DB_NAME = "test";
	private static final String TABLE_NAME = "unsigned_test";
	private static final int INITIALIZE_DB_MAX_RETRY = 3;
	private static DB db;
	private static String dbUrl;
	private static Connection connection;

	private StreamTableEnvironment tEnv;

	@BeforeClass
	public static void prepareMariaDB() throws IllegalStateException {
		boolean initDbSuccess = false;
		int i = 0;
		//The initialization of maria db instance is a little unstable according to past CI tests.
		//Add retry logic here to avoid initialization failure.
		while (i < INITIALIZE_DB_MAX_RETRY) {
			try {
				db = DB.newEmbeddedDB(DBConfigurationBuilder.newBuilder().build());
				db.start();
				dbUrl = db.getConfiguration().getURL(DEFAULT_DB_NAME);
				connection = DriverManager.getConnection(dbUrl);
				try (Statement statement = connection.createStatement()) {
					statement.execute("CREATE DATABASE IF NOT EXISTS `" + DEFAULT_DB_NAME + "`;");
					ResultSet resultSet = statement.executeQuery("SELECT SCHEMA_NAME FROM " +
						"INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + DEFAULT_DB_NAME + "';");
					if (resultSet.next()) {
						String dbName = resultSet.getString(1);
						initDbSuccess = DEFAULT_DB_NAME.equalsIgnoreCase(dbName);
					}
				}
			} catch (Exception e) {
				logger.warn("Initialize DB fail caused by {}", e);
				stopDb();
			}
			if (initDbSuccess) {
				break;
			}
			i++;
		}
		if (!initDbSuccess) {
			throw new IllegalStateException(String.format("Initialize MySQL database instance failed after {} attempts," +
				" please open an issue.", INITIALIZE_DB_MAX_RETRY));
		}
	}

	@Before
	public void setUp() throws SQLException, IllegalStateException {
		//dbUrl: jdbc:mysql://localhost:3306/test
		createMysqlTable();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		tEnv = StreamTableEnvironment.create(env);
		createFlinkTable();
		prepareData();
	}

	@Test
	public void testUnsignedType() throws Exception {
		// write data to db
		tEnv.executeSql("insert into jdbc_sink select" +
			" tiny_c," +
			" tiny_un_c," +
			" small_c," +
			" small_un_c ," +
			" int_c," +
			" int_un_c," +
			" big_c ," +
			" big_un_c from data").await();

		// read data from db using jdbc connection and compare
		PreparedStatement query = connection.prepareStatement(String.format("select tiny_c, tiny_un_c, small_c, small_un_c," +
			" int_c, int_un_c, big_c, big_un_c from %s", TABLE_NAME));
		ResultSet resultSet = query.executeQuery();
		while (resultSet.next()) {
			assertEquals(Integer.valueOf(127), resultSet.getObject("tiny_c"));
			assertEquals(Integer.valueOf(255), resultSet.getObject("tiny_un_c"));
			assertEquals(Integer.valueOf(32767), resultSet.getObject("small_c"));
			assertEquals(Integer.valueOf(65535), resultSet.getObject("small_un_c"));
			assertEquals(Integer.valueOf(2147483647), resultSet.getObject("int_c"));
			assertEquals(Long.valueOf(4294967295L), resultSet.getObject("int_un_c"));
			assertEquals(Long.valueOf(9223372036854775807L), resultSet.getObject("big_c"));
			assertEquals(new BigInteger("18446744073709551615"), resultSet.getObject("big_un_c"));
		}

		// read data from db using flink and compare
		Iterator<Row> collected = tEnv.executeSql("select tiny_c, tiny_un_c, small_c, small_un_c," +
			" int_c, int_un_c, big_c, big_un_c from jdbc_source")
			.collect();
		List<String> result = CollectionUtil.iteratorToList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());
		List<String> expected = Collections.singletonList(
			"127,255,32767,65535,2147483647,4294967295,9223372036854775807,18446744073709551615");
		assertEquals(expected, result);
	}

	private void createMysqlTable() throws SQLException {
		PreparedStatement ddlStatement = connection.prepareStatement("create table " + TABLE_NAME + " (" +
			" tiny_c TINYINT," +
			" tiny_un_c TINYINT UNSIGNED," +
			" small_c SMALLINT," +
			" small_un_c SMALLINT UNSIGNED," +
			" int_c INTEGER ," +
			" int_un_c INTEGER UNSIGNED," +
			" big_c BIGINT," +
			" big_un_c BIGINT UNSIGNED);");
		ddlStatement.execute();
	}

	private void createFlinkTable() {
		String commonDDL = "create table %s (" +
			"tiny_c TINYINT," +
			"tiny_un_c SMALLINT," +
			"small_c SMALLINT," +
			"small_un_c INT," +
			"int_c INT," +
			"int_un_c BIGINT," +
			"big_c BIGINT," +
			"big_un_c DECIMAL(20, 0)) with(" +
			" 'connector' = 'jdbc'," +
			" 'url' = '" + dbUrl + "'," +
			" 'table-name' = '" + TABLE_NAME + "'" +
			")";
		tEnv.executeSql(String.format(commonDDL, "jdbc_source"));
		tEnv.executeSql(String.format(commonDDL, "jdbc_sink"));
	}

	private void prepareData() {
		Table dataTable = tEnv.fromValues(
			DataTypes.ROW(
				DataTypes.FIELD("tiny_c", DataTypes.TINYINT().notNull()),
				DataTypes.FIELD("tiny_un_c", DataTypes.SMALLINT().notNull()),
				DataTypes.FIELD("small_c", DataTypes.SMALLINT().notNull()),
				DataTypes.FIELD("small_un_c", DataTypes.INT().notNull()),
				DataTypes.FIELD("int_c", DataTypes.INT().notNull()),
				DataTypes.FIELD("int_un_c", DataTypes.BIGINT().notNull()),
				DataTypes.FIELD("big_c", DataTypes.BIGINT().notNull()),
				DataTypes.FIELD("big_un_c", DataTypes.DECIMAL(20, 0).notNull())),
			row(
				new Integer(127).byteValue(),
				new Integer(255).shortValue(),
				new Integer(32767).shortValue(),
				Integer.valueOf(65535),
				Integer.valueOf(2147483647),
				Long.valueOf(4294967295L),
				Long.valueOf(9223372036854775807L),
				new BigDecimal(new BigInteger("18446744073709551615"), 0)));
		tEnv.createTemporaryView("data", dataTable);
	}

	@After
	public void cleanup() {
		stopDb();
	}

	private static void stopDb() {
		if (db == null) {
			return;
		}
		try {
			db.stop();
		} catch (ManagedProcessException e1) {
			logger.warn("Stop DB instance fail caused by {}", e1);
		}
	}
}
