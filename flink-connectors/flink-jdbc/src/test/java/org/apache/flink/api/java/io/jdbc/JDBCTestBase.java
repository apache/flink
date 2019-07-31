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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Base test class for JDBC Input and Output formats.
 */
public class JDBCTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:ebookshop";
	public static final String INPUT_TABLE = "books";
	public static final String OUTPUT_TABLE = "newbooks";
	public static final String OUTPUT_TABLE_2 = "newbooks2";
	public static final String[] FIELD_NAMES = new String[]{
		"id", "title", "author", "price", "qty", "print_date", "print_time", "print_timestamp"};

	public static final String SELECT_ALL_BOOKS = "select * from " + INPUT_TABLE;
	public static final String SELECT_ALL_NEWBOOKS = "select * from " + OUTPUT_TABLE;
	public static final String SELECT_ALL_NEWBOOKS_2 = "select * from " + OUTPUT_TABLE_2;
	public static final String SELECT_EMPTY = "select * from books WHERE QTY < 0";
	public static final String INSERT_TEMPLATE =
		"insert into %s (" + String.join(",", FIELD_NAMES) + ") values (?,?,?,?,?,?,?,?)";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_ID = SELECT_ALL_BOOKS + " WHERE id BETWEEN ? AND ?";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR = SELECT_ALL_BOOKS + " WHERE author = ?";

	public static final TestEntry[] TEST_DATA = {
			new TestEntry(
				1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11,
				Date.valueOf("2011-01-11"), Time.valueOf("01:11:11"), Timestamp.valueOf("2011-01-11 01:11:11")),
			new TestEntry(
				1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22,
				Date.valueOf("2012-02-12"), Time.valueOf("02:12:12"), Timestamp.valueOf("2012-02-12 02:12:12")),
			new TestEntry(
				1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33,
				Date.valueOf("2013-03-13"), Time.valueOf("03:13:13"), Timestamp.valueOf("2013-03-13 03:13:13")),
			new TestEntry(
				1004, ("A Cup of Java"), ("Kumar"), 44.44, 44,
				Date.valueOf("2014-04-14"), Time.valueOf("04:14:14"), Timestamp.valueOf("2014-04-14 04:14:14")),
			new TestEntry(
				1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55,
				Date.valueOf("2015-05-15"), Time.valueOf("05:15:15"), Timestamp.valueOf("2015-05-15 05:15:15")),
			new TestEntry(
				1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66,
				Date.valueOf("2016-06-16"), Time.valueOf("06:16:16"), Timestamp.valueOf("2016-06-16 06:16:16")),
			new TestEntry(
				1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77,
				Date.valueOf("2017-07-17"), Time.valueOf("07:17:17"), Timestamp.valueOf("2017-07-17 07:17:17")),
			new TestEntry(
				1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88,
				Date.valueOf("2018-08-18"), Time.valueOf("08:18:18"), Timestamp.valueOf("2018-08-18 08:18:18")),
			new TestEntry(
				1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99,
				Date.valueOf("2019-09-19"), Time.valueOf("09:19:19"), Timestamp.valueOf("2019-09-19 09:19:19")),
			new TestEntry(
				1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010,
				Date.valueOf("2020-10-20"), null, Timestamp.valueOf("2020-10-20 10:20:20"))
	};

	static class TestEntry {
		protected final Integer id;
		protected final String title;
		protected final String author;
		protected final Double price;
		protected final Integer qty;
		protected final Date printDate;
		protected final Time printTime;
		protected final Timestamp printTimestamp;

		private TestEntry(
				Integer id, String title, String author, Double price, Integer qty,
				Date printDate, Time printTime, Timestamp printTimestamp) {
			this.id = id;
			this.title = title;
			this.author = author;
			this.price = price;
			this.qty = qty;
			this.printDate = printDate;
			this.printTime = printTime;
			this.printTimestamp = printTimestamp;
		}
	}

	public static final RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.INT_TYPE_INFO,
		SqlTimeTypeInfo.DATE,
		SqlTimeTypeInfo.TIME,
		SqlTimeTypeInfo.TIMESTAMP);

	public static final int[] SQL_TYPES = new int[] {
		Types.INTEGER,
		Types.VARCHAR,
		Types.VARCHAR,
		Types.DOUBLE,
		Types.INTEGER,
		Types.DATE,
		Types.TIME,
		Types.TIMESTAMP};

	public static String getCreateQuery(String tableName) {
		StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE ");
		sqlQueryBuilder.append(tableName).append(" (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
		sqlQueryBuilder.append("qty INT DEFAULT NULL,");
		sqlQueryBuilder.append("print_date DATE DEFAULT NULL,");
		sqlQueryBuilder.append("print_time TIME DEFAULT NULL,");
		sqlQueryBuilder.append("print_timestamp TIMESTAMP DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");
		return sqlQueryBuilder.toString();
	}

	public static String getInsertQuery() {
		boolean[] surroundedByQuotes = new boolean[] {
			false, true, true, false, false, true, true, true
		};

		StringBuilder sqlQueryBuilder = new StringBuilder(
			"INSERT INTO books (" + String.join(", ", FIELD_NAMES) + ") VALUES ");
		for (int i = 0; i < TEST_DATA.length; i++) {
			sqlQueryBuilder.append("(");
			Object[] objs = new Object[]{
				TEST_DATA[i].id, TEST_DATA[i].title, TEST_DATA[i].author, TEST_DATA[i].price, TEST_DATA[i].qty,
				TEST_DATA[i].printDate, TEST_DATA[i].printTime, TEST_DATA[i].printTimestamp
			};
			for (int j = 0; j < objs.length; j++) {
				if (objs[j] == null) {
					sqlQueryBuilder.append("null");
				} else {
					if (surroundedByQuotes[j]) {
						sqlQueryBuilder.append("'");
					}
					sqlQueryBuilder.append(objs[j]);
					if (surroundedByQuotes[j]) {
						sqlQueryBuilder.append("'");
					}
				}
				if (j < objs.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			sqlQueryBuilder.append(")");
			if (i < TEST_DATA.length - 1) {
				sqlQueryBuilder.append(", ");
			}
		}
		return sqlQueryBuilder.toString();
	}

	public static final OutputStream DEV_NULL = new OutputStream() {
		@Override
		public void write(int b) {
		}
	};

	@BeforeClass
	public static void prepareDerbyDatabase() throws Exception {
		System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DRIVER_CLASS);
		try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true")) {
			createTable(conn, JDBCTestBase.INPUT_TABLE);
			createTable(conn, OUTPUT_TABLE);
			createTable(conn, OUTPUT_TABLE_2);
			insertDataIntoInputTable(conn);
		}
	}

	private static void createTable(Connection conn, String tableName) throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery(tableName));
		stat.close();
	}

	private static void insertDataIntoInputTable(Connection conn) throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(getInsertQuery());
		stat.close();
	}

	@AfterClass
	public static void cleanUpDerbyDatabases() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {

			stat.executeUpdate("DROP TABLE " + INPUT_TABLE);
			stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE);
			stat.executeUpdate("DROP TABLE " + OUTPUT_TABLE_2);
		}
	}
}
