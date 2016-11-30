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

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * Base test class for JDBC Input and Output formats
 */
public class JDBCTestBase {
	
	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DB_URL = "jdbc:derby:memory:ebookshop";
	public static final String INPUT_TABLE = "books";
	public static final String OUTPUT_TABLE = "newbooks";
	public static final String SELECT_ALL_BOOKS = "select * from " + INPUT_TABLE;
	public static final String SELECT_ALL_NEWBOOKS = "select * from " + OUTPUT_TABLE;
	public static final String SELECT_EMPTY = "select * from books WHERE QTY < 0";
	public static final String INSERT_TEMPLATE = "insert into %s (id, title, author, price, qty) values (?,?,?,?,?)";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_ID = JDBCTestBase.SELECT_ALL_BOOKS + " WHERE id BETWEEN ? AND ?";
	public static final String SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR = JDBCTestBase.SELECT_ALL_BOOKS + " WHERE author = ?";
	
	protected static Connection conn;

	public static final Object[][] testData = {
			{1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11},
			{1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22},
			{1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33},
			{1004, ("A Cup of Java"), ("Kumar"), 44.44, 44},
			{1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55},
			{1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66},
			{1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77},
			{1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88},
			{1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99},
			{1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010}};

	public static final TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.INT_TYPE_INFO
	};
	
	public static final RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

	public static String getCreateQuery(String tableName) {
		StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE ");
		sqlQueryBuilder.append(tableName).append(" (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
		sqlQueryBuilder.append("qty INT DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");
		return sqlQueryBuilder.toString();
	}
	
	public static String getInsertQuery() {
		StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
		for (int i = 0; i < JDBCTestBase.testData.length; i++) {
			sqlQueryBuilder.append("(")
			.append(JDBCTestBase.testData[i][0]).append(",'")
			.append(JDBCTestBase.testData[i][1]).append("','")
			.append(JDBCTestBase.testData[i][2]).append("',")
			.append(JDBCTestBase.testData[i][3]).append(",")
			.append(JDBCTestBase.testData[i][4]).append(")");
			if (i < JDBCTestBase.testData.length - 1) {
				sqlQueryBuilder.append(",");
			}
		}
		String insertQuery = sqlQueryBuilder.toString();
		return insertQuery;
	}
	
	public static final OutputStream DEV_NULL = new OutputStream() {
		@Override
		public void write(int b) {
		}
	};

	public static void prepareTestDb() throws Exception {
		System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");
		Class.forName(DRIVER_CLASS);
		Connection conn = DriverManager.getConnection(DB_URL + ";create=true");

		//create input table
		Statement stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery(INPUT_TABLE));
		stat.close();

		//create output table
		stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery(OUTPUT_TABLE));
		stat.close();

		//prepare input data
		stat = conn.createStatement();
		stat.execute(JDBCTestBase.getInsertQuery());
		stat.close();

		conn.close();
	}

	@BeforeClass
	public static void setUpClass() throws SQLException {
		try {
			System.setProperty("derby.stream.error.field", JDBCTestBase.class.getCanonicalName() + ".DEV_NULL");
			prepareDerbyDatabase();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	private static void prepareDerbyDatabase() throws ClassNotFoundException, SQLException {
		Class.forName(DRIVER_CLASS);
		conn = DriverManager.getConnection(DB_URL + ";create=true");
		createTable(INPUT_TABLE);
		createTable(OUTPUT_TABLE);
		insertDataIntoInputTable();
		conn.close();
	}
	
	private static void createTable(String tableName) throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate(getCreateQuery(tableName));
		stat.close();
	}
	
	private static void insertDataIntoInputTable() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(JDBCTestBase.getInsertQuery());
		stat.close();
	}

	@AfterClass
	public static void tearDownClass() {
		cleanUpDerbyDatabases();
	}

	private static void cleanUpDerbyDatabases() {
		try {
			Class.forName(DRIVER_CLASS);
			conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement();
			stat.executeUpdate("DROP TABLE "+INPUT_TABLE);
			stat.executeUpdate("DROP TABLE "+OUTPUT_TABLE);
			stat.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

}
