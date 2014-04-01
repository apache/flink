/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io.jdbc;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JDBCInputFormatTest {
	JDBCInputFormat jdbcInputFormat;

	static Connection conn;

	static final Object[][] dbData = {
		{1001, ("Java for dummies"), ("Tan Ah Teck"), 11.11, 11},
		{1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22},
		{1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33},
		{1004, ("A Cup of Java"), ("Kumar"), 44.44, 44},
		{1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55}};

	@BeforeClass
	public static void setUpClass() {
		try {
			prepareDerbyDatabase();
		} catch (Exception e) {
			Assert.fail();
		}
	}

	private static void prepareDerbyDatabase() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", "eu.stratosphere.api.java.record.io.jdbc.DevNullLogStream.DEV_NULL");
		String dbURL = "jdbc:derby:memory:ebookshop;create=true";
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		conn = DriverManager.getConnection(dbURL);
		createTable();
		insertDataToSQLTable();
		conn.close();
	}

	private static void createTable() throws SQLException {
		StringBuilder sqlQueryBuilder = new StringBuilder("CREATE TABLE books (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("author VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("price FLOAT DEFAULT NULL,");
		sqlQueryBuilder.append("qty INT DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");

		Statement stat = conn.createStatement();
		stat.executeUpdate(sqlQueryBuilder.toString());
		stat.close();
	}

	private static void insertDataToSQLTable() throws SQLException {
		StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
		sqlQueryBuilder.append("(1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11),");
		sqlQueryBuilder.append("(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),");
		sqlQueryBuilder.append("(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),");
		sqlQueryBuilder.append("(1004, 'A Cup of Java', 'Kumar', 44.44, 44),");
		sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

		Statement stat = conn.createStatement();
		stat.execute(sqlQueryBuilder.toString());
		stat.close();
	}

	@AfterClass
	public static void tearDownClass() {
		cleanUpDerbyDatabases();
	}

	private static void cleanUpDerbyDatabases() {
		try {
			String dbURL = "jdbc:derby:memory:ebookshop;create=true";
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

			conn = DriverManager.getConnection(dbURL);
			Statement stat = conn.createStatement();
			stat.executeUpdate("DROP TABLE books");
			stat.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@After
	public void tearDown() {
		jdbcInputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl("jdbc:derby:memory:ebookshop")
				.setQuery("select * from books")
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery("select * from books")
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:derby:memory:ebookshop")
				.setQuery("iamnotsql")
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setQuery("select * from books")
				.finish();
	}

	@Test(expected = IOException.class)
	public void testIncompatibleTuple() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:derby:memory:ebookshop")
				.setQuery("select * from books")
				.finish();
		jdbcInputFormat.open(null);
		jdbcInputFormat.nextRecord(new Tuple2());
	}

	@Test
	public void testJDBCInputFormat() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
				.setDBUrl("jdbc:derby:memory:ebookshop")
				.setQuery("select * from books")
				.finish();
		jdbcInputFormat.open(null);
		Tuple5 tuple = new Tuple5();
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(tuple);
			Assert.assertEquals("Field 0 should be int", Integer.class, tuple.getField(0).getClass());
			Assert.assertEquals("Field 1 should be String", String.class, tuple.getField(1).getClass());
			Assert.assertEquals("Field 2 should be String", String.class, tuple.getField(2).getClass());
			Assert.assertEquals("Field 3 should be float", Double.class, tuple.getField(3).getClass());
			Assert.assertEquals("Field 4 should be int", Integer.class, tuple.getField(4).getClass());

			for (int x = 0; x < 5; x++) {
				Assert.assertEquals(dbData[recordCount][x], tuple.getField(x));
			}
			recordCount++;
		}
		Assert.assertEquals(5, recordCount);
	}

}
