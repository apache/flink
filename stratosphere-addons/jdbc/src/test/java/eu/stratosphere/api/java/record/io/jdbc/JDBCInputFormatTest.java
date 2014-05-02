/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
* http://www.apache.org/licenses/LICENSE-2.0
 * 
* Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package eu.stratosphere.api.java.record.io.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class JDBCInputFormatTest {
	JDBCInputFormat jdbcInputFormat;
	Configuration config;
	static Connection conn;
	static final Value[][] dbData = {
		{new IntValue(1001), new StringValue("Java for dummies"), new StringValue("Tan Ah Teck"), new DoubleValue(11.11), new IntValue(11)},
		{new IntValue(1002), new StringValue("More Java for dummies"), new StringValue("Tan Ah Teck"), new DoubleValue(22.22), new IntValue(22)},
		{new IntValue(1003), new StringValue("More Java for more dummies"), new StringValue("Mohammad Ali"), new DoubleValue(33.33), new IntValue(33)},
		{new IntValue(1004), new StringValue("A Cup of Java"), new StringValue("Kumar"), new DoubleValue(44.44), new IntValue(44)},
		{new IntValue(1005), new StringValue("A Teaspoon of Java"), new StringValue("Kevin Jones"), new DoubleValue(55.55), new IntValue(55)}};

	@BeforeClass
	public static void setUpClass() {
		try {
			prepareDerbyDatabase();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	private static void prepareDerbyDatabase() throws ClassNotFoundException {
		System.setProperty("derby.stream.error.field","eu.stratosphere.api.java.record.io.jdbc.DevNullLogStream.DEV_NULL");
		String dbURL = "jdbc:derby:memory:ebookshop;create=true";
		createConnection(dbURL);
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
	
	/*
	 Loads JDBC derby driver ; creates(if necessary) and populates database.
	 */
	private static void createConnection(String dbURL) {
		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			conn = DriverManager.getConnection(dbURL);
			createTable();
			insertDataToSQLTables();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
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

		sqlQueryBuilder = new StringBuilder("CREATE TABLE bookscontent (");
		sqlQueryBuilder.append("id INT NOT NULL DEFAULT 0,");
		sqlQueryBuilder.append("title VARCHAR(50) DEFAULT NULL,");
		sqlQueryBuilder.append("content BLOB(10K) DEFAULT NULL,");
		sqlQueryBuilder.append("PRIMARY KEY (id))");

		stat = conn.createStatement();
		stat.executeUpdate(sqlQueryBuilder.toString());
		stat.close();
	}

	private static void insertDataToSQLTables() throws SQLException {
		StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO books (id, title, author, price, qty) VALUES ");
		sqlQueryBuilder.append("(1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11),");
		sqlQueryBuilder.append("(1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22),");
		sqlQueryBuilder.append("(1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33),");
		sqlQueryBuilder.append("(1004, 'A Cup of Java', 'Kumar', 44.44, 44),");
		sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

		Statement stat = conn.createStatement();
		stat.execute(sqlQueryBuilder.toString());
		stat.close();

		sqlQueryBuilder = new StringBuilder("INSERT INTO bookscontent (id, title, content) VALUES ");
		sqlQueryBuilder.append("(1001, 'Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1002, 'More Java for dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1003, 'More Java for more dummies', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1004, 'A Cup of Java', CAST(X'7f454c4602' AS BLOB)),");
		sqlQueryBuilder.append("(1005, 'A Teaspoon of Java', CAST(X'7f454c4602' AS BLOB))");

		stat = conn.createStatement();
		stat.execute(sqlQueryBuilder.toString());
		stat.close();
	}


	@After
	public void tearDown() {
		jdbcInputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidConnection() {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:idontexist", "select * from books;");
		jdbcInputFormat.configure(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "abc");
		jdbcInputFormat.configure(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDBType() {
		jdbcInputFormat = new JDBCInputFormat("idontexist.Driver", "jdbc:derby:memory:ebookshop", "select * from books;");
		jdbcInputFormat.configure(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUnsupportedSQLType() {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from bookscontent");
		jdbcInputFormat.configure(null);
		jdbcInputFormat.nextRecord(new Record());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNotConfiguredFormatNext() {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
		jdbcInputFormat.nextRecord(new Record());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNotConfiguredFormatEnd() {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
		jdbcInputFormat.reachedEnd();
	}

	@Test
	public void testJDBCInputFormat() throws IOException {
		jdbcInputFormat = new JDBCInputFormat("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:ebookshop", "select * from books");
		jdbcInputFormat.configure(null);
		Record record = new Record();
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(record);
			Assert.assertEquals(5, record.getNumFields());
			Assert.assertEquals("Field 0 should be int", IntValue.class, record.getField(0, IntValue.class).getClass());
			Assert.assertEquals("Field 1 should be String", StringValue.class, record.getField(1, StringValue.class).getClass());
			Assert.assertEquals("Field 2 should be String", StringValue.class, record.getField(2, StringValue.class).getClass());
			Assert.assertEquals("Field 3 should be float", DoubleValue.class, record.getField(3, DoubleValue.class).getClass());
			Assert.assertEquals("Field 4 should be int", IntValue.class, record.getField(4, IntValue.class).getClass());

			int[] pos = {0, 1, 2, 3, 4};
			Value[] values = {new IntValue(), new StringValue(), new StringValue(), new DoubleValue(), new IntValue()};
			Assert.assertTrue(record.equalsFields(pos, dbData[recordCount], values));

			recordCount++;
		}
		Assert.assertEquals(5, recordCount);
		
		cleanUpDerbyDatabases();
	}

}
