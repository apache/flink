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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;


import org.junit.Assert;
import org.apache.flink.api.java.io.jdbc.example.JDBCExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JDBCInputFormatTest {
	
	private static Connection conn;
	
	private JDBCInputFormat jdbcInputFormat;

	@BeforeClass
	public static void setUpClass() {
		try {
			prepareDerbyDatabase();
		} catch (Exception e) {
			Assert.fail();
		}
	}

	private static void prepareDerbyDatabase() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", "org.apache.flink.api.java.io.jdbc.DerbyUtil.DEV_NULL");
		Class.forName(JDBCExample.DRIVER_CLASS);
		conn = DriverManager.getConnection(JDBCExample.DB_URL+";create=true");
		createTable();
		insertDataToSQLTable();
		conn.close();
	}

	private static void createTable() throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate(JDBCExample.getCreateQuery());
		stat.close();
	}


	private static void insertDataToSQLTable() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute(JDBCExample.getInsertQuery());
		stat.close();
	}

	

	@AfterClass
	public static void tearDownClass() {
		cleanUpDerbyDatabases();
	}

	private static void cleanUpDerbyDatabases() {
		try {
			Class.forName(JDBCExample.DRIVER_CLASS);
			conn = DriverManager.getConnection(JDBCExample.DB_URL);
			Statement stat = conn.createStatement();
			stat.execute("DROP TABLE books");
			stat.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@After
	public void tearDown() throws IOException {
		if(jdbcInputFormat!=null)
			jdbcInputFormat.close();
		jdbcInputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(JDBCExample.DB_URL)
				.setQuery(JDBCExample.SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCExample.DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(JDBCExample.SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCExample.DRIVER_CLASS)
				.setDBUrl(JDBCExample.DB_URL)
				.setQuery("iamnotsql")
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCExample.DRIVER_CLASS)
				.setQuery(JDBCExample.SELECT_ALL_BOOKS)
				.finish();
	}

	@Test(expected = IOException.class)
	public void testIncompatibleTuple() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCExample.DRIVER_CLASS)
				.setDBUrl(JDBCExample.DB_URL)
				.setQuery(JDBCExample.SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.open(null);
		jdbcInputFormat.nextRecord(new Tuple2());
	}

	@Test
	public void testJDBCInputFormatWithoutParallelism() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(JDBCExample.DRIVER_CLASS)
				.setDBUrl(JDBCExample.DB_URL)
				.setQuery(JDBCExample.SELECT_ALL_BOOKS)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.open(null);
		Tuple5 tuple = new Tuple5();
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(tuple);
			if(tuple.getField(0)!=null) Assert.assertEquals("Field 0 should be int", Integer.class, tuple.getField(0).getClass());
			if(tuple.getField(1)!=null) Assert.assertEquals("Field 1 should be String", String.class, tuple.getField(1).getClass());
			if(tuple.getField(2)!=null) Assert.assertEquals("Field 2 should be String", String.class, tuple.getField(2).getClass());
			if(tuple.getField(3)!=null) Assert.assertEquals("Field 3 should be float", Double.class, tuple.getField(3).getClass());
			if(tuple.getField(4)!=null) Assert.assertEquals("Field 4 should be int", Integer.class, tuple.getField(4).getClass());

			for (int x = 0; x < 5; x++) {
				//TODO how to handle null for double???
				if(JDBCExample.testData[recordCount][x]!=null)
					Assert.assertEquals(JDBCExample.testData[recordCount][x], tuple.getField(x));
			}
			recordCount++;
		}
		Assert.assertEquals(10, recordCount);
	}

}
