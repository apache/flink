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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class JDBCOutputFormatTest extends JDBCTestBase {

	private JDBCOutputFormat jdbcOutputFormat;
	private Tuple5<Integer, String, String, Double, String> tuple5 = new Tuple5<>();

	@After
	public void tearDown() throws IOException {
		if (jdbcOutputFormat != null) {
			jdbcOutputFormat.close();
		}
		jdbcOutputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
	}


	@Test(expected = IllegalArgumentException.class)
	public void testIncompatibleTypes() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);

		tuple5.setField(4, 0);
		tuple5.setField("hello", 1);
		tuple5.setField("world", 2);
		tuple5.setField(0.99, 3);
		tuple5.setField("imthewrongtype", 4);

		Row row = new Row(tuple5.getArity());
		for (int i = 0; i < tuple5.getArity(); i++) {
			row.setField(i, tuple5.getField(i));
		}
		jdbcOutputFormat.writeRecord(row);
		jdbcOutputFormat.close();
	}

	@Test
	public void testJDBCOutputFormat() throws IOException, InstantiationException, IllegalAccessException {

		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);

		for (int i = 0; i < testData.length; i++) {
			Row row = new Row(testData[i].length);
			for (int j = 0; j < testData[i].length; j++) {
				row.setField(j, testData[i][j]);
			}
			jdbcOutputFormat.writeRecord(row);
		}

		jdbcOutputFormat.close();

		try (
			Connection dbConn = DriverManager.getConnection(JDBCTestBase.DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
			ResultSet resultSet = statement.executeQuery()
		) {
			int recordCount = 0;
			while (resultSet.next()) {
				Row row = new Row(tuple5.getArity());
				for (int i = 0; i < tuple5.getArity(); i++) {
					row.setField(i, resultSet.getObject(i + 1));
				}
				if (row.getField(0) != null) {
					Assert.assertEquals("Field 0 should be int", Integer.class, row.getField(0).getClass());
				}
				if (row.getField(1) != null) {
					Assert.assertEquals("Field 1 should be String", String.class, row.getField(1).getClass());
				}
				if (row.getField(2) != null) {
					Assert.assertEquals("Field 2 should be String", String.class, row.getField(2).getClass());
				}
				if (row.getField(3) != null) {
					Assert.assertEquals("Field 3 should be float", Double.class, row.getField(3).getClass());
				}
				if (row.getField(4) != null) {
					Assert.assertEquals("Field 4 should be int", Integer.class, row.getField(4).getClass());
				}

				for (int x = 0; x < tuple5.getArity(); x++) {
					if (JDBCTestBase.testData[recordCount][x] != null) {
						Assert.assertEquals(JDBCTestBase.testData[recordCount][x], row.getField(x));
					}
				}

				recordCount++;
			}
			Assert.assertEquals(JDBCTestBase.testData.length, recordCount);
		} catch (SQLException e) {
			Assert.fail("JDBC OutputFormat test failed. " + e.getMessage());
		}
	}
}
