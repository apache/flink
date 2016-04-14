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

import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JDBCOutputFormatTest extends JDBCTestBase {

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

		Tuple5 tuple5 = new Tuple5();
		tuple5.setField(4, 0);
		tuple5.setField("hello", 1);
		tuple5.setField("world", 2);
		tuple5.setField(0.99, 3);
		tuple5.setField("imthewrongtype", 4);

		jdbcOutputFormat.writeRecord(tuple5);
		jdbcOutputFormat.close();
	}

	@Test
	public void testJDBCOutputFormat() throws IOException {

		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);

		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.open(null);

		Tuple5 tuple = new Tuple5();
		while (!jdbcInputFormat.reachedEnd()) {
			if(jdbcInputFormat.nextRecord(tuple)!=null){
				jdbcOutputFormat.writeRecord(tuple);
			}
		}

		jdbcOutputFormat.close();
		jdbcInputFormat.close();

		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_NEWBOOKS)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.open(null);

		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(tuple);
			if(tuple.getField(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, tuple.getField(0).getClass());}
			if(tuple.getField(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, tuple.getField(1).getClass());}
			if(tuple.getField(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, tuple.getField(2).getClass());}
			if(tuple.getField(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, tuple.getField(3).getClass());}
			if(tuple.getField(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, tuple.getField(4).getClass());}

			for (int x = 0; x < 5; x++) {
				//TODO how to handle null for double???
				if(JDBCTestBase.testData[recordCount][x]!=null){
					Assert.assertEquals(JDBCTestBase.testData[recordCount][x], tuple.getField(x));
				}
			}

			recordCount++;
		}
		Assert.assertEquals(JDBCTestBase.testData.length, recordCount);

		jdbcInputFormat.close();
	}
}
