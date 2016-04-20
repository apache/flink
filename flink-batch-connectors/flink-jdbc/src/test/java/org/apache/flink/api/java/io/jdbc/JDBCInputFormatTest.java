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
import java.sql.ResultSet;

import org.apache.flink.api.java.io.GenericRow;
import org.junit.Assert;
import org.junit.Test;

public class JDBCInputFormatTest extends JDBCTestBase {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.finish();
		jdbcInputFormat.open(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
	}

	@Test
	public void testJDBCInputFormatWithoutParallelism() throws IOException, InstantiationException, IllegalAccessException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.open(null);
		GenericRow tuple =  GenericRow.class.newInstance();
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(tuple);
			if(tuple.getField(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, tuple.getField(0).getClass());}
			if(tuple.getField(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, tuple.getField(1).getClass());}
			if(tuple.getField(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, tuple.getField(2).getClass());}
			if(tuple.getField(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, tuple.getField(3).getClass());}
			if(tuple.getField(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, tuple.getField(4).getClass());}

			for (int x = 0; x < 5; x++) {
				if(testData[recordCount][x]!=null) {
					Assert.assertEquals(testData[recordCount][x], tuple.getField(x));
				}
			}
			recordCount++;
		}
		Assert.assertEquals(testData.length, recordCount);
	}
	
	@Test
	public void testEmptyResults() throws IOException, InstantiationException, IllegalAccessException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_EMPTY)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.open(null);
		GenericRow tuple = GenericRow.class.newInstance();
		int loopCnt = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			Assert.assertNull(jdbcInputFormat.nextRecord(tuple));
			loopCnt++;
		}
		Assert.assertEquals(1, loopCnt);
	}

}