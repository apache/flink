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
import java.io.Serializable;
import java.sql.ResultSet;

import org.apache.flink.api.java.io.jdbc.split.GenericParameterValuesProvider;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.types.Row;
import org.apache.flink.core.io.InputSplit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class JDBCInputFormatTest extends JDBCTestBase {

	private JDBCInputFormat jdbcInputFormat;

	@After
	public void tearDown() throws IOException {
		if (jdbcInputFormat != null) {
			jdbcInputFormat.close();
		}
		jdbcInputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUntypedRowInfo() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(rowTypeInfo)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(rowTypeInfo)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.setRowTypeInfo(rowTypeInfo)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(rowTypeInfo)
				.finish();
	}

	@Test
	public void testJDBCInputFormatWithoutParallelism() throws IOException, InstantiationException, IllegalAccessException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setRowTypeInfo(rowTypeInfo)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		//this query does not exploit parallelism
		Assert.assertEquals(1, jdbcInputFormat.createInputSplits(1).length);
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row =  new Row(5);
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			Row next = jdbcInputFormat.nextRecord(row);
			if (next == null) {
				break;
			}
			
			if(next.getField(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, next.getField(0).getClass());}
			if(next.getField(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, next.getField(1).getClass());}
			if(next.getField(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, next.getField(2).getClass());}
			if(next.getField(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, next.getField(3).getClass());}
			if(next.getField(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, next.getField(4).getClass());}

			for (int x = 0; x < 5; x++) {
				if(testData[recordCount][x]!=null) {
					Assert.assertEquals(testData[recordCount][x], next.getField(x));
				}
			}
			recordCount++;
		}
		jdbcInputFormat.close();
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(testData.length, recordCount);
	}
	
	@Test
	public void testJDBCInputFormatWithParallelismAndNumericColumnSplitting() throws IOException, InstantiationException, IllegalAccessException {
		final int fetchSize = 1;
		final Long min = new Long(JDBCTestBase.testData[0][0] + "");
		final Long max = new Long(JDBCTestBase.testData[JDBCTestBase.testData.length - fetchSize][0] + "");
		ParameterValuesProvider pramProvider = new NumericBetweenParametersProvider(fetchSize, min, max);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
				.setRowTypeInfo(rowTypeInfo)
				.setParametersProvider(pramProvider)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();

		jdbcInputFormat.openInputFormat();
		InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every id)
		Assert.assertEquals(testData.length, splits.length);
		int recordCount = 0;
		Row row =  new Row(5);
		for (int i = 0; i < splits.length; i++) {
			jdbcInputFormat.open(splits[i]);
			while (!jdbcInputFormat.reachedEnd()) {
				Row next = jdbcInputFormat.nextRecord(row);
				if (next == null) {
					break;
				}
				if(next.getField(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, next.getField(0).getClass());}
				if(next.getField(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, next.getField(1).getClass());}
				if(next.getField(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, next.getField(2).getClass());}
				if(next.getField(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, next.getField(3).getClass());}
				if(next.getField(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, next.getField(4).getClass());}

				for (int x = 0; x < 5; x++) {
					if(testData[recordCount][x]!=null) {
						Assert.assertEquals(testData[recordCount][x], next.getField(x));
					}
				}
				recordCount++;
			}
			jdbcInputFormat.close();
		}
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(testData.length, recordCount);
	}
	
	@Test
	public void testJDBCInputFormatWithParallelismAndGenericSplitting() throws IOException, InstantiationException, IllegalAccessException {
		Serializable[][] queryParameters = new String[2][1];
		queryParameters[0] = new String[]{"Kumar"};
		queryParameters[1] = new String[]{"Tan Ah Teck"};
		ParameterValuesProvider paramProvider = new GenericParameterValuesProvider(queryParameters);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR)
				.setRowTypeInfo(rowTypeInfo)
				.setParametersProvider(paramProvider)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		InputSplit[] splits = jdbcInputFormat.createInputSplits(1);
		//this query exploit parallelism (1 split for every queryParameters row)
		Assert.assertEquals(queryParameters.length, splits.length);
		int recordCount = 0;
		Row row =  new Row(5);
		for (int i = 0; i < splits.length; i++) {
			jdbcInputFormat.open(splits[i]);
			while (!jdbcInputFormat.reachedEnd()) {
				Row next = jdbcInputFormat.nextRecord(row);
				if (next == null) {
					break;
				}
				if(next.getField(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, next.getField(0).getClass());}
				if(next.getField(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, next.getField(1).getClass());}
				if(next.getField(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, next.getField(2).getClass());}
				if(next.getField(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, next.getField(3).getClass());}
				if(next.getField(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, next.getField(4).getClass());}

				recordCount++;
			}
			jdbcInputFormat.close();
		}
		Assert.assertEquals(3, recordCount);
		jdbcInputFormat.closeInputFormat();
	}
	
	@Test
	public void testEmptyResults() throws IOException, InstantiationException, IllegalAccessException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_EMPTY)
				.setRowTypeInfo(rowTypeInfo)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row = new Row(5);
		int recordsCnt = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			Assert.assertNull(jdbcInputFormat.nextRecord(row));
			recordsCnt++;
		}
		jdbcInputFormat.close();
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(0, recordsCnt);
	}

}
