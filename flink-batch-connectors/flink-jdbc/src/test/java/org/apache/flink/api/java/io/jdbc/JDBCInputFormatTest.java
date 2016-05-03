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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.io.QueryParamInputSplit;
import org.apache.flink.api.java.io.jdbc.split.GenericSplitsGenerator;
import org.apache.flink.api.java.io.jdbc.split.JDBCInputSplitsGenerator;
import org.apache.flink.api.java.io.jdbc.split.NumericColumnSplitsGenerator;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.table.Row;
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
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(SELECT_ALL_BOOKS)
				.finish();
		jdbcInputFormat.openInputFormat();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.finish();
		jdbcInputFormat.openInputFormat();
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
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row =  new Row(5);
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			jdbcInputFormat.nextRecord(row);
			if(row.productElement(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, row.productElement(0).getClass());}
			if(row.productElement(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, row.productElement(1).getClass());}
			if(row.productElement(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, row.productElement(2).getClass());}
			if(row.productElement(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, row.productElement(3).getClass());}
			if(row.productElement(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, row.productElement(4).getClass());}

			for (int x = 0; x < 5; x++) {
				if(testData[recordCount][x]!=null) {
					Assert.assertEquals(testData[recordCount][x], row.productElement(x));
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
		final Long max = new Long(JDBCTestBase.testData[JDBCTestBase.testData.length-fetchSize][0] + "");
		JDBCInputSplitsGenerator inputSplitsGenerator = new NumericColumnSplitsGenerator(fetchSize, min, max);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_ID)
				.setSplitsGenerator(inputSplitsGenerator)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		int recordCount = 0;
		Row row =  new Row(5);
		for (int i = 0; i < JDBCTestBase.testData.length; i++) {
			jdbcInputFormat.open(inputSplitsGenerator.getInputSplits(1)[i]);
			while (!jdbcInputFormat.reachedEnd()) {
				jdbcInputFormat.nextRecord(row);
				if(row.productElement(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, row.productElement(0).getClass());}
				if(row.productElement(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, row.productElement(1).getClass());}
				if(row.productElement(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, row.productElement(2).getClass());}
				if(row.productElement(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, row.productElement(3).getClass());}
				if(row.productElement(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, row.productElement(4).getClass());}
	
				for (int x = 0; x < 5; x++) {
					if(testData[recordCount][x]!=null) {
						Assert.assertEquals(testData[recordCount][x], row.productElement(x));
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
		QueryParamInputSplit[] splits = new QueryParamInputSplit[2];
		splits[0] = new QueryParamInputSplit(0, new String[]{"Kumar"});
		splits[1] = new QueryParamInputSplit(1, new String[]{"Tan Ah Teck"});
		JDBCInputSplitsGenerator inputSplitsGenerator = new GenericSplitsGenerator(splits);
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(JDBCTestBase.SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR)
				.setSplitsGenerator(inputSplitsGenerator)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		int recordCount = 0;
		Row row =  new Row(5);
		for (int i = 0; i < splits.length; i++) {
			jdbcInputFormat.open(splits[i]);
			while (!jdbcInputFormat.reachedEnd()) {
				jdbcInputFormat.nextRecord(row);
				if(row.productElement(0)!=null) { Assert.assertEquals("Field 0 should be int", Integer.class, row.productElement(0).getClass());}
				if(row.productElement(1)!=null) { Assert.assertEquals("Field 1 should be String", String.class, row.productElement(1).getClass());}
				if(row.productElement(2)!=null) { Assert.assertEquals("Field 2 should be String", String.class, row.productElement(2).getClass());}
				if(row.productElement(3)!=null) { Assert.assertEquals("Field 3 should be float", Double.class, row.productElement(3).getClass());}
				if(row.productElement(4)!=null) { Assert.assertEquals("Field 4 should be int", Integer.class, row.productElement(4).getClass());}

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
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row = new Row(5);
		int loopCnt = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			Assert.assertNull(jdbcInputFormat.nextRecord(row));
			loopCnt++;
		}
		jdbcInputFormat.close();
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(1, loopCnt);

	}
	
	@Test
	public void testUninitializedRow() throws IOException, InstantiationException, IllegalAccessException {
		jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(SELECT_ALL_BOOKS)
				.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)
				.finish();
		jdbcInputFormat.openInputFormat();
		jdbcInputFormat.open(null);
		Row row = new KryoSerializer<Row>(Row.class, new ExecutionConfig()).createInstance();
		int recordCount = 0;
		while (!jdbcInputFormat.reachedEnd()) {
			//re-assign here is necessary...
			row = jdbcInputFormat.nextRecord(row);
			for (int x = 0; x < 5; x++) {
				if(testData[recordCount][x]!=null) {
					Assert.assertEquals(testData[recordCount][x], row.productElement(x));
				}
			}
			recordCount++;
		}
		jdbcInputFormat.close();
		jdbcInputFormat.closeInputFormat();
		Assert.assertEquals(testData.length, recordCount);
	}

}