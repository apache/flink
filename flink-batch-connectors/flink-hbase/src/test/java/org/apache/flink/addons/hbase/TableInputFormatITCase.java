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

package org.apache.flink.addons.hbase;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableInputFormatITCase extends HBaseTestingClusterAutostarter {
	private static final String TEST_TABLE_NAME = "TableInputFormatTestTable";
	private static final byte[] TEST_TABLE_FAMILY_NAME = "F".getBytes();
	private static final byte[] TEST_TABLE_COLUMN_NAME = "Col".getBytes();

	// These are the row ids AND also the values we will put in the test table
	private static final String[] ROW_IDS = {"000", "111", "222", "333", "444", "555", "666", "777", "888", "999"};

	@BeforeClass
	public static void activateHBaseCluster(){
		registerHBaseMiniClusterInClasspath();
	}

	@Before
	public void createTestTable() throws IOException {
		TableName tableName = TableName.valueOf(TEST_TABLE_NAME);
		byte[][] splitKeys = {"0".getBytes(), "3".getBytes(), "6".getBytes(), "9".getBytes()};
		createTable(tableName, TEST_TABLE_FAMILY_NAME, splitKeys);
		HTable table = openTable(tableName);

		for (String rowId : ROW_IDS) {
			byte[] rowIdBytes = rowId.getBytes();
			Put p = new Put(rowIdBytes);
			// Use the rowId as the value to facilitate the testing better
			p.add(TEST_TABLE_FAMILY_NAME, TEST_TABLE_COLUMN_NAME, rowIdBytes);
			table.put(p);
		}

		table.close();
	}

	class InputFormatForTestTable extends TableInputFormat<Tuple1<String>> {
		@Override
		protected Scan getScanner() {
			return new Scan();
		}

		@Override
		protected String getTableName() {
			return TEST_TABLE_NAME;
		}

		@Override
		protected Tuple1<String> mapResultToTuple(Result r) {
			return new Tuple1<>(new String(r.getValue(TEST_TABLE_FAMILY_NAME, TEST_TABLE_COLUMN_NAME)));
		}
	}

	@Test
	public void testTableInputFormat() {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		environment.setParallelism(1);

		DataSet<String> resultDataSet =
			environment.createInput(new InputFormatForTestTable()).map(new MapFunction<Tuple1<String>, String>() {
				@Override
				public String map(Tuple1<String> value) throws Exception {
					return value.f0;
				}
			});

		List<String> resultSet = new ArrayList<>();
		resultDataSet.output(new LocalCollectionOutputFormat<>(resultSet));

		try {
			environment.execute("HBase InputFormat Test");
		} catch (Exception e) {
			Assert.fail("HBase InputFormat test failed. " + e.getMessage());
		}

		for (String rowId : ROW_IDS) {
			assertTrue("Missing rowId from table: " + rowId, resultSet.contains(rowId));
		}

		assertEquals("The number of records is wrong.", ROW_IDS.length, resultSet.size());
	}

}
