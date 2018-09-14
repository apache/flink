/*
 * Copyright The Apache Software Foundation
 *
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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This class contains integrations tests for multiple HBase connectors:
 * - TableInputFormat
 * - HBaseTableSource
 *
 * <p>These tests are located in a single test file to avoided unnecessary initializations of the
 * HBaseTestingCluster which takes about half a minute.
 *
 */
public class HBaseConnectorITCase extends HBaseTestingClusterAutostarter {

	private static final String TEST_TABLE = "testTable";

	private static final String FAMILY1 = "family1";
	private static final String F1COL1 = "col1";

	private static final String FAMILY2 = "family2";
	private static final String F2COL1 = "col1";
	private static final String F2COL2 = "col2";

	private static final String FAMILY3 = "family3";
	private static final String F3COL1 = "col1";
	private static final String F3COL2 = "col2";
	private static final String F3COL3 = "col3";

	@BeforeClass
	public static void activateHBaseCluster() throws IOException {
		registerHBaseMiniClusterInClasspath();
		prepareTable();
		LimitNetworkBuffersTestEnvironment.setAsContext();
	}

	@AfterClass
	public static void resetExecutionEnvironmentFactory() {
		LimitNetworkBuffersTestEnvironment.unsetAsContext();
	}

	private static void prepareTable() throws IOException {

		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE);
		// column families
		byte[][] families = new byte[][]{
			Bytes.toBytes(FAMILY1),
			Bytes.toBytes(FAMILY2),
			Bytes.toBytes(FAMILY3)
		};
		// split keys
		byte[][] splitKeys = new byte[][]{ Bytes.toBytes(4) };
		createTable(tableName, families, splitKeys);

		// get the HTable instance
		HTable table = openTable(tableName);
		List<Put> puts = new ArrayList<>();
		// add some data
		puts.add(putRow(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1"));
		puts.add(putRow(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2"));
		puts.add(putRow(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3"));
		puts.add(putRow(4, 40, null, 400L, 4.04, true, "Welt-4"));
		puts.add(putRow(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5"));
		puts.add(putRow(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6"));
		puts.add(putRow(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7"));
		puts.add(putRow(8, 80, null, 800L, 8.08, true, "Welt-8"));

		// append rows to table
		table.put(puts);
		table.close();
	}

	private static Put putRow(int rowKey, int f1c1, String f2c1, long f2c2, double f3c1, boolean f3c2, String f3c3) {
		Put put = new Put(Bytes.toBytes(rowKey));
		// family 1
		put.addColumn(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1), Bytes.toBytes(f1c1));
		// family 2
		if (f2c1 != null) {
			put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL1), Bytes.toBytes(f2c1));
		}
		put.addColumn(Bytes.toBytes(FAMILY2), Bytes.toBytes(F2COL2), Bytes.toBytes(f2c2));
		// family 3
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL1), Bytes.toBytes(f3c1));
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL2), Bytes.toBytes(f3c2));
		put.addColumn(Bytes.toBytes(FAMILY3), Bytes.toBytes(F3COL3), Bytes.toBytes(f3c3));

		return put;
	}

	// ######## HBaseTableSource tests ############

	@Test
	public void testTableSourceFullScan() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tableEnv.registerTableSource("hTable", hbaseTable);

		Table result = tableEnv.sqlQuery(
			"SELECT " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3 " +
				"FROM hTable AS h"
		);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected =
			"10,Hello-1,100,1.01,false,Welt-1\n" +
			"20,Hello-2,200,2.02,true,Welt-2\n" +
			"30,Hello-3,300,3.03,false,Welt-3\n" +
			"40,null,400,4.04,true,Welt-4\n" +
			"50,Hello-5,500,5.05,false,Welt-5\n" +
			"60,Hello-6,600,6.06,true,Welt-6\n" +
			"70,Hello-7,700,7.07,false,Welt-7\n" +
			"80,null,800,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceProjection() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tableEnv.registerTableSource("hTable", hbaseTable);

		Table result = tableEnv.sqlQuery(
			"SELECT " +
				"  h.family1.col1, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3 " +
				"FROM hTable AS h"
		);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected =
			"10,1.01,false,Welt-1\n" +
			"20,2.02,true,Welt-2\n" +
			"30,3.03,false,Welt-3\n" +
			"40,4.04,true,Welt-4\n" +
			"50,5.05,false,Welt-5\n" +
			"60,6.06,true,Welt-6\n" +
			"70,7.07,false,Welt-7\n" +
			"80,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceFieldOrder() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE);
		// shuffle order of column registration
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tableEnv.registerTableSource("hTable", hbaseTable);

		Table result = tableEnv.sqlQuery(
			"SELECT * FROM hTable AS h"
		);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected =
			"Hello-1,100,1.01,false,Welt-1,10\n" +
			"Hello-2,200,2.02,true,Welt-2,20\n" +
			"Hello-3,300,3.03,false,Welt-3,30\n" +
			"null,400,4.04,true,Welt-4,40\n" +
			"Hello-5,500,5.05,false,Welt-5,50\n" +
			"Hello-6,600,6.06,true,Welt-6,60\n" +
			"Hello-7,700,7.07,false,Welt-7,70\n" +
			"null,800,8.08,true,Welt-8,80\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceReadAsByteArray() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		// fetch row2 from the table till the end
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE);
		hbaseTable.addColumn(FAMILY2, F2COL1, byte[].class);
		hbaseTable.addColumn(FAMILY2, F2COL2, byte[].class);

		tableEnv.registerTableSource("hTable", hbaseTable);
		tableEnv.registerFunction("toUTF8", new ToUTF8());
		tableEnv.registerFunction("toLong", new ToLong());

		Table result = tableEnv.sqlQuery(
			"SELECT " +
				"  toUTF8(h.family2.col1), " +
				"  toLong(h.family2.col2) " +
				"FROM hTable AS h"
		);
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected =
			"Hello-1,100\n" +
			"Hello-2,200\n" +
			"Hello-3,300\n" +
			"null,400\n" +
			"Hello-5,500\n" +
			"Hello-6,600\n" +
			"Hello-7,700\n" +
			"null,800\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	/**
	 * A {@link ScalarFunction} that maps byte arrays to UTF-8 strings.
	 */
	public static class ToUTF8 extends ScalarFunction {

		public String eval(byte[] bytes) {
			return Bytes.toString(bytes);
		}
	}

	/**
	 * A {@link ScalarFunction} that maps byte array to longs.
	 */
	public static class ToLong extends ScalarFunction {

		public long eval(byte[] bytes) {
			return Bytes.toLong(bytes);
		}
	}

	// ######## TableInputFormat tests ############

	class InputFormatForTestTable extends TableInputFormat<Tuple1<Integer>> {

		@Override
		protected Scan getScanner() {
			return new Scan();
		}

		@Override
		protected String getTableName() {
			return TEST_TABLE;
		}

		@Override
		protected Tuple1<Integer> mapResultToTuple(Result r) {
			return new Tuple1<>(Bytes.toInt(r.getValue(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1))));
		}
	}

	@Test
	public void testTableInputFormat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Tuple1<Integer>> result = env
			.createInput(new InputFormatForTestTable())
			.reduce(new ReduceFunction<Tuple1<Integer>>(){

				@Override
				public Tuple1<Integer> reduce(Tuple1<Integer> v1, Tuple1<Integer> v2) throws Exception {
					return Tuple1.of(v1.f0 + v2.f0);
				}
			});

		List<Tuple1<Integer>> resultSet = result.collect();

		assertEquals(1, resultSet.size());
		assertEquals(360, (int) resultSet.get(0).f0);
	}

	/**
	 * Allows the tests to use {@link ExecutionEnvironment#getExecutionEnvironment()} but with a
	 * configuration that limits the maximum memory used for network buffers since the current
	 * defaults are too high for Travis-CI.
	 */
	private abstract static class LimitNetworkBuffersTestEnvironment extends ExecutionEnvironment {

		public static void setAsContext() {
			Configuration config = new Configuration();
			// the default network buffers size (10% of heap max =~ 150MB) seems to much for this test case
			config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(80L << 20)); // 80 MB
			final LocalEnvironment le = new LocalEnvironment(config);

			initializeContextEnvironment(new ExecutionEnvironmentFactory() {
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					return le;
				}
			});
		}

		public static void unsetAsContext() {
			resetContextEnvironment();
		}
	}

}
