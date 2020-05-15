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

package org.apache.flink.connector.hbase;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.source.HBaseInputFormat;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase.util.HBaseTestBase;
import org.apache.flink.connector.hbase.util.PlannerType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;
import org.apache.flink.table.planner.sinks.CollectRowTableSink;
import org.apache.flink.table.planner.sinks.CollectTableSink;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

import static org.apache.flink.connector.hbase.util.PlannerType.OLD_PLANNER;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.junit.Assert.assertEquals;

/**
 * IT cases for HBase connector (including HBaseTableSource and HBaseTableSink).
 */
@RunWith(Parameterized.class)
public class HBaseConnectorITCase extends HBaseTestBase {

	@Parameterized.Parameter
	public PlannerType planner;

	@Parameterized.Parameter(1)
	public boolean isLegacyConnector;

	@Override
	protected PlannerType planner() {
		return planner;
	}

	@Parameterized.Parameters(name = "planner = {0}, legacy = {1}")
	public static Object[] parameters() {
		return new Object[][]{
			new Object[]{PlannerType.BLINK_PLANNER, true},
			new Object[]{PlannerType.BLINK_PLANNER, false},
			new Object[]{PlannerType.OLD_PLANNER, true}
		};
	}

	// -------------------------------------------------------------------------------------
	// HBaseTableSource tests
	// -------------------------------------------------------------------------------------

	@Test
	public void testTableSourceFullScan() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();
		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			hbaseTable.setRowKey("rowkey", Integer.class);
			((TableEnvironmentInternal) tEnv).registerTableSourceInternal("hTable", hbaseTable);
		} else {
			tEnv.executeSql(
					"CREATE TABLE hTable (" +
					" family1 ROW<col1 INT>," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>," +
					" rowkey INT," +
					" PRIMARY KEY (rowkey) NOT ENFORCED" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}

		Table table = tEnv.sqlQuery("SELECT " +
			"  h.family1.col1, " +
			"  h.family2.col1, " +
			"  h.family2.col2, " +
			"  h.family3.col1, " +
			"  h.family3.col2, " +
			"  h.family3.col3 " +
			"FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
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
		TableEnvironment tEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			hbaseTable.setRowKey("rowkey", Integer.class);
			((TableEnvironmentInternal) tEnv).registerTableSourceInternal("hTable", hbaseTable);
		} else {
			tEnv.executeSql(
					"CREATE TABLE hTable (" +
					" family1 ROW<col1 INT>," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>," +
					" rowkey INT," +
					" PRIMARY KEY (rowkey) NOT ENFORCED" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}

		Table table = tEnv.sqlQuery("SELECT " +
			"  h.family1.col1, " +
			"  h.family3.col1, " +
			"  h.family3.col2, " +
			"  h.family3.col3 " +
			"FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
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
		TableEnvironment tEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			// shuffle order of column registration
			hbaseTable.setRowKey("rowkey", Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			((TableEnvironmentInternal) tEnv).registerTableSourceInternal("hTable", hbaseTable);
		} else {
			tEnv.executeSql(
					"CREATE TABLE hTable (" +
					" rowkey INT PRIMARY KEY," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>," +
					" family1 ROW<col1 INT>" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}

		Table table = tEnv.sqlQuery("SELECT * FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
		String expected =
			"1,Hello-1,100,1.01,false,Welt-1,10\n" +
				"2,Hello-2,200,2.02,true,Welt-2,20\n" +
				"3,Hello-3,300,3.03,false,Welt-3,30\n" +
				"4,null,400,4.04,true,Welt-4,40\n" +
				"5,Hello-5,500,5.05,false,Welt-5,50\n" +
				"6,Hello-6,600,6.06,true,Welt-6,60\n" +
				"7,Hello-7,700,7.07,false,Welt-7,70\n" +
				"8,null,800,8.08,true,Welt-8,80\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceReadAsByteArray() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			// fetch row2 from the table till the end
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			hbaseTable.addColumn(FAMILY2, F2COL1, byte[].class);
			hbaseTable.addColumn(FAMILY2, F2COL2, byte[].class);
			hbaseTable.setRowKey("rowkey", Integer.class);
			((TableEnvironmentInternal) tEnv).registerTableSourceInternal("hTable", hbaseTable);
		} else {
			tEnv.executeSql(
					"CREATE TABLE hTable (" +
					" family2 ROW<col1 BYTES, col2 BYTES>," +
					" rowkey INT" + // no primary key syntax
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}
		tEnv.registerFunction("toUTF8", new ToUTF8());
		tEnv.registerFunction("toLong", new ToLong());

		Table table = tEnv.sqlQuery(
			"SELECT " +
				"  toUTF8(h.family2.col1), " +
				"  toLong(h.family2.col2) " +
				"FROM hTable AS h"
		);

		List<Row> results = collectBatchResult(table);
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

	@Test
	public void testTableInputFormat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple1<Integer>> result = env
			.createInput(new InputFormatForTestTable())
			.reduce((ReduceFunction<Tuple1<Integer>>) (v1, v2) -> Tuple1.of(v1.f0 + v2.f0));

		List<Tuple1<Integer>> resultSet = result.collect();

		assertEquals(1, resultSet.size());
		assertEquals(360, (int) resultSet.get(0).f0);
	}

	// -------------------------------------------------------------------------------------
	// HBaseTableSink tests
	// -------------------------------------------------------------------------------------

	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.LONG, Types.DOUBLE,
			Types.BOOLEAN, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_DATE, Types.SQL_TIME},
		new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3", "f4c1", "f4c2", "f4c3"});

	static {
		testData1.add(Row.of(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1",
			Timestamp.valueOf("2019-08-18 19:00:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:00:00")));
		testData1.add(Row.of(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2",
			Timestamp.valueOf("2019-08-18 19:01:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:01:00")));
		testData1.add(Row.of(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3",
			Timestamp.valueOf("2019-08-18 19:02:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:02:00")));
		testData1.add(Row.of(4, 40, null, 400L, 4.04, true, "Welt-4",
			Timestamp.valueOf("2019-08-18 19:03:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:03:00")));
		testData1.add(Row.of(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5",
			Timestamp.valueOf("2019-08-19 19:10:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:10:00")));
		testData1.add(Row.of(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6",
			Timestamp.valueOf("2019-08-19 19:20:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:20:00")));
		testData1.add(Row.of(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7",
			Timestamp.valueOf("2019-08-19 19:30:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:30:00")));
		testData1.add(Row.of(8, 80, null, 800L, 8.08, true, "Welt-8",
			Timestamp.valueOf("2019-08-19 19:40:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:40:00")));
	}

	@Test
	public void testTableSink() throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		if (isLegacyConnector) {
			HBaseTableSchema schema = new HBaseTableSchema();
			schema.addColumn(FAMILY1, F1COL1, Integer.class);
			schema.addColumn(FAMILY2, F2COL1, String.class);
			schema.addColumn(FAMILY2, F2COL2, Long.class);
			schema.setRowKey("rk", Integer.class);
			schema.addColumn(FAMILY3, F3COL1, Double.class);
			schema.addColumn(FAMILY3, F3COL2, Boolean.class);
			schema.addColumn(FAMILY3, F3COL3, String.class);

			Map<String, String> tableProperties = new HashMap<>();
			tableProperties.put("connector.type", "hbase");
			tableProperties.put("connector.version", "1.4.3");
			tableProperties.put("connector.property-version", "1");
			tableProperties.put("connector.table-name", TEST_TABLE_2);
			tableProperties.put("connector.zookeeper.quorum", getZookeeperQuorum());
			tableProperties.put("connector.zookeeper.znode.parent", "/hbase");
			DescriptorProperties descriptorProperties = new DescriptorProperties(true);
			descriptorProperties.putTableSchema(SCHEMA, schema.convertsToTableSchema());
			descriptorProperties.putProperties(tableProperties);
			TableSink tableSink = TableFactoryService
				.find(HBaseTableFactory.class, descriptorProperties.asMap())
				.createTableSink(descriptorProperties.asMap());
			((TableEnvironmentInternal) tEnv).registerTableSinkInternal("hbase", tableSink);
		} else {
			tEnv.executeSql(
					"CREATE TABLE hbase (" +
					" family1 ROW<col1 INT>," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" rk INT," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'," +
					" 'zookeeper.znode-parent' = '/hbase'" +
					")");
		}

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.createTemporaryView("src", ds);

		String query = "INSERT INTO hbase SELECT ROW(f1c1), ROW(f2c1, f2c2), rowkey, ROW(f3c1, f3c2, f3c3) FROM src";
		TableEnvUtil.execInsertSqlAndWaitResult(tEnv, query);

		// start a batch scan job to verify contents in HBase table
		// start a batch scan job to verify contents in HBase table
		TableEnvironment batchTableEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_2);
			hbaseTable.setRowKey("rowkey", Integer.class);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			((TableEnvironmentInternal) batchTableEnv).registerTableSourceInternal("hTable", hbaseTable);
		} else {
			batchTableEnv.executeSql(
					"CREATE TABLE hTable (" +
					" rowkey INT," +
					" family1 ROW<col1 INT>," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}

		Table table = batchTableEnv.sqlQuery(
			"SELECT " +
				"  h.rowkey, " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3 " +
				"FROM hTable AS h"
		);

		List<Row> results = collectBatchResult(table);
		String expected =
				"1,10,Hello-1,100,1.01,false,Welt-1\n" +
				"2,20,Hello-2,200,2.02,true,Welt-2\n" +
				"3,30,Hello-3,300,3.03,false,Welt-3\n" +
				"4,40,null,400,4.04,true,Welt-4\n" +
				"5,50,Hello-5,500,5.05,false,Welt-5\n" +
				"6,60,Hello-6,600,6.06,true,Welt-6\n" +
				"7,70,Hello-7,700,7.07,false,Welt-7\n" +
				"8,80,null,800,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceSinkWithDDL() throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.createTemporaryView("src", ds);

		// register hbase table
		String quorum = getZookeeperQuorum();
		String ddl;
		if (isLegacyConnector) {
			ddl = "CREATE TABLE hbase (\n" +
				"    rowkey INT," +
				"    family1 ROW<col1 INT>,\n" +
				"    family2 ROW<col1 VARCHAR, col2 BIGINT>,\n" +
				"    family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>,\n" +
				"    family4 ROW<col1 TIMESTAMP(3), col2 DATE, col3 TIME(3)>\n" +
				") WITH (\n" +
				"    'connector.type' = 'hbase',\n" +
				"    'connector.version' = '1.4.3',\n" +
				"    'connector.table-name' = 'testTable3',\n" +
				"    'connector.zookeeper.quorum' = '" + quorum + "',\n" +
				"    'connector.zookeeper.znode.parent' = '/hbase' " +
				")";
		} else {
			ddl = "CREATE TABLE hbase (\n" +
				"    rowkey INT," +
				"    family1 ROW<col1 INT>,\n" +
				"    family2 ROW<col1 VARCHAR, col2 BIGINT>,\n" +
				"    family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>,\n" +
				"    family4 ROW<col1 TIMESTAMP(3), col2 DATE, col3 TIME(3)>\n" +
				") WITH (\n" +
				"    'connector' = 'hbase-1.4',\n" +
				"    'table-name' = 'testTable3',\n" +
				"    'zookeeper.quorum' = '" + quorum + "',\n" +
				"    'zookeeper.znode-parent' = '/hbase' " +
				")";
		}
		tEnv.executeSql(ddl);

		String query = "INSERT INTO hbase " +
			"SELECT rowkey, ROW(f1c1), ROW(f2c1, f2c2), ROW(f3c1, f3c2, f3c3), ROW(f4c1, f4c2, f4c3) " +
			"FROM src";
		TableEnvUtil.execInsertSqlAndWaitResult(tEnv, query);

		// start a batch scan job to verify contents in HBase table
		TableEnvironment batchTableEnv = createBatchTableEnv();
		batchTableEnv.executeSql(ddl);

		Table table = batchTableEnv.sqlQuery(
			"SELECT " +
				"  h.rowkey, " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3, " +
				"  h.family4.col1, " +
				"  h.family4.col2, " +
				"  h.family4.col3 " +
				"FROM hbase AS h"
		);

		List<Row> results = collectBatchResult(table);
		String expected =
				"1,10,Hello-1,100,1.01,false,Welt-1,2019-08-18 19:00:00.0,2019-08-18,19:00:00\n" +
				"2,20,Hello-2,200,2.02,true,Welt-2,2019-08-18 19:01:00.0,2019-08-18,19:01:00\n" +
				"3,30,Hello-3,300,3.03,false,Welt-3,2019-08-18 19:02:00.0,2019-08-18,19:02:00\n" +
				"4,40,null,400,4.04,true,Welt-4,2019-08-18 19:03:00.0,2019-08-18,19:03:00\n" +
				"5,50,Hello-5,500,5.05,false,Welt-5,2019-08-19 19:10:00.0,2019-08-19,19:10:00\n" +
				"6,60,Hello-6,600,6.06,true,Welt-6,2019-08-19 19:20:00.0,2019-08-19,19:20:00\n" +
				"7,70,Hello-7,700,7.07,false,Welt-7,2019-08-19 19:30:00.0,2019-08-19,19:30:00\n" +
				"8,80,null,800,8.08,true,Welt-8,2019-08-19 19:40:00.0,2019-08-19,19:40:00\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}


	// -------------------------------------------------------------------------------------
	// HBase lookup source tests
	// -------------------------------------------------------------------------------------

	// prepare a source collection.
	private static final List<Row> testData2 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo2 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.LONG, Types.STRING},
		new String[]{"a", "b", "c"});

	static {
		testData2.add(Row.of(1, 1L, "Hi"));
		testData2.add(Row.of(2, 2L, "Hello"));
		testData2.add(Row.of(3, 2L, "Hello world"));
		testData2.add(Row.of(3, 3L, "Hello world!"));
	}

	@Test
	public void testHBaseLookupTableSource() throws Exception {
		if (OLD_PLANNER.equals(planner)) {
			// lookup table source is only supported in blink planner, skip for old planner
			return;
		}
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, streamSettings);
		StreamITCase.clear();

		// prepare a source table
		String srcTableName = "src";
		DataStream<Row> ds = streamEnv.fromCollection(testData2).returns(testTypeInfo2);
		Table in = streamTableEnv.fromDataStream(ds, $("a"), $("b"), $("c"), $("proc").proctime());
		streamTableEnv.registerTable(srcTableName, in);

		if (isLegacyConnector) {
			Map<String, String> tableProperties = hbaseTableProperties();
			TableSource<?> source = TableFactoryService
				.find(HBaseTableFactory.class, tableProperties)
				.createTableSource(tableProperties);
			((TableEnvironmentInternal) streamTableEnv).registerTableSourceInternal("hbaseLookup", source);
		} else {
			streamTableEnv.executeSql(
					"CREATE TABLE hbaseLookup (" +
					" family1 ROW<col1 INT>," +
					" rk INT," +
					" family2 ROW<col1 STRING, col2 BIGINT>," +
					" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>" +
					") WITH (" +
					" 'connector' = 'hbase-1.4'," +
					" 'table-name' = '" + TEST_TABLE_1 + "'," +
					" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
					")");
		}
		// perform a temporal table join query
		String query = "SELECT a,family1.col1, family3.col3 FROM src " +
			"JOIN hbaseLookup FOR SYSTEM_TIME AS OF src.proc as h ON src.a = h.rk";
		Table result = streamTableEnv.sqlQuery(query);

		DataStream<Row> resultSet = streamTableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());

		streamEnv.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Welt-1");
		expected.add("2,20,Welt-2");
		expected.add("3,30,Welt-3");
		expected.add("3,30,Welt-3");

		StreamITCase.compareWithList(expected);
	}

	private static Map<String, String> hbaseTableProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE);
		properties.put(CONNECTOR_VERSION, CONNECTOR_VERSION_VALUE_143);
		properties.put(CONNECTOR_PROPERTY_VERSION, "1");
		properties.put(CONNECTOR_TABLE_NAME, TEST_TABLE_1);
		// get zk quorum from "hbase-site.xml" in classpath
		String hbaseZk = HBaseConfiguration.create().get(HConstants.ZOOKEEPER_QUORUM);
		properties.put(CONNECTOR_ZK_QUORUM, hbaseZk);
		// schema
		String[] columnNames = {FAMILY1, ROWKEY, FAMILY2, FAMILY3};
		TypeInformation<Row> f1 = Types.ROW_NAMED(new String[]{F1COL1}, Types.INT);
		TypeInformation<Row> f2 = Types.ROW_NAMED(new String[]{F2COL1, F2COL2}, Types.STRING, Types.LONG);
		TypeInformation<Row> f3 = Types.ROW_NAMED(new String[]{F3COL1, F3COL2, F3COL3}, Types.DOUBLE, Types.BOOLEAN, Types.STRING);
		TypeInformation[] columnTypes = new TypeInformation[]{f1, Types.INT, f2, f3};

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		TableSchema tableSchema = new TableSchema(columnNames, columnTypes);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(properties);
		return descriptorProperties.asMap();
	}

	// ------------------------------- Utilities -------------------------------------------------

	/**
	 * Creates a Batch {@link TableEnvironment} depends on the {@link #planner} context.
	 */
	private TableEnvironment createBatchTableEnv() {
		if (OLD_PLANNER.equals(planner)) {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			return BatchTableEnvironment.create(env, new TableConfig());
		} else {
			return TableEnvironment.create(batchSettings);
		}
	}

	/**
	 * Collects batch result depends on the {@link #planner} context.
	 */
	private List<Row> collectBatchResult(Table table) throws Exception {
		TableImpl tableImpl = (TableImpl) table;
		if (OLD_PLANNER.equals(planner)) {
			BatchTableEnvironment batchTableEnv = (BatchTableEnvironment) tableImpl.getTableEnvironment();
			DataSet<Row> resultSet = batchTableEnv.toDataSet(table, Row.class);
			return resultSet.collect();
		} else {
			TableImpl t = (TableImpl) table;
			TableSchema schema = t.getSchema();
			List<TypeInformation> types = new ArrayList<>();
			for (TypeInformation typeInfo : t.getSchema().getFieldTypes()) {
				// convert LOCAL_DATE_TIME to legacy TIMESTAMP to make the output consistent with flink batch planner
				if (typeInfo.equals(Types.LOCAL_DATE_TIME)) {
					types.add(Types.SQL_TIMESTAMP);
				} else if (typeInfo.equals(Types.LOCAL_DATE)) {
					types.add(Types.SQL_DATE);
				} else if (typeInfo.equals(Types.LOCAL_TIME)) {
					types.add(Types.SQL_TIME);
				} else {
					types.add(typeInfo);
				}
			}
			CollectRowTableSink sink = new CollectRowTableSink();
			CollectTableSink<Row> configuredSink = (CollectTableSink<Row>) sink.configure(
				schema.getFieldNames(), types.toArray(new TypeInformation[0]));
			return JavaScalaConversionUtil.toJava(
				BatchTableEnvUtil.collect(
					t.getTableEnvironment(), table, configuredSink, Option.apply("JOB")));
		}
	}

	/**
	 * A {@link ScalarFunction} that maps byte arrays to UTF-8 strings.
	 */
	public static class ToUTF8 extends ScalarFunction {
		private static final long serialVersionUID = 1L;

		public String eval(byte[] bytes) {
			return Bytes.toString(bytes);
		}
	}

	/**
	 * A {@link ScalarFunction} that maps byte array to longs.
	 */
	public static class ToLong extends ScalarFunction {
		private static final long serialVersionUID = 1L;

		public long eval(byte[] bytes) {
			return Bytes.toLong(bytes);
		}
	}

	/**
	 * A {@link HBaseInputFormat} for testing.
	 */
	public static class InputFormatForTestTable extends HBaseInputFormat<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		protected Scan getScanner() {
			return new Scan();
		}

		@Override
		protected String getTableName() {
			return TEST_TABLE_1;
		}

		@Override
		protected Tuple1<Integer> mapResultToTuple(Result r) {
			return new Tuple1<>(Bytes.toInt(r.getValue(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1))));
		}
	}

}
