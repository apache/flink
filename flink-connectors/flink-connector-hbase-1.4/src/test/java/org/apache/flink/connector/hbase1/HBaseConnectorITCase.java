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

package org.apache.flink.connector.hbase1;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase.util.PlannerType;
import org.apache.flink.connector.hbase1.source.AbstractTableInputFormat;
import org.apache.flink.connector.hbase1.source.HBaseInputFormat;
import org.apache.flink.connector.hbase1.source.HBaseRowDataInputFormat;
import org.apache.flink.connector.hbase1.source.HBaseRowInputFormat;
import org.apache.flink.connector.hbase1.source.HBaseTableSource;
import org.apache.flink.connector.hbase1.util.HBaseTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.hbase.util.PlannerType.OLD_PLANNER;
import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
	public void testTableSourceFullScan() {
		TableEnvironment tEnv = createBatchTableEnv();
		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			hbaseTable.setRowKey(ROW_KEY, Integer.class);
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

		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
	public void testTableSourceProjection() {
		TableEnvironment tEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
			hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
			hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
			hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
			hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
			hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
			hbaseTable.setRowKey(ROW_KEY, Integer.class);
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

		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
	public void testTableSourceFieldOrder() {
		TableEnvironment tEnv = createBatchTableEnv();

		if (isLegacyConnector) {
			HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
			// shuffle order of column registration
			hbaseTable.setRowKey(ROW_KEY, Integer.class);
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

		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
	public void testTableSourceWithTableAPI() throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
		tEnv.connect(new HBase()
			.version("1.4.3")
			.tableName(TEST_TABLE_1)
			.zookeeperQuorum(getZookeeperQuorum()))
			.withSchema(new Schema()
				.field("rowkey", DataTypes.INT())
				.field("family2", DataTypes.ROW(DataTypes.FIELD("col1", DataTypes.STRING()), DataTypes.FIELD("col2", DataTypes.BIGINT())))
				.field("family3", DataTypes.ROW(DataTypes.FIELD("col1", DataTypes.DOUBLE()), DataTypes.FIELD("col2", DataTypes.BOOLEAN()), DataTypes.FIELD("col3", DataTypes.STRING())))
				.field("family1", DataTypes.ROW(DataTypes.FIELD("col1", DataTypes.INT()))))
			.createTemporaryTable("hTable");
		Table table = tEnv.sqlQuery("SELECT * FROM hTable AS h");
		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
			hbaseTable.setRowKey(ROW_KEY, Integer.class);
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

		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
			.createInput(new InputFormatForTestTable(getConf()))
			.reduce((ReduceFunction<Tuple1<Integer>>) (v1, v2) -> Tuple1.of(v1.f0 + v2.f0));

		List<Tuple1<Integer>> resultSet = result.collect();

		assertEquals(1, resultSet.size());
		assertEquals(360, (int) resultSet.get(0).f0);
	}

	@Test
	public void testTableSink() throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		// register HBase table testTable1 which contains test data
		String table1DDL = createHBaseTableDDL(TEST_TABLE_1, false);
		tEnv.executeSql(table1DDL);

		String table2DDL = createHBaseTableDDL(TEST_TABLE_2, false);
		tEnv.executeSql(table2DDL);

		String query = "INSERT INTO " + TEST_TABLE_2 + " SELECT" +
			" rowkey," +
			" family1," +
			" family2," +
			" family3" +
			" FROM " + TEST_TABLE_1;

		tEnv.executeSql(query).await();

		// start a batch scan job to verify contents in HBase table
		TableEnvironment batchEnv = createBatchTableEnv();
		batchEnv.executeSql(table2DDL);

		Table table = batchEnv.sqlQuery(
			"SELECT " +
				"  h.rowkey, " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3 " +
				"FROM " + TEST_TABLE_2 + " AS h"
		);
		List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
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
		if (OLD_PLANNER.equals(planner) || isLegacyConnector) {
			// only test for blink planner and new connector, because types TIMESTAMP/DATE/TIME/DECIMAL works well in
			// new connector(using blink-planner), but exits some precision problem in old planner or legacy connector.
			return;
		}

		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		// register HBase table testTable1 which contains test data
		String table1DDL = createHBaseTableDDL(TEST_TABLE_1, true);
		tEnv.executeSql(table1DDL);

		// register HBase table which is empty
		String table3DDL = createHBaseTableDDL(TEST_TABLE_3, true);
		tEnv.executeSql(table3DDL);

		String insertStatement = "INSERT INTO " + TEST_TABLE_3 +
			" SELECT rowkey," +
			" family1," +
			" family2," +
			" family3," +
			" family4" +
			" from " + TEST_TABLE_1;
		tEnv.executeSql(insertStatement).await();

		// start a batch scan job to verify contents in HBase table
		TableEnvironment batchEnv = createBatchTableEnv();
		batchEnv.executeSql(table3DDL);
		String query = "SELECT " +
				"  h.rowkey, " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3, " +
				"  h.family4.col1, " +
				"  h.family4.col2, " +
				"  h.family4.col3, " +
				"  h.family4.col4 " +
				" FROM " + TEST_TABLE_3 + " AS h";
		Iterator<Row> collected = tEnv.executeSql(query).collect();
		List<String> result = CollectionUtil.iteratorToList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Hello-1,100,1.01,false,Welt-1,2019-08-18T19:00,2019-08-18,19:00,12345678.0001");
		expected.add("2,20,Hello-2,200,2.02,true,Welt-2,2019-08-18T19:01,2019-08-18,19:01,12345678.0002");
		expected.add("3,30,Hello-3,300,3.03,false,Welt-3,2019-08-18T19:02,2019-08-18,19:02,12345678.0003");
		expected.add("4,40,null,400,4.04,true,Welt-4,2019-08-18T19:03,2019-08-18,19:03,12345678.0004");
		expected.add("5,50,Hello-5,500,5.05,false,Welt-5,2019-08-19T19:10,2019-08-19,19:10,12345678.0005");
		expected.add("6,60,Hello-6,600,6.06,true,Welt-6,2019-08-19T19:20,2019-08-19,19:20,12345678.0006");
		expected.add("7,70,Hello-7,700,7.07,false,Welt-7,2019-08-19T19:30,2019-08-19,19:30,12345678.0007");
		expected.add("8,80,null,800,8.08,true,Welt-8,2019-08-19T19:40,2019-08-19,19:40,12345678.0008");
		assertEquals(expected, result);
	}

	@Test
	public void testHBaseLookupTableSource() {
		if (OLD_PLANNER.equals(planner) || isLegacyConnector) {
			// lookup table source is only supported in blink planner, skip for old planner
			// types TIMESTAMP/DATE/TIME/DECIMAL works well in new connector, skip legacy connector
			return;
		}

		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		tEnv.executeSql(
			"CREATE TABLE " + TEST_TABLE_1 + " (" +
				" family1 ROW<col1 INT>," +
				" family2 ROW<col1 STRING, col2 BIGINT>," +
				" family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 STRING>," +
				" rowkey INT," +
				" family4 ROW<col1 TIMESTAMP(3), col2 DATE, col3 TIME(3), col4 DECIMAL(12, 4)>," +
				" PRIMARY KEY (rowkey) NOT ENFORCED" +
				") WITH (" +
				" 'connector' = 'hbase-1.4'," +
				" 'table-name' = '" + TEST_TABLE_1 + "'," +
				" 'zookeeper.quorum' = '" + getZookeeperQuorum() + "'" +
				")");

		// prepare a source table
		String srcTableName = "src";
		DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(testTypeInfo);
		Table in = tEnv.fromDataStream(srcDs, $("a"), $("b"), $("c"), $("proc").proctime());
		tEnv.registerTable(srcTableName, in);

		// perform a temporal table join query
		String dimJoinQuery = "SELECT" +
			" a," +
			" b," +
			" h.family1.col1," +
			" h.family2.col1," +
			" h.family2.col2," +
			" h.family3.col1," +
			" h.family3.col2," +
			" h.family3.col3," +
			" h.family4.col1," +
			" h.family4.col2," +
			" h.family4.col3," +
			" h.family4.col4 " +
			" FROM src JOIN " + TEST_TABLE_1 + " FOR SYSTEM_TIME AS OF src.proc as h ON src.a = h.rowkey";
		Iterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
		List<String> result = CollectionUtil.iteratorToList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,10,Hello-1,100,1.01,false,Welt-1,2019-08-18T19:00,2019-08-18,19:00,12345678.0001");
		expected.add("2,2,20,Hello-2,200,2.02,true,Welt-2,2019-08-18T19:01,2019-08-18,19:01,12345678.0002");
		expected.add("3,2,30,Hello-3,300,3.03,false,Welt-3,2019-08-18T19:02,2019-08-18,19:02,12345678.0003");
		expected.add("3,3,30,Hello-3,300,3.03,false,Welt-3,2019-08-18T19:02,2019-08-18,19:02,12345678.0003");

		assertEquals(expected, result);
	}

	@Test
	public void testTableInputFormatOpenClose() throws IOException {
		HBaseTableSchema tableSchema = new HBaseTableSchema();
		tableSchema.addColumn(FAMILY1, F1COL1, byte[].class);
		AbstractTableInputFormat<?> inputFormat;
		if (isLegacyConnector) {
			inputFormat = new HBaseRowInputFormat(getConf(), TEST_TABLE_1, tableSchema);
		} else {
			inputFormat = new HBaseRowDataInputFormat(getConf(), TEST_TABLE_1, tableSchema, "null");
		}
		inputFormat.open(inputFormat.createInputSplits(1)[0]);
		assertNotNull(inputFormat.getConnection());
		assertNotNull(inputFormat.getConnection().getTable(TableName.valueOf(TEST_TABLE_1)));

		inputFormat.close();
		assertNull(inputFormat.getConnection());
	}

	// -------------------------------------------------------------------------------------
	// HBase lookup source tests
	// -------------------------------------------------------------------------------------

	// prepare a source collection.
	private static final List<Row> testData = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.LONG, Types.STRING},
		new String[]{"a", "b", "c"});

	static {
		testData.add(Row.of(1, 1L, "Hi"));
		testData.add(Row.of(2, 2L, "Hello"));
		testData.add(Row.of(3, 2L, "Hello world"));
		testData.add(Row.of(3, 3L, "Hello world!"));
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

		public InputFormatForTestTable(org.apache.hadoop.conf.Configuration hConf) {
			super(hConf);
		}

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

	private String createHBaseTableDDL(String tableName, boolean testTimeAndDecimalTypes) {
		StringBuilder family4Statement = new StringBuilder();
		if (testTimeAndDecimalTypes) {
			family4Statement.append(", family4 ROW<col1 TIMESTAMP(3)");
			family4Statement.append(", col2 DATE");
			family4Statement.append(", col3 TIME(3)");
			family4Statement.append(", col4 DECIMAL(12, 4)");
			family4Statement.append("> \n");
		}
		if (isLegacyConnector) {
			return "CREATE TABLE " + tableName + "(\n" +
				"	rowkey INT,\n" +
				"   family1 ROW<col1 INT>,\n" +
				"   family2 ROW<col1 VARCHAR, col2 BIGINT>,\n" +
				"   family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>" +
				family4Statement.toString() +
				") WITH (\n" +
				"   'connector.type' = 'hbase',\n" +
				"   'connector.version' = '1.4.3',\n" +
				"   'connector.table-name' = '" + tableName + "',\n" +
				"   'connector.zookeeper.quorum' = '" + getZookeeperQuorum() + "',\n" +
				"   'connector.zookeeper.znode.parent' = '/hbase' " +
				")";
		} else {
			return "CREATE TABLE " + tableName + "(\n" +
				"   rowkey INT," +
				"   family1 ROW<col1 INT>,\n" +
				"   family2 ROW<col1 VARCHAR, col2 BIGINT>,\n" +
				"   family3 ROW<col1 DOUBLE, col2 BOOLEAN, col3 VARCHAR>" +
				family4Statement.toString() +
				") WITH (\n" +
				"   'connector' = 'hbase-1.4',\n" +
				"   'table-name' = '" + tableName + "',\n" +
				"   'zookeeper.quorum' = '" + getZookeeperQuorum() + "',\n" +
				"   'zookeeper.znode.parent' = '/hbase' " +
				")";
		}
	}
}
