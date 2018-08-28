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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * IT case Test for {@link HBaseUpsertTableSink}.
 */
public class HBaseSinkITCase extends HBaseTestingClusterAutostarter {
	private static final long serialVersionUID = 1L;

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

	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.LONG, Types.DOUBLE, Types.BOOLEAN, Types.STRING},
		new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3"});

	static {
		testData1.add(Row.of(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1"));
		testData1.add(Row.of(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2"));
		testData1.add(Row.of(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3"));
		testData1.add(Row.of(4, 40, null, 400L, 4.04, true, "Welt-4"));
		testData1.add(Row.of(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5"));
		testData1.add(Row.of(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6"));
		testData1.add(Row.of(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7"));
		testData1.add(Row.of(8, 80, null, 800L, 8.08, true, "Welt-8"));
	}

	@BeforeClass
	public static void activateHBaseCluster() throws IOException {
		registerHBaseMiniClusterInClasspath();
		createHBaseTable();
	}

	private static void createHBaseTable() {
		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE);
		// column families
		byte[][] families = new byte[][]{Bytes.toBytes(FAMILY1), Bytes.toBytes(FAMILY2), Bytes.toBytes(FAMILY3)};
		// split keys
		byte[][] splitKeys = new byte[][]{Bytes.toBytes(4)};
		createTable(tableName, families, splitKeys);
	}

	@Test
	public void testTableSink() throws Exception {
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
		tableProperties.put("connector.table-name", TEST_TABLE);
		tableProperties.put("connector.zookeeper.quorum", getZookeeperQuorum());
		tableProperties.put("connector.zookeeper.znode.parent", "/hbase");
		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, schema.convertsToTableSchema());
		descriptorProperties.putProperties(tableProperties);
		TableSink tableSink = TableFactoryService
			.find(HBaseTableFactory.class, descriptorProperties.asMap())
			.createTableSink(descriptorProperties.asMap());

		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		execEnv.setParallelism(4);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv);
		StreamITCase.clear();

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);
		tEnv.registerTableSink("hbase", tableSink);

		String query = "INSERT INTO hbase SELECT ROW(f1c1), ROW(f2c1, f2c2), rowkey, ROW(f3c1, f3c2, f3c3) FROM src";
		tEnv.sqlUpdate(query);

		// wait to finish
		tEnv.execute("HBase Job");

		// start a batch scan job to verify contents in HBase table
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env, new TableConfig());

		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE);
		hbaseTable.setRowKey("rowkey", Integer.class);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tableEnv.registerTableSource("hTable", hbaseTable);

		Table result = tableEnv.sqlQuery(
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
		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();

		String expected =
				"1,10,Hello-1,100,1.01,false,Welt-1\n" +
				"2,20,Hello-2,200,2.02,true,Welt-2\n" +
				"3,30,Hello-3,300,3.03,false,Welt-3\n" +
				"4,40,,400,4.04,true,Welt-4\n" +
				"5,50,Hello-5,500,5.05,false,Welt-5\n" +
				"6,60,Hello-6,600,6.06,true,Welt-6\n" +
				"7,70,Hello-7,700,7.07,false,Welt-7\n" +
				"8,80,,800,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}
}
