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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connectors.hbase.util.HBaseTestingClusterAutostarter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.MemoryTableSourceSinkUtil;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.DEFAULT_ROW_KEY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * Test HBase143TableFactory.
 * Set execution env parallelism to 4 for avoiding network memory limitation when running on multi-core servers.
 */
public class HBase143TableFactoryITCase extends HBaseTestingClusterAutostarter {

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
	private static List<Row> testData1 = new ArrayList<>();
	static {
		testData1.add(Row.of(1, 1L, "Hi"));
		testData1.add(Row.of(2, 2L, "Hello"));
		testData1.add(Row.of(3, 2L, "Hello world"));
		testData1.add(Row.of(3, 3L, "Hello world!"));
	}

	private static TypeInformation<?>[] testTypes1 = {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
	private static String[] testColumns1 = {"a", "b", "c"};
	private static RowTypeInfo testTypeInfo1 = new RowTypeInfo(testTypes1, testColumns1);

	@BeforeClass
	public static void activateHBaseCluster() throws IOException {
		registerHBaseMiniClusterInClasspath();
	}

	@AfterClass
	public static void resetExecutionEnvironmentFactory() {
		LimitNetworkBuffersTestEnvironment.unsetAsContext();
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
			config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 80L << 20); // 80 MB
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

	private void prepareHBaseTableWithData(String hTableName) throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(hTableName);
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

	private void prepareEmptyHBaseTable(String sinkTableName, int splitKeyLength) throws IOException {

		// create a empty table for sink testing
		TableName tableName = TableName.valueOf(sinkTableName);
		// column families
		byte[][] sinkFamilies = new byte[][]{
				Bytes.toBytes(FAMILY1),
				Bytes.toBytes(FAMILY2)
		};

		// split keys
		createTable(tableName, sinkFamilies, createSplitKeys(splitKeyLength));
	}

	private byte[][] createSplitKeys(int splitLength) {
		return new byte[][]{Bytes.toBytes(splitLength)};
	}

	private Put putRow(int rowKey, int f1c1, String f2c1, long f2c2, double f3c1, boolean f3c2, String f3c3) {
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

	private Map<String, String> tableServiceLookupConf() {
		Map<String, String> tableServiceLookupConf = new HashMap<>();
		tableServiceLookupConf.put(ConnectorDescriptorValidator.CONNECTOR_TYPE.toLowerCase(), "HBASE");
		tableServiceLookupConf.put(CONNECTOR_VERSION.toLowerCase(), CONNECTOR_VERSION_VALUE_143);
		tableServiceLookupConf.put(CONNECTOR_PROPERTY_VERSION.toLowerCase(), "1");
		return tableServiceLookupConf;
	}

	private TableSink prepareHBaseTableSink(String hTableName, String[] columnNames, InternalType[] columnTypes) {
		// simulate a table schema from sql-client
		RichTableSchema tableSchema = new RichTableSchema(columnNames, columnTypes);
		tableSchema.setPrimaryKey(DEFAULT_ROW_KEY);

		Map<String, String> tableServiceLookupConf = tableServiceLookupConf();

		TableProperties properties = new TableProperties();
		properties.putTableNameIntoProperties(hTableName);
		properties.putSchemaIntoProperties(tableSchema);
		properties.putProperties(tableServiceLookupConf);

		Map<String, String> sinkConfigMap = new HashMap<>();
		sinkConfigMap.putAll(properties.toKeyLowerCase().toMap());
		sinkConfigMap.put(CONNECTOR_HBASE_TABLE_NAME.toLowerCase(), hTableName);

		return TableFactoryService.find(HBase143TableFactory.class, tableServiceLookupConf).createTableSink(sinkConfigMap);
	}

	@Test
	public void testHBaseStreamSink() throws IOException {
		// prepare an empty HBase table for sink
		String hTableName = "testHBaseTable1";
		prepareEmptyHBaseTable(hTableName, 4);

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);

		// prepare a source table
		String srcTableName = "testSrcTable1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		Table in = streamTableEnv.fromDataStream(ds, String.join(",", testColumns1));
		streamTableEnv.registerTable(srcTableName, in);

		// simulate a table schema from sql-client
		// create table TEST_SINK_TABLE (
		//      ROWKEY integer,
		//      `family1.cnt` bigint,
		//      `family1.aggStr` string,
		//      PRIMARY KEY(ROWKEY)
		// )
		String cf = "family1";
		String qualifier1 = "cnt";
		String qualifier2 = "aggStr";
		String[] columnNames = {DEFAULT_ROW_KEY, cf + "." + qualifier1, cf + "." + qualifier2};
		InternalType[] columnTypes = {DataTypes.INT, DataTypes.LONG, DataTypes.STRING};

		TableSink sink = prepareHBaseTableSink(hTableName, columnNames, columnTypes);

		Table result = streamTableEnv.sqlQuery("select a, max(b), concat_agg('#', c) from " + srcTableName + " group by a");
		result.writeToSink(sink);
		streamTableEnv.execute();

		// verify result data in HBase
		// expect wrote to sink table: 'family1:cnt', 'cf1:aggStr'
		byte[] cfBytes = cf.getBytes();
		byte[] q1 = qualifier1.getBytes();
		byte[] q2 = qualifier2.getBytes();
		HTable table = openTable(TableName.valueOf(hTableName));
		Get get = new Get(Bytes.toBytes(3));
		get.addColumn(cfBytes, q1);
		get.addColumn(cfBytes, q2);

		Result res = table.get(get);
		Assert.assertEquals(3, Bytes.toLong(res.getValue(cfBytes, q1)));
		Assert.assertEquals("Hello world#Hello world!", Bytes.toString(res.getValue(cfBytes, q2)));

		table.close();
	}

	@Test
	public void testHBaseBoundedStreamSink() throws IOException {
		// prepare an empty HBase table for sink
		String hTableName = "testHBaseSinkTableBounded1";
		prepareEmptyHBaseTable(hTableName, 4);

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		BatchTableEnvironment batchTableEnv = TableEnvironment.getBatchTableEnvironment(streamEnv);
		// prepare a source table
		String srcTableName = "testSrcTableBounded1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		batchTableEnv.registerBoundedStream(srcTableName, ds,  String.join(",", testColumns1));

		// simulate a table schema from sql-client
		// create table TEST_SINK_TABLE (
		//      ROWKEY integer,
		//      `family1.cnt` bigint,
		//      `family1.aggStr` string,
		//      PRIMARY KEY(ROWKEY)
		// )
		String cf = "family1";
		String qualifier1 = "cnt";
		String qualifier2 = "aggStr";
		String[] columnNames = {DEFAULT_ROW_KEY, cf + "." + qualifier1, cf + "." + qualifier2};
		InternalType[] columnTypes = {DataTypes.INT, DataTypes.LONG, DataTypes.STRING};

		TableSink sink = prepareHBaseTableSink(hTableName, columnNames, columnTypes);

		Table result = batchTableEnv.sqlQuery("select a, max(b), concat_agg('#', c) from " + srcTableName + " group by a");
		result.writeToSink(sink);
		batchTableEnv.execute();

		// verify wrote data in HBase
		// expect write to sink table: 'family1:cnt', 'cf1:aggStr'
		byte[] cfBytes = cf.getBytes();
		byte[] q1 = qualifier1.getBytes();
		byte[] q2 = qualifier2.getBytes();
		HTable table = openTable(TableName.valueOf(hTableName));
		Get get = new Get(Bytes.toBytes(3));
		get.addColumn(cfBytes, q1);
		get.addColumn(cfBytes, q2);

		Result res = table.get(get);
		Assert.assertEquals(3, Bytes.toLong(res.getValue(cfBytes, q1)));
		Assert.assertEquals("Hello world#Hello world!", Bytes.toString(res.getValue(cfBytes, q2)));

		table.close();
	}

	@Test
	public void testHBaseStreamSource() throws IOException {
		// prepare a HBase table with data for lookup via rowKey.
		String hTableName = "testSrcHBaseTable1";
		prepareHBaseTableWithData(hTableName);

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);

		// prepare a source table
		String srcTableName = "testStreamSrcTable1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		Table in = streamTableEnv.fromDataStream(ds, String.join(",", testColumns1));
		streamTableEnv.registerTable(srcTableName, in);

		// simulate a table schema from sql-client for joining.
		// create table TEST_SINK_TABLE (
		//      ROWKEY bigint,
		//      `family1.col1` ,
		//      `family2.col1` ,
		//      `family2.col2` ,
		//      `family3.col1` ,
		//      `family3.col2` ,
		//      `family3.col3` ,
		//      PRIMARY KEY(ROWKEY)
		// )
		String[] columnNames = {
			DEFAULT_ROW_KEY,
			FAMILY1 + "." + F1COL1,
			FAMILY2 + "." + F2COL1,
			FAMILY2 + "." + F2COL2,
			FAMILY3 + "." + F3COL1,
			FAMILY3 + "." + F3COL2,
			FAMILY3 + "." + F3COL3};
		InternalType[] columnTypes = {DataTypes.INT, DataTypes.INT, DataTypes.STRING, DataTypes.LONG, DataTypes.DOUBLE, DataTypes.BOOLEAN, DataTypes.STRING};
		RichTableSchema tableSchema = new RichTableSchema(columnNames, columnTypes);
		tableSchema.setPrimaryKey(DEFAULT_ROW_KEY);

		Map<String, String> tableServiceLookupConf = tableServiceLookupConf();
		TableProperties properties = new TableProperties();

		properties.putTableNameIntoProperties(hTableName);
		properties.putSchemaIntoProperties(tableSchema);
		properties.putProperties(tableServiceLookupConf);

		Map<String, String> sinkConfigMap = new HashMap<>();
		sinkConfigMap.putAll(properties.toKeyLowerCase().toMap());
		sinkConfigMap.put(CONNECTOR_HBASE_TABLE_NAME.toLowerCase(), hTableName);

		TableSource source = TableFactoryService.find(HBase143TableFactory.class, tableServiceLookupConf).createTableSource(sinkConfigMap, false);
		String dimTableName = "dimHBaseTable1";
		streamTableEnv.registerTableSource(dimTableName, source);

		// perform a temporal table join query
		Table result = streamTableEnv.sqlQuery(
			"SELECT a, `family1.col1` as c1, `family3.col3` as c3 " +
				" FROM " + srcTableName + " s"
				+ " JOIN " + dimTableName + " FOR SYSTEM_TIME AS OF PROCTIME() AS T ON s.a = T.ROWKEY");

		MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink();
		result.writeToSink(sink);
		streamTableEnv.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Welt-1");
		expected.add("2,20,Welt-2");
		expected.add("3,30,Welt-3");
		expected.add("3,30,Welt-3");

		List<String> results = scala.collection.JavaConversions.mutableSeqAsJavaList(MemoryTableSourceSinkUtil.results());
		Collections.sort(expected);
		Collections.sort(results);
		Assert.assertEquals(expected, results);
	}
}
