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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.decorator.CachedLookupFunctionDecorator;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * IT case Test HBaseLookupFunction.
 */
public class HBaseLookupFunctionITCase extends HBaseTestingClusterAutostarter {
	private static final String ROWKEY = "rk";
	private static final String FAMILY1 = "family1";
	private static final String F1COL1 = "col1";

	private static final String FAMILY2 = "family2";
	private static final String F2COL1 = "col1";
	private static final String F2COL2 = "col2";

	private static final String FAMILY3 = "family3";
	private static final String F3COL1 = "col1";
	private static final String F3COL2 = "col2";
	private static final String F3COL3 = "col3";

	private static final String HTABLE_NAME = "testSrcHBaseTable1";

	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();

	static {
		testData1.add(Row.of(1, 1L, "Hi"));
		testData1.add(Row.of(2, 2L, "Hello"));
		testData1.add(Row.of(3, 2L, "Hello world"));
		testData1.add(Row.of(3, 3L, "Hello world!"));
	}

	private static final TypeInformation<?>[] testTypes1 = {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO};
	private static final String[] testColumns1 = {"a", "b", "c"};
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(testTypes1, testColumns1);

	@BeforeClass
	public static void activateHBaseCluster() throws IOException {
		registerHBaseMiniClusterInClasspath();
		prepareHBaseTableWithData();
	}

	private static void prepareHBaseTableWithData() throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(HTABLE_NAME);
		// column families
		byte[][] families = new byte[][]{Bytes.toBytes(FAMILY1), Bytes.toBytes(FAMILY2), Bytes.toBytes(FAMILY3)};
		// split keys
		byte[][] splitKeys = new byte[][]{Bytes.toBytes(4)};
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

	private static Map<String, String> hbaseTableProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE);
		properties.put(CONNECTOR_VERSION, CONNECTOR_VERSION_VALUE_143);
		properties.put(CONNECTOR_PROPERTY_VERSION, "1");
		properties.put(CONNECTOR_TABLE_NAME, HTABLE_NAME);
		// get zk quorum from "hbase-site.xml" in classpath
		String hbaseZk = HBaseConfiguration.create().get(HConstants.ZOOKEEPER_QUORUM);
		properties.put(CONNECTOR_ZK_QUORUM, hbaseZk);
		// schema
		String[] columnNames = {FAMILY1, ROWKEY, FAMILY2, FAMILY3};
		TypeInformation<Row> f1 = Types.ROW_NAMED(new String[]{F1COL1}, Types.INT);
		TypeInformation<Row> f2 = Types.ROW_NAMED(new String[]{F2COL1, F2COL2}, Types.STRING, Types.LONG);
		TypeInformation<Row> f3 = Types.ROW_NAMED(new String[]{F3COL1, F3COL2, F3COL3},
			Types.DOUBLE,
			Types.BOOLEAN,
			Types.STRING);
		TypeInformation[] columnTypes = new TypeInformation[]{f1, Types.INT, f2, f3};

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		TableSchema tableSchema = new TableSchema(columnNames, columnTypes);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(properties);
		return descriptorProperties.asMap();
	}

	@Test
	public void testHBaseLookupFunction() throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);
		StreamITCase.clear();

		// prepare a source table
		String srcTableName = "testStreamSrcTable1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		Table in = streamTableEnv.fromDataStream(ds, String.join(",", testColumns1));
		streamTableEnv.registerTable(srcTableName, in);

		Map<String, String> tableProperties = hbaseTableProperties();
		TableSource source = TableFactoryService.find(HBaseTableFactory.class, tableProperties).createTableSource(
			tableProperties);

		streamTableEnv.registerFunction("hbaseLookup",
			((HBaseTableSource) source).getLookupFunction(new String[]{ROWKEY}));

		// perform a temporal table join query
		String sqlQuery = "SELECT a,family1.col1, family3.col3 FROM testStreamSrcTable1, LATERAL TABLE(hbaseLookup(a))";
		Table result = streamTableEnv.sqlQuery(sqlQuery);

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

	@Test
	public void testHBaseLookupFunctionWithCache() throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);
		StreamITCase.clear();

		// prepare a source table
		String srcTableName = "testStreamSrcTable1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		Table in = streamTableEnv.fromDataStream(ds, String.join(",", testColumns1));
		streamTableEnv.registerTable(srcTableName, in);

		Map<String, String> tableProperties = hbaseTableProperties();
		TableSource source = TableFactoryService.find(HBaseTableFactory.class, tableProperties).createTableSource(
			tableProperties);

		TableFunction<Row> realTableFunction = ((HBaseTableSource) source).getLookupFunction(new String[]{ROWKEY});
		CachedLookupFunctionDecorator<Row> cachedLookup = new CachedLookupFunctionDecorator<>(realTableFunction,
			1024L * 1024 * 1024);

		streamTableEnv.registerFunction("hbaseLookup", cachedLookup);

		// perform a temporal table join query
		String sqlQuery = "SELECT a,family1.col1, family3.col3 FROM testStreamSrcTable1, LATERAL TABLE(hbaseLookup(a))";
		Table result = streamTableEnv.sqlQuery(sqlQuery);

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
}
