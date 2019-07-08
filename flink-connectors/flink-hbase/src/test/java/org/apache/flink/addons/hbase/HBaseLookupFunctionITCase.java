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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.addons.hbase.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.addons.hbase.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.addons.hbase.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * IT case Test HBaseLookupFunction.
 */
public class HBaseLookupFunctionITCase extends HBaseTestingClusterAutostarter {
	private static final Log LOG = LogFactory.getLog(HBaseLookupFunctionITCase.class);
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
	public static void activateHBaseCluster() {
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
			final LocalEnvironment le = new LocalEnvironment(config);

			initializeContextEnvironment(new ExecutionEnvironmentFactory() {
				@Override
				public ExecutionEnvironment createExecutionEnvironment() {
					return le;
				}
			});
		}

		static void unsetAsContext() {
			resetContextEnvironment();
		}
	}

	private void prepareHBaseTableWithData(String hTableName) throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(hTableName);
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
		tableServiceLookupConf.put(ConnectorDescriptorValidator.CONNECTOR_TYPE.toLowerCase(),
			CONNECTOR_TYPE_VALUE_HBASE);
		tableServiceLookupConf.put(CONNECTOR_VERSION.toLowerCase(), CONNECTOR_VERSION_VALUE_143);
		tableServiceLookupConf.put(CONNECTOR_PROPERTY_VERSION.toLowerCase(), "1");
		return tableServiceLookupConf;
	}

	@Test
	public void testHBaseStreamSourceLookupWithNestedSchema() throws IOException {
		// prepare a HBase table with data for lookup via rowKey.
		String hTableName = "testSrcHBaseTable1";
		prepareHBaseTableWithData(hTableName);

		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(4);
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

		// prepare a source table
		String srcTableName = "testStreamSrcTable1";
		DataStream<Row> ds = streamEnv.fromCollection(testData1).returns(testTypeInfo1);
		Table in = streamTableEnv.fromDataStream(ds, String.join(",", testColumns1));
		streamTableEnv.registerTable(srcTableName, in);

		String[] columnNames = {"rowkey", FAMILY1, FAMILY2, FAMILY3};

		RowTypeInfo f1 = new RowTypeInfo(new TypeInformation[]{TypeConversions.fromDataTypeToLegacyInfo(DataTypes.INT())},
			new String[]{F1COL1});
		RowTypeInfo f2 = new RowTypeInfo(new TypeInformation[]{TypeConversions.fromDataTypeToLegacyInfo(DataTypes.STRING()), TypeConversions.fromDataTypeToLegacyInfo(
			DataTypes.BIGINT())}, new String[]{F2COL1, F2COL2});
		RowTypeInfo f3 = new RowTypeInfo(new TypeInformation[]{TypeConversions.fromDataTypeToLegacyInfo(DataTypes.DOUBLE()), TypeConversions.fromDataTypeToLegacyInfo(
			DataTypes.BOOLEAN()), TypeConversions.fromDataTypeToLegacyInfo(DataTypes.STRING())},
			new String[]{F3COL1, F3COL2, F3COL3});
		DataType[] columnTypes = {DataTypes.INT(), TypeConversions.fromLegacyInfoToDataType(f1), TypeConversions.fromLegacyInfoToDataType(
			f2), TypeConversions.fromLegacyInfoToDataType(f3)};

		TableSchema.Builder builder = new TableSchema.Builder();
		TableSchema tableSchema = builder.fields(columnNames, columnTypes).build();

		Map<String, String> tableServiceLookupConf = tableServiceLookupConf();
		tableServiceLookupConf.put(CONNECTOR_HBASE_TABLE_NAME.toLowerCase(), hTableName);

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(tableServiceLookupConf);

		TableSource source = TableFactoryService.find(HBaseTableFactory.class,
			tableServiceLookupConf).createTableSource(descriptorProperties.asMap());
		streamTableEnv.registerFunction("hbaseLookup", ((HBaseTableSource) source).getLookupFunction(new String[]{""}));

		// perform a temporal table join query
		String sqlQuery = "SELECT a,family1.col1, family3.col3 FROM testStreamSrcTable1, " + "LATERAL TABLE(hbaseLookup(a))";
		Table result = streamTableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = streamTableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());

		try {
			streamEnv.execute();
		} catch (Exception e) {
			LOG.error("error", e);
		}

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Welt-1");
		expected.add("2,20,Welt-2");
		expected.add("3,30,Welt-3");
		expected.add("3,30,Welt-3");

		StreamITCase.compareWithList(expected);
	}
}
