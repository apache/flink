/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.addons.hbase.HBaseTestingClusterAutostarter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.hbase.util.HBaseUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

/**
 * IT cases for all HBase sinks.
 */
public class HBaseSinkITCase extends HBaseTestingClusterAutostarter {

	private static final String TEST_TABLE = "testTable";

	private static final ArrayList<Row> rowCollection = new ArrayList<>(20);

	private static HTable table;

	static {
		for (int i = 0; i < 20; i++) {
			rowCollection.add(Row.of(String.valueOf(i), i, 0));
		}
	}

	private static final String FAMILY1 = "family1";
	private static final String F1COL1 = "col1";

	private static final String FAMILY2 = "family2";
	private static final String F2COL1 = "col1";

	private static final int rowKeyIndex = 0;
	private static final String[] columnFamilies = new String[] {null, FAMILY1, FAMILY2};
	private static final String[] qualifiers = new String[] {null, F1COL1, F2COL1};
	private static final TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {Types.STRING, Types.INT, Types.INT};

	@BeforeClass
	public static void activateHBaseCluster() throws Exception {
		registerHBaseMiniClusterInClasspath();
		table = prepareTable();
	}

	public static DataStream<Tuple3<Integer, Long, String>> get3TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple3<Integer, Long, String>> data = new ArrayList<>();
		data.add(new Tuple3<>(1, 1L, "Hi"));
		data.add(new Tuple3<>(2, 2L, "Hello"));
		data.add(new Tuple3<>(3, 2L, "Hello world"));
		data.add(new Tuple3<>(4, 3L, "Hello world, how are you?"));
		data.add(new Tuple3<>(5, 3L, "I am fine."));
		data.add(new Tuple3<>(6, 3L, "Luke Skywalker"));
		data.add(new Tuple3<>(7, 4L, "Comment#1"));
		data.add(new Tuple3<>(8, 4L, "Comment#2"));
		data.add(new Tuple3<>(9, 4L, "Comment#3"));
		data.add(new Tuple3<>(10, 4L, "Comment#4"));
		data.add(new Tuple3<>(11, 5L, "Comment#5"));
		data.add(new Tuple3<>(12, 5L, "Comment#6"));
		data.add(new Tuple3<>(13, 5L, "Comment#7"));
		data.add(new Tuple3<>(14, 5L, "Comment#8"));
		data.add(new Tuple3<>(15, 5L, "Comment#9"));
		data.add(new Tuple3<>(16, 6L, "Comment#10"));
		data.add(new Tuple3<>(17, 6L, "Comment#11"));
		data.add(new Tuple3<>(18, 6L, "Comment#12"));
		data.add(new Tuple3<>(19, 6L, "Comment#13"));
		data.add(new Tuple3<>(20, 6L, "Comment#14"));
		data.add(new Tuple3<>(21, 6L, "Comment#15"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	private static HTable prepareTable() throws IOException {
		// create a table
		TableName tableName = TableName.valueOf(TEST_TABLE);
		// column families
		byte[][] families = new byte[][]{
			Bytes.toBytes(FAMILY1),
			Bytes.toBytes(FAMILY2),
		};
		// split keys
		byte[][] splitKeys = new byte[][]{ Bytes.toBytes(4) };

		createTable(tableName, families, splitKeys);

		// get the HTable instance
		HTable table = openTable(tableName);
		return table;
	}

	@Test
	public void testHBaseUpsertSink() throws Exception {
		String[] rowFieldNames = new String[] {"key", "value", "oldValue"};
		RowTypeInfo typeInfo = (RowTypeInfo) Types.ROW_NAMED(rowFieldNames, fieldTypes);
		HBaseUpsertTableSink.HBaseUpsertSinkFunction sink =
			new HBaseUpsertTableSink.HBaseUpsertSinkFunction(
				new TestHBaseTableBuilder(),
				new HashMap<>(),
				rowKeyIndex,
				rowFieldNames,
				columnFamilies,
				qualifiers,
				fieldTypes,
				typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, Row> validationMap = new HashMap<>();

		sink.open(null);
		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(true, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validate(rowKeys, validationMap, (row, i) -> row.getField(i));

		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(false, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validateNull(rowKeys);
	}

	@Test
	public void testHBaseUpsertSinkWithBatchMode() throws Exception {
		HashMap<String, String> userConfig = new HashMap<>();
		userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_ENABLE, "true");
		userConfig.put(HBaseSinkFunctionBase.CONFIG_KEY_BATCH_FLUSH_INTERVAL_MS, "100000");

		String[] rowFieldNames = new String[] {"key", "value", "oldValue"};
		RowTypeInfo typeInfo = (RowTypeInfo) Types.ROW_NAMED(rowFieldNames, fieldTypes);

		HBaseUpsertTableSink.HBaseUpsertSinkFunction sink =
			new HBaseUpsertTableSink.HBaseUpsertSinkFunction(
				new TestHBaseTableBuilder(),
				userConfig,
				rowKeyIndex,
				rowFieldNames,
				columnFamilies,
				qualifiers,
				fieldTypes,
				typeInfo);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		Map<String, Row> validationMap = new HashMap<>();

		sink.open(null);
		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(true, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validateNull(rowKeys);
		sink.snapshotState(null);
		validate(rowKeys, validationMap, (row, i) -> row.getField(i));

		for (Row value : rowCollection) {
			sink.invoke(Tuple2.of(false, value), SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowKeyIndex);
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		validate(rowKeys, validationMap, (row, i) -> row.getField(i));
		sink.snapshotState(null);
		validateNull(rowKeys);
	}

	@Test
	public void testHBaseUpsertTableSink() throws Exception {
		Map<String, String> userConfig = new HashMap<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		env.setParallelism(1);

		org.apache.flink.table.api.Table t = tEnv.fromDataStream(get3TupleDataStream(env).assignTimestampsAndWatermarks(
			new AscendingTimestampExtractor<Tuple3<Integer, Long, String>>() {
				@Override
				public long extractAscendingTimestamp(Tuple3<Integer, Long, String> element) {
					return element.f0;
				}}), "id, num, text");

		tEnv.registerTable("T", t);

		String[] fields = {"rowkey", FAMILY1 + ":cnt", FAMILY1 + ":lencnt", FAMILY1 + ":cTag"};
		tEnv.registerTableSink("upsertSink", HBaseUpsertTableSink.builder()
			.setTableBuilder(new TestHBaseTableBuilder())
			.setSchema(TableSchema.builder().fields(fields, new DataType[] {STRING(), BIGINT(), BIGINT(), INT()}).build())
			.setUserConfig(userConfig)
			.setRowKeyField("rowkey")
			.setDelimiter(":")
			.build());

		tEnv.sqlUpdate("INSERT INTO upsertSink SELECT CAST(cnt AS VARCHAR) || CAST(cTag AS VARCHAR) as rowkey, "
			+ "cnt AS `" + fields[0] + "`, COUNT(len) AS `" + fields[1] + "`, cTag AS `" + fields[3] + "` FROM" +
			" (SELECT len, COUNT(id) as cnt, cTag FROM" +
			" (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag FROM T)" +
			" GROUP BY len, cTag)" +
			" GROUP BY cnt, cTag");
		env.execute();

		// Validate result
		ArrayList<byte[]> rowKeys = new ArrayList<>();
		rowKeys.add("11".getBytes());
		rowKeys.add("71".getBytes());
		rowKeys.add("91".getBytes());

		Map<String, Row> validationMap = new HashMap<>();
		validationMap.put("11", Row.of("11", Long.valueOf(1), Long.valueOf(5), 1));
		validationMap.put("71", Row.of("71", Long.valueOf(7), Long.valueOf(1), 1));
		validationMap.put("91", Row.of("91", Long.valueOf(9), Long.valueOf(1), 1));

		validate(
			rowKeys,
			0,
			new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.LONG, Types.INT},
			new String[]{null, FAMILY1, FAMILY1, FAMILY1},
			new String[]{"rowkey", "cnt", "lencnt", "cTag"},
			validationMap,
			(row, i) -> row.getField(i));
	}

	private <T> void validate(
		ArrayList<byte[]> rowKeys,
		int rowKeyIndex,
		TypeInformation<?>[] fieldTypes,
		String[] columnFamilies,
		String[] qualifiers,
		Map<String, T> validationMap,
		BiFunction<T, Integer, Object> biFunction) throws IOException {
		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < fieldTypes.length; i++) {
				if (i != rowKeyIndex) {
					byte[] result = table.get(new Get(rowKey)).getValue(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
					byte[] validation = HBaseUtils.serialize(fieldTypes[i], biFunction.apply(validationMap.get(new String(rowKey)), i));
					Assert.assertTrue(Arrays.equals(result, validation));
				}
			}
		}
	}

	private <T> void validate(ArrayList<byte[]> rowKeys, Map<String, T> validationMap, BiFunction<T, Integer, Object> biFunction)
		throws IOException {
		validate(rowKeys, rowKeyIndex, fieldTypes, columnFamilies, qualifiers, validationMap, biFunction);
	}

	private void validateNull(ArrayList<byte[]> rowKeys) throws IOException {
		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < fieldTypes.length; i++) {
				if (i != rowKeyIndex) {
					byte[] result = table.get(new Get(rowKey)).getValue(columnFamilies[i].getBytes(), qualifiers[i].getBytes());
					Assert.assertNull(result);
				}
			}
		}
	}

	private final class TestHBaseTableBuilder extends HBaseTableBuilder {

		@Override
		public Connection buildConnection() throws IOException {
			return null;
		}

		@Override
		public Table buildTable(Connection connection) throws IOException {
			return table;
		}
	}
}
