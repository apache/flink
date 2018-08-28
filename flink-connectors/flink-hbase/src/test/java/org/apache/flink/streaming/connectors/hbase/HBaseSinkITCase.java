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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * IT cases for all HBase sinks.
 */
public class HBaseSinkITCase extends HBaseTestingClusterAutostarter {

	private static final String TEST_TABLE = "testTable";

	private static final ArrayList<Tuple3<String, Integer, Integer>> collection = new ArrayList<>(20);
	private static final ArrayList<TestPojo> pojoCollection = new ArrayList<>(20);
	private static final ArrayList<Row> rowCollection = new ArrayList<>(20);
	private static final List<scala.Tuple3<String, Integer, Integer>> scalaTupleCollection = new ArrayList<>(20);

	private static HTable table;

	static {
		for (int i = 0; i < 20; i++) {
			collection.add(new Tuple3<>(UUID.randomUUID().toString(), i, 0));
			pojoCollection.add(new TestPojo(UUID.randomUUID().toString(), i, 0));
			rowCollection.add(Row.of(UUID.randomUUID().toString(), i, 0));
			scalaTupleCollection.add(new scala.Tuple3<>(UUID.randomUUID().toString(), i, 0));
		}
	}

	private static final String FAMILY1 = "family1";
	private static final String F1COL1 = "col1";

	private static final String FAMILY2 = "family2";
	private static final String F2COL1 = "col1";
	private static final String F2COL2 = "col2";

	@BeforeClass public static void activateHBaseCluster() throws Exception {
		registerHBaseMiniClusterInClasspath();
		table = prepareTable();
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

	// ####### HBaseSink tests ############

	@Test
	public void testHBaseTupleSink() throws Exception {
		HBaseTableMapper tableMapper = new HBaseTableMapper();
		tableMapper.addMapping(0, FAMILY1, F1COL1, String.class)
			.addMapping(1, FAMILY2, F2COL1, Integer.class)
			.addMapping(2, FAMILY2, F2COL2, Integer.class)
			.setRowKey(0, String.class);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		String[] keyList = tableMapper.getKeyList();
		Map<String, Tuple3<byte[], byte[], byte[]>> validationMap = new HashMap<>();

		HBaseTupleSink<Tuple3<String, Integer, Integer>> sink = new HBaseTupleSink<>(table, tableMapper);

		Configuration configuration = new Configuration();
		sink.open(configuration);

		for (Tuple3<String, Integer, Integer> value : collection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value.f0));
			validationMap.put(value.f0, Tuple3.of(Bytes.toBytes(value.f0), Bytes.toBytes(value.f1), Bytes.toBytes(value.f2)));
		}
		sink.close();
		for (byte[] rowKey : rowKeys) {
			// the field was processed in the ordered of keyList
			for (int i = 0; i < keyList.length; i++) {
				Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(keyList[i]);
				byte[] result = table.get(new Get(rowKey)).getValue(colInfo.f0, colInfo.f1);
				byte[] validation = validationMap.get(new String(rowKey, tableMapper.getCharset())).getField(i);
				Assert.assertTrue(Arrays.equals(result, validation));
			}
		}
	}

	@Test
	public void testHBasePojoSink() throws Exception {
		HBaseTableMapper tableMapper = new HBaseTableMapper();
		tableMapper.addMapping("key", FAMILY1, F1COL1, String.class)
			.addMapping("value", FAMILY2, F2COL1, Integer.class)
			.addMapping("oldValue", FAMILY2, F2COL2, Integer.class)
			.setRowKey("key", String.class);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		String[] keyList = tableMapper.getKeyList();
		Map<String, TestPojo> validationMap = new HashMap<>();

		HBasePojoSink<TestPojo> sink = new HBasePojoSink<TestPojo>(table, tableMapper, TypeInformation.of(TestPojo.class));

		Configuration configuration = new Configuration();
		sink.open(configuration);

		for (TestPojo value : pojoCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value.getKey()));
			validationMap.put(value.getKey(), value);
		}
		sink.close();

		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < keyList.length; i++) {
				Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(keyList[i]);
				byte[] result = table.get(new Get(rowKey)).getValue(colInfo.f0, colInfo.f1);
				Field field = TestPojo.class.getDeclaredField(keyList[i]);
				field.setAccessible(true);
				Object fieldValue = field.get(validationMap.get(new String(rowKey, tableMapper.getCharset())));
				byte[] validation = HBaseTableMapper.serialize(colInfo.f2, fieldValue);
				Assert.assertTrue(Arrays.equals(result, validation));
			}
		}
	}

	@Test
	public void testHBaseScalaProductSink() throws Exception {
		HBaseTableMapper tableMapper = new HBaseTableMapper();
		tableMapper.addMapping(0, FAMILY1, F1COL1, String.class)
			.addMapping(1, FAMILY2, F2COL1, Integer.class)
			.addMapping(2, FAMILY2, F2COL2, Integer.class)
			.setRowKey(0, String.class);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		String[] keyList = tableMapper.getKeyList();
		Map<String, scala.Tuple3<String, Integer, Integer>> validationMap = new HashMap<>();

		HBaseScalaProductSink<scala.Tuple3<String, Integer, Integer>> sink =
			new HBaseScalaProductSink<scala.Tuple3<String, Integer, Integer>>(table, tableMapper);
		for (scala.Tuple3<String, Integer, Integer> value : scalaTupleCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			rowKeys.add(Bytes.toBytes(value._1()));
			validationMap.put(value._1(), value);
		}
		sink.close();
		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < keyList.length; i++) {
				Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(keyList[i]);
				byte[] result = table.get(new Get(rowKey)).getValue(colInfo.f0, colInfo.f1);
				scala.Tuple3<String, Integer, Integer> validationTuple =
					validationMap.get(new String(rowKey, tableMapper.getCharset()));
				byte[] validation = HBaseTableMapper.serialize(colInfo.f2, validationTuple.productElement(i));
				Assert.assertTrue(Arrays.equals(result, validation));
			}
		}
	}

	@Test
	public void testHBaseRowSink() throws Exception {
		HBaseTableMapper tableMapper = new HBaseTableMapper();
		tableMapper.addMapping("key", FAMILY1, F1COL1, String.class)
			.addMapping("value", FAMILY2, F2COL1, Integer.class)
			.addMapping("oldValue", FAMILY2, F2COL2, Integer.class)
			.setRowKey("key", String.class);

		ArrayList<byte[]> rowKeys = new ArrayList<>();
		String[] keyList = tableMapper.getKeyList();
		Map<String, Row> validationMap = new HashMap<>();

		TypeInformation[] types = {Types.STRING(), Types.INT(), Types.INT()};
		String[] fieldNames = {"key", "value", "oldValue"};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

		HBaseRowSink sink = new HBaseRowSink(table, tableMapper, rowTypeInfo);

		Configuration configuration = new Configuration();
		sink.open(configuration);

		for (Row value : rowCollection) {
			sink.invoke(value, SinkContextUtil.forTimestamp(0));
			String valueKey = (String) value.getField(rowTypeInfo.getFieldIndex("key"));
			rowKeys.add(Bytes.toBytes(valueKey));
			validationMap.put(valueKey, value);
		}
		sink.close();

		for (byte[] rowKey : rowKeys) {
			for (int i = 0; i < keyList.length; i++) {
				Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(keyList[i]);
				byte[] result = table.get(new Get(rowKey)).getValue(colInfo.f0, colInfo.f1);
				byte[] validation = HBaseTableMapper.serialize(colInfo.f2,
					validationMap.get(new String(rowKey, tableMapper.getCharset()))
						.getField(rowTypeInfo.getFieldIndex(keyList[i])));
				Assert.assertTrue(Arrays.equals(result, validation));
			}
		}
	}
}
