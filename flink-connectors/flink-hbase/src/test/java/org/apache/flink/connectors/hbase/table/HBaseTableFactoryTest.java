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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * UT for HBaseTableFactory.
 */
public class HBaseTableFactoryTest {
	private static final String FAMILY1 = "f1";
	private static final String FAMILY2 = "f2";
	private static final String FAMILY3 = "f3";
	private static final String COL1 = "c1";
	private static final String COL2 = "c2";
	private static final String COL3 = "c3";

	private DescriptorProperties createDescriptor(String[] columnNames, DataType[] columnTypes) {
		TableSchema.Builder builder = new TableSchema.Builder();
		TableSchema tableSchema = builder.fields(columnNames, columnTypes).build();

		Map<String, String> tableServiceLookupConf = new HashMap<>();
		tableServiceLookupConf.put(ConnectorDescriptorValidator.CONNECTOR_TYPE.toLowerCase(), "hbase");
		tableServiceLookupConf.put(CONNECTOR_VERSION.toLowerCase(), CONNECTOR_VERSION_VALUE_143);
		tableServiceLookupConf.put(CONNECTOR_PROPERTY_VERSION.toLowerCase(), "1");
		tableServiceLookupConf.put(CONNECTOR_HBASE_TABLE_NAME, "testHBastTable");

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(tableServiceLookupConf);
		return descriptorProperties;
	}

	@Test
	public void testConstructor() {
		String[] columnNames = {FAMILY1 + ":" + COL1, FAMILY2 + ":" + COL1, "ROWKEY", FAMILY2 + ":" + COL2, FAMILY3 + ":" + COL1, FAMILY3 + ":" + COL2, FAMILY3 + ":" + COL3};
		DataType[] columnTypes = {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.STRING()};

		DescriptorProperties descriptorProperties = createDescriptor(columnNames, columnTypes);
		TableSource source = TableFactoryService.find(HBaseTableFactory.class,
			descriptorProperties.asMap()).createTableSource(descriptorProperties.asMap());
		Assert.assertTrue(source instanceof HBaseStreamTableSource);
		HBaseTableContext hBaseTableContext = ((HBaseStreamTableSource) source).gethBaseTableContext();
		Assert.assertEquals("testHBastTable", hBaseTableContext.gethTableName());
		Assert.assertEquals("ROWKEY", hBaseTableContext.getRowkey());
		Assert.assertEquals(2, hBaseTableContext.getRowKeyIndex());
		Assert.assertEquals(DataTypes.BIGINT(),
			TypeConversions.fromLegacyInfoToDataType(hBaseTableContext.getRowKeyType()));
		Assert.assertArrayEquals(new Integer[]{0, 1, 3, 4, 5, 6},
			hBaseTableContext.getQualifierIndexes().toArray(new Integer[0]));

		HBaseTableSchema hbaseTableSchema = hBaseTableContext.gethBaseTableSchema();
		List<Tuple3<byte[], byte[], TypeInformation<?>>> qulifiers = hbaseTableSchema.getFlatByteQualifiers();

		List<String> resultColumn = new ArrayList<>();
		List<DataType> resultDataType = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			resultColumn.add(Bytes.toString(qulifiers.get(i).f0) + ":" + Bytes.toString(qulifiers.get(i).f1));
			resultDataType.add(TypeConversions.fromLegacyInfoToDataType(qulifiers.get(i).f2));
		}

		Assert.assertArrayEquals(new String[]{"f1:c1", "f2:c1", "f2:c2", "f3:c1", "f3:c2", "f3:c3"},
			resultColumn.toArray(new String[0]));
		Assert.assertArrayEquals(new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.STRING()},
			resultDataType.toArray(new DataType[0]));

		TableFunction<Row> tf = ((HBaseStreamTableSource) source).getLookupFunction(new String[]{""});
		Assert.assertTrue(tf instanceof HBaseLookupFunction);

		String expectExceptionString = "WANT_EXCEPTION";
		try {
			((HBaseStreamTableSource) source).getLookupFunction(new String[]{"1", "2"});
		} catch (Exception e) {
			expectExceptionString = e.getMessage();
		}
		Assert.assertEquals("HBase table can only be retrieved by rowKey for now.", expectExceptionString);
	}

	@Test
	public void testErrorKeyForHack() {
		String expectExceptionString = "WANT_EXCEPTION";
		try {
			String[] columnNames = {FAMILY1 + ":" + COL1, "ERROR2", "ROWKEY", FAMILY2 + ":" + COL2, FAMILY3 + ":" + COL1, FAMILY3 + ":" + COL2, FAMILY3 + ":" + COL3};
			DataType[] columnTypes = {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(), DataTypes.STRING()};

			DescriptorProperties descriptorProperties = createDescriptor(columnNames, columnTypes);
			TableFactoryService.find(HBaseTableFactory.class, descriptorProperties.asMap()).createTableSource(
				descriptorProperties.asMap());
		} catch (Exception e) {
			expectExceptionString = e.getMessage();
		}
		Assert.assertEquals(
			"a column which doesn't contain delimiter(:) is regarded as rowkey, now only support 1 rowkey, but now has 2",
			expectExceptionString);
	}
}
