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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_HBASE_ZK_QUORUM;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
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
	private static final String ROWKEY = "rowkey";

	private DescriptorProperties createDescriptor(String[] columnNames, TypeInformation[] columnTypes) {
		TableSchema tableSchema = new TableSchema(columnNames, columnTypes);

		Map<String, String> tableServiceLookupConf = new HashMap<>();
		tableServiceLookupConf.put(CONNECTOR_TYPE, "hbase");
		tableServiceLookupConf.put(CONNECTOR_VERSION, CONNECTOR_VERSION_VALUE_143);
		tableServiceLookupConf.put(CONNECTOR_PROPERTY_VERSION, "1");
		tableServiceLookupConf.put(CONNECTOR_HBASE_TABLE_NAME, "testHBastTable");
		tableServiceLookupConf.put(CONNECTOR_HBASE_ZK_QUORUM, "localhost:2181");

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(tableServiceLookupConf);
		return descriptorProperties;
	}

	@Test
	public void testConstructorForNestedSchema() {
		String[] columnNames = {FAMILY1, FAMILY2, ROWKEY, FAMILY3};

		TypeInformation<Row> f1 = Types.ROW_NAMED(new String[]{COL1}, Types.INT);
		TypeInformation<Row> f2 = Types.ROW_NAMED(new String[]{COL1, COL2}, Types.INT, Types.LONG);
		TypeInformation<Row> f3 = Types.ROW_NAMED(new String[]{COL1, COL2, COL3}, Types.DOUBLE, Types.BOOLEAN, Types.STRING);
		TypeInformation[] columnTypes = new TypeInformation[]{f1, f2, Types.LONG, f3};

		DescriptorProperties descriptorProperties = createDescriptor(columnNames, columnTypes);
		TableSource source = TableFactoryService.find(HBaseTableFactory.class,
			descriptorProperties.asMap()).createTableSource(descriptorProperties.asMap());
		Assert.assertTrue(source instanceof HBaseTableSource);
		TableFunction<Row> tableFunction = ((HBaseTableSource) source).getLookupFunction(new String[]{ROWKEY});
		Assert.assertTrue(tableFunction instanceof HBaseLookupFunction);
		Assert.assertEquals("testHBastTable", ((HBaseLookupFunction) tableFunction).getHTableName());

		HBaseTableSchema hbaseSchema = ((HBaseTableSource) source).getHBaseTableSchema();
		Assert.assertEquals(2, hbaseSchema.getRowKeyIndex());
		Assert.assertEquals(Optional.of(Types.LONG), hbaseSchema.getRowKeyTypeInfo());

		Assert.assertArrayEquals(new String[]{"f1", "f2", "f3"}, hbaseSchema.getFamilyNames());
		Assert.assertArrayEquals(new String[]{"c1"}, hbaseSchema.getQualifierNames("f1"));
		Assert.assertArrayEquals(new String[]{"c1", "c2"}, hbaseSchema.getQualifierNames("f2"));
		Assert.assertArrayEquals(new String[]{"c1", "c2", "c3"}, hbaseSchema.getQualifierNames("f3"));

		Assert.assertArrayEquals(new TypeInformation[]{Types.INT}, hbaseSchema.getQualifierTypes("f1"));
		Assert.assertArrayEquals(new TypeInformation[]{Types.INT, Types.LONG}, hbaseSchema.getQualifierTypes("f2"));
		Assert.assertArrayEquals(new TypeInformation[]{Types.DOUBLE, Types.BOOLEAN, Types.STRING}, hbaseSchema.getQualifierTypes("f3"));
	}
}
