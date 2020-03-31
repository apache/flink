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
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

		Map<String, String> tableProperties = new HashMap<>();
		tableProperties.put("connector.type", "hbase");
		tableProperties.put("connector.version", "1.4.3");
		tableProperties.put("connector.property-version", "1");
		tableProperties.put("connector.table-name", "testHBastTable");
		tableProperties.put("connector.zookeeper.quorum", "localhost:2181");
		tableProperties.put("connector.zookeeper.znode.parent", "/flink");
		tableProperties.put("connector.write.buffer-flush.max-size", "10mb");
		tableProperties.put("connector.write.buffer-flush.max-rows", "1000");
		tableProperties.put("connector.write.buffer-flush.interval", "10s");

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(tableProperties);
		return descriptorProperties;
	}

	@Test
	public void testTableSourceFactory() {
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

	@Test
	public void testTableSinkFactory() {
		String[] columnNames = {ROWKEY, FAMILY1, FAMILY2, FAMILY3};
		TypeInformation<Row> f1 = Types.ROW_NAMED(new String[]{COL1, COL2}, Types.DOUBLE, Types.INT);
		TypeInformation<Row> f2 = Types.ROW_NAMED(new String[]{COL1, COL3}, Types.INT, Types.LONG);
		TypeInformation<Row> f3 = Types.ROW_NAMED(new String[]{COL2, COL3}, Types.BOOLEAN, Types.STRING);
		TypeInformation[] columnTypes = new TypeInformation[]{Types.STRING, f1, f2, f3};
		DescriptorProperties descriptorProperties = createDescriptor(columnNames, columnTypes);

		TableSink sink = TableFactoryService
			.find(HBaseTableFactory.class, descriptorProperties.asMap())
			.createTableSink(descriptorProperties.asMap());

		Assert.assertTrue(sink instanceof HBaseUpsertTableSink);

		HBaseTableSchema hbaseSchema = ((HBaseUpsertTableSink) sink).getHBaseTableSchema();
		Assert.assertEquals(0, hbaseSchema.getRowKeyIndex());
		Assert.assertEquals(Optional.of(Types.STRING), hbaseSchema.getRowKeyTypeInfo());

		Assert.assertArrayEquals(new String[]{"f1", "f2", "f3"}, hbaseSchema.getFamilyNames());
		Assert.assertArrayEquals(new String[]{"c1", "c2"}, hbaseSchema.getQualifierNames("f1"));
		Assert.assertArrayEquals(new String[]{"c1", "c3"}, hbaseSchema.getQualifierNames("f2"));
		Assert.assertArrayEquals(new String[]{"c2", "c3"}, hbaseSchema.getQualifierNames("f3"));

		Assert.assertArrayEquals(new TypeInformation[]{Types.DOUBLE, Types.INT}, hbaseSchema.getQualifierTypes("f1"));
		Assert.assertArrayEquals(new TypeInformation[]{Types.INT, Types.LONG}, hbaseSchema.getQualifierTypes("f2"));
		Assert.assertArrayEquals(new TypeInformation[]{Types.BOOLEAN, Types.STRING}, hbaseSchema.getQualifierTypes("f3"));

		HBaseOptions expectedHBaseOptions = HBaseOptions.builder()
			.setTableName("testHBastTable")
			.setZkQuorum("localhost:2181")
			.setZkNodeParent("/flink")
			.build();
		HBaseOptions actualHBaseOptions = ((HBaseUpsertTableSink) sink).getHBaseOptions();
		Assert.assertEquals(expectedHBaseOptions, actualHBaseOptions);

		HBaseWriteOptions expectedWriteOptions = HBaseWriteOptions.builder()
			.setBufferFlushMaxRows(1000)
			.setBufferFlushIntervalMillis(10 * 1000)
			.setBufferFlushMaxSizeInBytes(10 * 1024 * 1024)
			.build();
		HBaseWriteOptions actualWriteOptions = ((HBaseUpsertTableSink) sink).getWriteOptions();
		Assert.assertEquals(expectedWriteOptions, actualWriteOptions);
	}
}
