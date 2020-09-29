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

package org.apache.flink.connector.hbase2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.source.HBaseRowDataLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase2.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase2.source.HBaseDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for {@link HBase2DynamicTableFactory}.
 */
public class HBaseDynamicTableFactoryTest {

	private static final String FAMILY1 = "f1";
	private static final String FAMILY2 = "f2";
	private static final String FAMILY3 = "f3";
	private static final String FAMILY4 = "f4";
	private static final String COL1 = "c1";
	private static final String COL2 = "c2";
	private static final String COL3 = "c3";
	private static final String COL4 = "c4";
	private static final String ROWKEY = "rowkey";

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@SuppressWarnings("rawtypes")
	@Test
	public void testTableSourceFactory() {
		TableSchema schema = TableSchema.builder()
			.field(FAMILY1, ROW(FIELD(COL1, INT())))
			.field(FAMILY2, ROW(
				FIELD(COL1, INT()),
				FIELD(COL2, BIGINT())))
			.field(ROWKEY, BIGINT())
			.field(FAMILY3, ROW(
				FIELD(COL1, DOUBLE()),
				FIELD(COL2, BOOLEAN()),
				FIELD(COL3, STRING())))
			.field(FAMILY4, ROW(
				FIELD(COL1, DECIMAL(10, 3)),
				FIELD(COL2, TIMESTAMP(3)),
				FIELD(COL3, DATE()),
				FIELD(COL4, TIME())))
			.build();

		DynamicTableSource source = createTableSource(schema, getAllOptions());
		assertTrue(source instanceof HBaseDynamicTableSource);
		HBaseDynamicTableSource hbaseSource = (HBaseDynamicTableSource) source;

		int[][] lookupKey = {{2}};
		LookupTableSource.LookupRuntimeProvider lookupProvider = hbaseSource
			.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
		assertTrue(lookupProvider instanceof TableFunctionProvider);

		TableFunction tableFunction = ((TableFunctionProvider) lookupProvider).createTableFunction();
		assertTrue(tableFunction instanceof HBaseRowDataLookupFunction);
		assertEquals("testHBastTable", ((HBaseRowDataLookupFunction) tableFunction).getHTableName());

		HBaseTableSchema hbaseSchema = hbaseSource.getHBaseTableSchema();
		assertEquals(2, hbaseSchema.getRowKeyIndex());
		assertEquals(Optional.of(Types.LONG), hbaseSchema.getRowKeyTypeInfo());

		assertArrayEquals(new String[]{"f1", "f2", "f3", "f4"}, hbaseSchema.getFamilyNames());
		assertArrayEquals(new String[]{"c1"}, hbaseSchema.getQualifierNames("f1"));
		assertArrayEquals(new String[]{"c1", "c2"}, hbaseSchema.getQualifierNames("f2"));
		assertArrayEquals(new String[]{"c1", "c2", "c3"}, hbaseSchema.getQualifierNames("f3"));
		assertArrayEquals(new String[]{"c1", "c2", "c3", "c4"}, hbaseSchema.getQualifierNames("f4"));

		assertArrayEquals(new DataType[]{INT()}, hbaseSchema.getQualifierDataTypes("f1"));
		assertArrayEquals(new DataType[]{INT(), BIGINT()}, hbaseSchema.getQualifierDataTypes("f2"));
		assertArrayEquals(new DataType[]{DOUBLE(), BOOLEAN(), STRING()}, hbaseSchema.getQualifierDataTypes("f3"));
		assertArrayEquals(new DataType[]{DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME()},
			hbaseSchema.getQualifierDataTypes("f4"));
	}

	@Test
	public void testTableSinkFactory() {
		TableSchema schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.field(FAMILY1, ROW(
				FIELD(COL1, DOUBLE()),
				FIELD(COL2, INT())))
			.field(FAMILY2, ROW(
				FIELD(COL1, INT()),
				FIELD(COL3, BIGINT())))
			.field(FAMILY3, ROW(
				FIELD(COL2, BOOLEAN()),
				FIELD(COL3, STRING())))
			.field(FAMILY4, ROW(
				FIELD(COL1, DECIMAL(10, 3)),
				FIELD(COL2, TIMESTAMP(3)),
				FIELD(COL3, DATE()),
				FIELD(COL4, TIME())))
			.build();

		DynamicTableSink sink = createTableSink(schema, getAllOptions());
		assertTrue(sink instanceof HBaseDynamicTableSink);
		HBaseDynamicTableSink hbaseSink = (HBaseDynamicTableSink) sink;

		HBaseTableSchema hbaseSchema = hbaseSink.getHBaseTableSchema();
		assertEquals(0, hbaseSchema.getRowKeyIndex());
		assertEquals(Optional.of(STRING()), hbaseSchema.getRowKeyDataType());

		assertArrayEquals(new String[]{"f1", "f2", "f3", "f4"}, hbaseSchema.getFamilyNames());
		assertArrayEquals(new String[]{"c1", "c2"}, hbaseSchema.getQualifierNames("f1"));
		assertArrayEquals(new String[]{"c1", "c3"}, hbaseSchema.getQualifierNames("f2"));
		assertArrayEquals(new String[]{"c2", "c3"}, hbaseSchema.getQualifierNames("f3"));
		assertArrayEquals(new String[]{"c1", "c2", "c3", "c4"}, hbaseSchema.getQualifierNames("f4"));

		assertArrayEquals(new DataType[]{DOUBLE(), INT()}, hbaseSchema.getQualifierDataTypes("f1"));
		assertArrayEquals(new DataType[]{INT(), BIGINT()}, hbaseSchema.getQualifierDataTypes("f2"));
		assertArrayEquals(new DataType[]{BOOLEAN(), STRING()}, hbaseSchema.getQualifierDataTypes("f3"));
		assertArrayEquals(
			new DataType[]{DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME()},
			hbaseSchema.getQualifierDataTypes("f4"));

		HBaseWriteOptions expectedWriteOptions = HBaseWriteOptions.builder()
			.setBufferFlushMaxRows(1000)
			.setBufferFlushIntervalMillis(1000)
			.setBufferFlushMaxSizeInBytes(2 * 1024 * 1024)
			.build();
		HBaseWriteOptions actualWriteOptions = hbaseSink.getWriteOptions();
		assertEquals(expectedWriteOptions, actualWriteOptions);
	}

	@Test
	public void testBufferFlushOptions() {
		Map<String, String> options = getAllOptions();
		options.put("sink.buffer-flush.max-size", "10mb");
		options.put("sink.buffer-flush.max-rows", "100");
		options.put("sink.buffer-flush.interval", "10s");

		TableSchema schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.build();

		DynamicTableSink sink = createTableSink(schema, options);
		HBaseWriteOptions expected = HBaseWriteOptions.builder()
			.setBufferFlushMaxRows(100)
			.setBufferFlushIntervalMillis(10 * 1000)
			.setBufferFlushMaxSizeInBytes(10 * 1024 * 1024)
			.build();
		HBaseWriteOptions actual = ((HBaseDynamicTableSink) sink).getWriteOptions();
		assertEquals(expected, actual);
	}

	@Test
	public void testDisabledBufferFlushOptions() {
		Map<String, String> options = getAllOptions();
		options.put("sink.buffer-flush.max-size", "0");
		options.put("sink.buffer-flush.max-rows", "0");
		options.put("sink.buffer-flush.interval", "0");

		TableSchema schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.build();

		DynamicTableSink sink = createTableSink(schema, options);
		HBaseWriteOptions expected = HBaseWriteOptions.builder()
			.setBufferFlushMaxRows(0)
			.setBufferFlushIntervalMillis(0)
			.setBufferFlushMaxSizeInBytes(0)
			.build();
		HBaseWriteOptions actual = ((HBaseDynamicTableSink) sink).getWriteOptions();
		assertEquals(expected, actual);
	}

	@Test
	public void testUnknownOption() {
		Map<String, String> options = getAllOptions();
		options.put("sink.unknown.key", "unknown-value");
		TableSchema schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.field(FAMILY1, ROW(
				FIELD(COL1, DOUBLE()),
				FIELD(COL2, INT())))
			.build();

		try {
			createTableSource(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "Unsupported options:\n\nsink.unknown.key")
				.isPresent());
		}

		try {
			createTableSink(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "Unsupported options:\n\nsink.unknown.key")
				.isPresent());
		}
	}

	@Test
	public void testTypeWithUnsupportedPrecision() {
		Map<String, String> options = getAllOptions();
		// test unsupported timestamp precision
		TableSchema schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.field(FAMILY1, ROW(
				FIELD(COL1, TIMESTAMP(6)),
				FIELD(COL2, INT())))
			.build();
		try {
			createTableSource(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "The precision 6 of TIMESTAMP type is out of the range [0, 3]" +
					" supported by HBase connector")
				.isPresent());
		}

		try {
			createTableSink(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "The precision 6 of TIMESTAMP type is out of the range [0, 3]" +
					" supported by HBase connector")
				.isPresent());
		}
		// test unsupported time precision
		schema = TableSchema.builder()
			.field(ROWKEY, STRING())
			.field(FAMILY1, ROW(
				FIELD(COL1, TIME(6)),
				FIELD(COL2, INT())))
			.build();

		try {
			createTableSource(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "The precision 6 of TIME type is out of the range [0, 3]" +
					" supported by HBase connector")
				.isPresent());
		}

		try {
			createTableSink(schema, options);
			fail("Should fail");
		} catch (Exception e) {
			assertTrue(ExceptionUtils
				.findThrowableWithMessage(e, "The precision 6 of TIME type is out of the range [0, 3]" +
					" supported by HBase connector")
				.isPresent());
		}
	}

	private Map<String, String> getAllOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", "hbase-2.2");
		options.put("table-name", "testHBastTable");
		options.put("zookeeper.quorum", "localhost:2181");
		options.put("zookeeper.znode.parent", "/flink");
		return options;
	}

	private static DynamicTableSource createTableSource(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock source"),
			new Configuration(),
			HBase2DynamicTableFactory.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock sink"),
			new Configuration(),
			HBase2DynamicTableFactory.class.getClassLoader());
	}

}
