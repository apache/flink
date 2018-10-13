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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.generated.SimpleRecord;
import org.apache.flink.formats.parquet.pojo.PojoSimpleRecord;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Simple test case for reading {@link org.apache.flink.types.Row}, Map and Pojo from Parquet files.
 */
public class ParquetInputFormatTest {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder temp = new TemporaryFolder();

	@Test
	public void testReadRowFromSimpleRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = TestUtil.getSimpleRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, Collections.singletonList(simple.f1));
		MessageType simpleType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(path, simpleType);

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		rowInputFormat.setRuntimeContext(mockContext);

		FileInputSplit[] splits = rowInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowInputFormat.open(splits[0]);

		Row row = rowInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(simple.f2, row);
	}

	@Test
	public void testFailureRecoverySimpleRecord() throws IOException {
		temp.create();
		List<IndexedRecord> records = new ArrayList<>();
		Long[] longArray = {1L};
		for (long i = 0; i < 100; i++) {
			final SimpleRecord simpleRecord = SimpleRecord.newBuilder()
				.setBar("test_simple")
				.setFoo(i)
				.setArr(Arrays.asList(longArray)).build();
			records.add(simpleRecord);
		}

		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, records);
		MessageType simpleType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(path, simpleType);

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		rowInputFormat.setRuntimeContext(mockContext);

		FileInputSplit[] splits = rowInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);

		final Tuple2<Long, Long> checkpoint = new Tuple2<>();
		checkpoint.f0 = 0L;
		checkpoint.f1 = 51L;
		rowInputFormat.reopen(splits[0], checkpoint);
		Row row = rowInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(51L, row.getField(0));

		for (int i = 0; i < 10; i++) {
			rowInputFormat.nextRecord(null);
		}

		Tuple2<Long, Long> state = rowInputFormat.getCurrentState();
		assertEquals(0L, state.f0.longValue());
		assertEquals(62L, state.f1.longValue());
	}

	@Test
	public void testReadRowFromNestedRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = TestUtil.getNestedRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.NESTED_SCHEMA, Collections.singletonList(nested.f1));
		MessageType nestedType = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(path, nestedType);
		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		rowInputFormat.setRuntimeContext(mockContext);

		FileInputSplit[] splits = rowInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowInputFormat.open(splits[0]);

		Row row = rowInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(7, row.getArity());

		assertEquals(nested.f2.getField(0), row.getField(0));
		assertEquals(nested.f2.getField(1), row.getField(1));
		assertArrayEquals((Long[]) nested.f2.getField(3), (Long[]) row.getField(3));
		assertArrayEquals((String[]) nested.f2.getField(4), (String[]) row.getField(4));
		assertEquals(nested.f2.getField(5), row.getField(5));
		assertArrayEquals((Row[]) nested.f2.getField(6), (Row[]) row.getField(6));
	}

	@Test
	public void testReadPojoFromSimpleRecord() throws IOException, NoSuchFieldException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = TestUtil.getSimpleRecordTestData();
		MessageType messageType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, Collections.singletonList(simple.f1));

		List<PojoField> fieldList = new ArrayList<>();
		fieldList.add(new PojoField(PojoSimpleRecord.class.getField("foo"), BasicTypeInfo.LONG_TYPE_INFO));
		fieldList.add(new PojoField(PojoSimpleRecord.class.getField("bar"), BasicTypeInfo.STRING_TYPE_INFO));
		fieldList.add(new PojoField(PojoSimpleRecord.class.getField("arr"),
			BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO));

		ParquetPojoInputFormat<PojoSimpleRecord> pojoInputFormat =
			new ParquetPojoInputFormat<PojoSimpleRecord>(path, messageType, new PojoTypeInfo<PojoSimpleRecord>(
				PojoSimpleRecord.class, fieldList));

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		pojoInputFormat.setRuntimeContext(mockContext);

		FileInputSplit[] splits = pojoInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		pojoInputFormat.open(splits[0]);

		PojoSimpleRecord simpleRecord = pojoInputFormat.nextRecord(null);
		assertEquals(simple.f2.getField(0), simpleRecord.getFoo());
		assertEquals(simple.f2.getField(1), simpleRecord.getBar());
		assertArrayEquals((Long[]) simple.f2.getField(2), simpleRecord.getArr());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReadMapFromNestedRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = TestUtil.getNestedRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.NESTED_SCHEMA, Collections.singletonList(nested.f1));
		MessageType nestedType = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		ParquetMapInputFormat mapInputFormat = new ParquetMapInputFormat(path, nestedType);

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		mapInputFormat.setRuntimeContext(mockContext);
		FileInputSplit[] splits = mapInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		mapInputFormat.open(splits[0]);

		Map map = mapInputFormat.nextRecord(null);
		assertNotNull(map);
		assertArrayEquals((Long[]) map.get("arr"), (Long[]) nested.f2.getField(3));
		assertArrayEquals((String[]) map.get("strArray"), (String[]) nested.f2.getField(4));

		Map<String, String> mapItem = (Map<String, String>) ((Map) map.get("nestedMap")).get("mapItem");
		assertEquals("map", mapItem.get("type"));
		assertEquals("hashMap", mapItem.get("value"));

		List<Map<String, String>> nestedArray = (List<Map<String, String>>) map.get("nestedArray");

		assertEquals(1, nestedArray.size());
		assertEquals("color", nestedArray.get(0).get("type"));
		assertEquals("yellow", nestedArray.get(0).get("value"));
	}

	@Test
	public void testSerialization() throws Exception {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = TestUtil.getSimpleRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, Collections.singletonList(simple.f1));
		MessageType simpleType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(path, simpleType);
		byte[] bytes = InstantiationUtil.serializeObject(rowInputFormat);
		ParquetRowInputFormat copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		copy.setRuntimeContext(mockContext);

		FileInputSplit[] splits = copy.createInputSplits(1);
		assertEquals(1, splits.length);
		copy.open(splits[0]);

		Row row = copy.nextRecord(null);
		assertNotNull(row);
		assertEquals(simple.f2, row);
	}
}
