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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.generated.SimpleRecord;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
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
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, simple.f1, 1);
		MessageType simpleType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(
			path, (RowTypeInfo) ParquetSchemaConverter.fromParquetType(simpleType));

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
	public void testReadRowFromNestedRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = TestUtil.getNestedRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.NESTED_SCHEMA, nested.f1, 1);
		MessageType nestedType = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);

		ParquetRowInputFormat rowInputFormat = new ParquetRowInputFormat(
			path, (RowTypeInfo) ParquetSchemaConverter.fromParquetType(nestedType));

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
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.SIMPLE_SCHEMA, simple.f1, 1);

		List<PojoField> fieldList = new ArrayList<>();
		fieldList.add(new PojoField(simple.f0.getField("foo"), BasicTypeInfo.LONG_TYPE_INFO));
		fieldList.add(new PojoField(simple.f0.getField("bar"), BasicTypeInfo.STRING_TYPE_INFO));

		ParquetPojoInputFormat<SimpleRecord> pojoInputFormat =
			new ParquetPojoInputFormat(path, new PojoTypeInfo<SimpleRecord>(SimpleRecord.class, fieldList));

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		pojoInputFormat.setRuntimeContext(mockContext);

		FileInputSplit[] splits = pojoInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		pojoInputFormat.open(splits[0]);

		SimpleRecord simpleRecord = pojoInputFormat.nextRecord(null);
		assertEquals(simple.f1, simpleRecord);
	}

	@Test
	public void testReadMapFromNestedRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = TestUtil.getNestedRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.NESTED_SCHEMA, nested.f1, 1);
		MessageType nestedType = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(nestedType);

		ParquetMapInputFormat mapInputFormat = new ParquetMapInputFormat(
			path, rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames());

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		mapInputFormat.setRuntimeContext(mockContext);
		FileInputSplit[] splits = mapInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		mapInputFormat.open(splits[0]);

		Map map = mapInputFormat.nextRecord(null);
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
}
