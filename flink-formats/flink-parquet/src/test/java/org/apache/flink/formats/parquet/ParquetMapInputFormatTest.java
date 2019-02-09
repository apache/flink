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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test cases for reading Map from Parquet files.
 */
public class ParquetMapInputFormatTest {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder temp = new TemporaryFolder();

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
		assertEquals(5, map.size());
		assertArrayEquals((Long[]) map.get("arr"), (Long[]) nested.f2.getField(3));
		assertArrayEquals((String[]) map.get("strArray"), (String[]) nested.f2.getField(4));

		Map<String, String> mapItem = (Map<String, String>) ((Map) map.get("nestedMap")).get("mapItem");
		assertEquals(2, mapItem.size());
		assertEquals("map", mapItem.get("type"));
		assertEquals("hashMap", mapItem.get("value"));

		List<Map<String, String>> nestedArray = (List<Map<String, String>>) map.get("nestedArray");

		assertEquals(1, nestedArray.size());
		assertEquals("color", nestedArray.get(0).get("type"));
		assertEquals("yellow", nestedArray.get(0).get("value"));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testProjectedReadMapFromNestedRecord() throws IOException {
		temp.create();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> nested = TestUtil.getNestedRecordTestData();
		Path path = TestUtil.createTempParquetFile(temp, TestUtil.NESTED_SCHEMA, Collections.singletonList(nested.f1));
		MessageType nestedType = SCHEMA_CONVERTER.convert(TestUtil.NESTED_SCHEMA);
		ParquetMapInputFormat mapInputFormat = new ParquetMapInputFormat(path, nestedType);
		mapInputFormat.selectFields(Arrays.asList("nestedMap").toArray(new String[0]));

		RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
		Mockito.doReturn(UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup())
			.when(mockContext).getMetricGroup();
		mapInputFormat.setRuntimeContext(mockContext);
		FileInputSplit[] splits = mapInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		mapInputFormat.open(splits[0]);

		Map map = mapInputFormat.nextRecord(null);
		assertNotNull(map);
		assertEquals(1, map.size());

		Map<String, String> mapItem = (Map<String, String>) ((Map) map.get("nestedMap")).get("mapItem");
		assertEquals(2, mapItem.size());
		assertEquals("map", mapItem.get("type"));
		assertEquals("hashMap", mapItem.get("value"));
	}
}
