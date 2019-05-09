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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.pojo.PojoSimpleRecord;
import org.apache.flink.formats.parquet.utils.TestUtil;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for reading Pojo from Parquet files.
 */
public class ParquetPojoInputFormatTest {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	@Test
	public void testReadPojoFromSimpleRecord() throws IOException {
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = TestUtil.getSimpleRecordTestData();
		MessageType messageType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);
		Path path = TestUtil.createTempParquetFile(tempRoot.getRoot(), TestUtil.SIMPLE_SCHEMA, Collections.singletonList(simple.f1));

		ParquetPojoInputFormat<PojoSimpleRecord> inputFormat = new ParquetPojoInputFormat<>(
			path, messageType, (PojoTypeInfo<PojoSimpleRecord>) Types.POJO(PojoSimpleRecord.class));
		inputFormat.setRuntimeContext(TestUtil.getMockRuntimeContext());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		inputFormat.open(splits[0]);

		PojoSimpleRecord simpleRecord = inputFormat.nextRecord(null);
		assertEquals(simple.f2.getField(0), simpleRecord.getFoo());
		assertEquals(simple.f2.getField(1), simpleRecord.getBar());
		assertArrayEquals((Long[]) simple.f2.getField(2), simpleRecord.getArr());
	}

	@Test
	public void testProjectedReadPojoFromSimpleRecord() throws IOException, NoSuchFieldError {
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = TestUtil.getSimpleRecordTestData();
		MessageType messageType = SCHEMA_CONVERTER.convert(TestUtil.SIMPLE_SCHEMA);
		Path path = TestUtil.createTempParquetFile(tempRoot.getRoot(), TestUtil.SIMPLE_SCHEMA, Collections.singletonList(simple.f1));

		ParquetPojoInputFormat<PojoSimpleRecord> inputFormat = new ParquetPojoInputFormat<>(
			path, messageType, (PojoTypeInfo<PojoSimpleRecord>) Types.POJO(PojoSimpleRecord.class));
		inputFormat.setRuntimeContext(TestUtil.getMockRuntimeContext());

		FileInputSplit[] splits = inputFormat.createInputSplits(1);
		assertEquals(1, splits.length);

		inputFormat.selectFields(new String[]{"foo"});
		inputFormat.open(splits[0]);

		PojoSimpleRecord simpleRecord = inputFormat.nextRecord(null);
		assertEquals(simple.f2.getField(0), simpleRecord.getFoo());
		assertEquals("", simpleRecord.getBar());
		assertArrayEquals(new Long[0], simpleRecord.getArr());
	}
}
