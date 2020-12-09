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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for reading Pojo from Parquet files.
 */
@RunWith(Parameterized.class)
public class ParquetPojoInputFormatTest extends TestUtil {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	public ParquetPojoInputFormatTest(boolean useLegacyMode) {
		super(useLegacyMode);
	}

	@Test
	public void testReadPojoFromSimpleRecord() throws IOException {
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> simple = getSimpleRecordTestData();
		MessageType messageType = getSchemaConverter().convert(SIMPLE_SCHEMA);
		Path path = createTempParquetFile(tempRoot.getRoot(), SIMPLE_SCHEMA,
			Collections.singletonList(simple.f1), getConfiguration());

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
		MessageType messageType = getSchemaConverter().convert(SIMPLE_SCHEMA);
		Path path = createTempParquetFile(tempRoot.getRoot(), SIMPLE_SCHEMA,
			Collections.singletonList(simple.f1), getConfiguration());

		ParquetPojoInputFormat<PojoSimpleRecord> inputFormat = new ParquetPojoInputFormat<>(
			path, messageType, (PojoTypeInfo<PojoSimpleRecord>) Types.POJO(PojoSimpleRecord.class));
		inputFormat.setRuntimeContext(getMockRuntimeContext());

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
