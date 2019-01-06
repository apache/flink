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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.pojo.PojoSimpleRecord;
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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for reading Pojo from Parquet files.
 */
public class ParquetPojoInputFormatTest {
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	@ClassRule
	public static TemporaryFolder temp = new TemporaryFolder();

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

}
