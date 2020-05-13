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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDataDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserialize() throws IOException {
		final Tuple2<String, RowData> testData = AvroTestUtils.getGenericTestRowData();

		final RowType rowType = (RowType) AvroSchemaConverter.convertToLogicalType(testData.f0);
		final TypeInformation<RowData> typeInfo = new RowDataTypeInfo(rowType);

		final AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		final AvroRowDataDeserializationSchema deserializationSchema =
			new AvroRowDataDeserializationSchema(rowType, typeInfo);

		final byte[] bytes = serializationSchema.serialize(testData.f1);
		final RowData actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}

	@Test
	public void testSerializeSeveralTimes() throws IOException {
		final Tuple2<String, RowData> testData = AvroTestUtils.getGenericTestRowData();

		final RowType rowType = (RowType) AvroSchemaConverter.convertToLogicalType(testData.f0);
		final TypeInformation<RowData> typeInfo = new RowDataTypeInfo(rowType);

		final AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		final AvroRowDataDeserializationSchema deserializationSchema =
			new AvroRowDataDeserializationSchema(rowType, typeInfo);

		serializationSchema.serialize(testData.f1);
		serializationSchema.serialize(testData.f1);
		final byte[] bytes = serializationSchema.serialize(testData.f1);
		final RowData actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}

	@Test
	public void testDeserializeSeveralTimes() throws IOException {
		final Tuple2<String, RowData> testData = AvroTestUtils.getGenericTestRowData();

		final RowType rowType = (RowType) AvroSchemaConverter.convertToLogicalType(testData.f0);
		final TypeInformation<RowData> typeInfo = new RowDataTypeInfo(rowType);

		final AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		final AvroRowDataDeserializationSchema deserializationSchema =
			new AvroRowDataDeserializationSchema(rowType, typeInfo);

		final byte[] bytes = serializationSchema.serialize(testData.f1);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final RowData actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}
}
