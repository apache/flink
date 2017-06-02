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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.testutils.AvroTestUtils;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.AvroRowSerializationSchema;
import org.apache.flink.types.Row;

import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserializeSimpleRow() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeSimpleRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testDeserializeRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSimpleTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeDeserializeComplexRow() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSerializeComplexRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testDeserializeComplexRowSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getComplexTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}
}
