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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDeSerializationSchemaTest {

	@Test
	public void testSpecificSerializeDeserializeFromClass() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSpecificSerializeDeserializeFromSchema() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final String schemaString = testData.f1.getSchema().toString();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(schemaString);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(schemaString);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testGenericSerializeDeserialize() throws IOException {
		final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f2.toString());
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f2.toString());

		final byte[] bytes = serializationSchema.serialize(testData.f1);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}

	@Test
	public void testSpecificSerializeFromClassSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSpecificSerializeFromSchemaSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final String schemaString = testData.f1.getSchema().toString();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(schemaString);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(schemaString);

		serializationSchema.serialize(testData.f2);
		serializationSchema.serialize(testData.f2);
		final byte[] bytes = serializationSchema.serialize(testData.f2);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testGenericSerializeSeveralTimes() throws IOException {
		final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f2.toString());
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f2.toString());

		serializationSchema.serialize(testData.f1);
		serializationSchema.serialize(testData.f1);
		final byte[] bytes = serializationSchema.serialize(testData.f1);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}

	@Test
	public void testSpecificDeserializeFromClassSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f0);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testSpecificDeserializeFromSchemaSeveralTimes() throws IOException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final String schemaString = testData.f1.getSchema().toString();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(schemaString);
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(schemaString);

		final byte[] bytes = serializationSchema.serialize(testData.f2);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f2, actual);
	}

	@Test
	public void testGenericDeserializeSeveralTimes() throws IOException {
		final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

		final AvroRowSerializationSchema serializationSchema = new AvroRowSerializationSchema(testData.f2.toString());
		final AvroRowDeserializationSchema deserializationSchema = new AvroRowDeserializationSchema(testData.f2.toString());

		final byte[] bytes = serializationSchema.serialize(testData.f1);
		deserializationSchema.deserialize(bytes);
		deserializationSchema.deserialize(bytes);
		final Row actual = deserializationSchema.deserialize(bytes);

		assertEquals(testData.f1, actual);
	}

	@Test
	public void testSerializability() throws Exception {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		final String schemaString = testData.f1.getSchema().toString();

		// from class
		final AvroRowSerializationSchema classSer = new AvroRowSerializationSchema(testData.f0);
		final AvroRowDeserializationSchema classDeser = new AvroRowDeserializationSchema(testData.f0);
		testSerializability(classSer, classDeser, testData.f2);

		// from schema string
		final AvroRowSerializationSchema schemaSer = new AvroRowSerializationSchema(schemaString);
		final AvroRowDeserializationSchema schemaDeser = new AvroRowDeserializationSchema(schemaString);
		testSerializability(schemaSer, schemaDeser, testData.f2);
	}

	@Test
	public void testSchemaAddColumn() throws Exception {
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> oldData = AvroTestUtils.getOldRecordData();
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> newData0 = AvroTestUtils.getNewRecordData(102L, "abc");
		Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> newData1 = AvroTestUtils.getNewRecordData(104L, "def");

		AvroRowSerializationSchema oldClassSer = new AvroRowSerializationSchema(oldData.f0);
		AvroRowSerializationSchema newClassSer = new AvroRowSerializationSchema(newData0.f0);

		AvroRowDeserializationSchema classDeser = new AvroRowDeserializationSchema(oldData.f0);

		byte[] data0 = oldClassSer.serialize(oldData.f2);
		byte[] data1 = newClassSer.serialize(newData0.f2);
		byte[] data2 = newClassSer.serialize(newData1.f2);

		Row row0 = classDeser.deserialize(data0);
		Row row1 = classDeser.deserialize(data1);
		Row row2 = classDeser.deserialize(data2);

		Assert.assertEquals(oldData.f2, row0);
		Assert.assertEquals(Row.of(102L), row1);
		Assert.assertEquals(Row.of(104L), row2);
	}

	private void testSerializability(AvroRowSerializationSchema ser, AvroRowDeserializationSchema deser, Row data) throws Exception {
		final byte[] serBytes = InstantiationUtil.serializeObject(ser);
		final byte[] deserBytes = InstantiationUtil.serializeObject(deser);

		final AvroRowSerializationSchema serCopy =
			InstantiationUtil.deserializeObject(serBytes, Thread.currentThread().getContextClassLoader());
		final AvroRowDeserializationSchema deserCopy =
			InstantiationUtil.deserializeObject(deserBytes, Thread.currentThread().getContextClassLoader());

		final byte[] bytes = serCopy.serialize(data);
		deserCopy.deserialize(bytes);
		deserCopy.deserialize(bytes);
		final Row actual = deserCopy.deserialize(bytes);

		assertEquals(data, actual);
	}
}
