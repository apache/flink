/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.registry.confluent.util.AvroTestUtils;
import org.apache.flink.types.Row;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConfluentRegistryAvroRowDeserializationSchemaTest}.
 */
public class ConfluentRegistryAvroRowDeserializationSchemaTest {
	@Test
	public void testDeserializeSpecific() throws IOException, RestClientException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		Class<? extends SpecificRecord> recordClass = testData.f0;
		SpecificRecord record = testData.f1;
		Row expected = testData.f2;

		ConfluentRegistryAvroRowDeserializationSchema deser = ConfluentRegistryAvroRowDeserializationSchema
			.forSpecific(recordClass, "localhost");

		MockSchemaRegistryClient client = new MockSchemaRegistryClient();
		int id = client.register("subj", record.getSchema());

		ConfluenceCachedSchemaCoderProvider mockSchemaCoderProvider = mock(ConfluenceCachedSchemaCoderProvider.class);
		when(mockSchemaCoderProvider.get()).thenReturn(new ConfluentSchemaRegistryCoder(client));

		deser.innerDeserializationSchema = new ConfluentRegistryAvroDeserializationSchema<>(recordClass, null,
			mockSchemaCoderProvider);

		Row actual = deser.deserialize(AvroTestUtils.serialize(testData.f1, id));

		assertEquals(expected, actual);
	}

	@Test
	public void testDeserializeGeneric() throws IOException, RestClientException {
		Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();
		GenericRecord record = testData.f0;
		Row expected = testData.f1;
		Schema schema = testData.f2;

		ConfluentRegistryAvroRowDeserializationSchema deser = ConfluentRegistryAvroRowDeserializationSchema
			.forGeneric(schema, "localhost");

		MockSchemaRegistryClient client = new MockSchemaRegistryClient();
		int id = client.register("subj", schema);

		ConfluenceCachedSchemaCoderProvider mockSchemaCoderProvider = mock(ConfluenceCachedSchemaCoderProvider.class);
		when(mockSchemaCoderProvider.get()).thenReturn(new ConfluentSchemaRegistryCoder(client));

		deser.innerDeserializationSchema = new ConfluentRegistryAvroDeserializationSchema<>(GenericRecord.class, schema,
			mockSchemaCoderProvider);

		Row actual = deser.deserialize(AvroTestUtils.serialize(record, id));

		assertEquals(expected, actual);
	}

	@Test
	public void testReadWriteObjectGeneric() throws IOException, ClassNotFoundException {
		Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();
		Schema schema = testData.f2;

		ConfluentRegistryAvroRowDeserializationSchema deser = ConfluentRegistryAvroRowDeserializationSchema
			.forGeneric(schema, "localhost");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		deser.writeObject(oos);

		oos.flush();

		ConfluentRegistryAvroRowDeserializationSchema mock = spy(ConfluentRegistryAvroRowDeserializationSchema.class);
		mock.readObject(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));

		assertEquals(deser.schemaString, mock.schemaString);
		assertEquals(deser.recordClazz, mock.recordClazz);
	}

	@Test
	public void testReadWriteObjectSpecific() throws IOException, ClassNotFoundException {
		final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData = AvroTestUtils.getSpecificTestData();
		Class<? extends SpecificRecord> recordClass = testData.f0;

		ConfluentRegistryAvroRowDeserializationSchema deser = ConfluentRegistryAvroRowDeserializationSchema
			.forSpecific(recordClass, "localhost");

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		deser.writeObject(oos);

		oos.flush();

		ConfluentRegistryAvroRowDeserializationSchema mock = spy(ConfluentRegistryAvroRowDeserializationSchema.class);
		mock.readObject(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));

		assertEquals(deser.schemaString, mock.schemaString);
		assertEquals(deser.recordClazz, mock.recordClazz);
	}
}
