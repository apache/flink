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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.generated.User;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.formats.DeserializationSchemaFactory;
import org.apache.flink.table.formats.SerializationSchemaFactory;
import org.apache.flink.table.formats.TableFormatFactoryService;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link AvroRowFormatFactory}.
 */
public class AvroRowFormatFactoryTest {

	private static final Class<User> AVRO_SPECIFIC_RECORD = User.class;

	private static final String AVRO_SCHEMA = User.getClassSchema().toString();

	@Test
	public void testRecordClass() {
		final Map<String, String> props = toMap(new Avro().recordClass(AVRO_SPECIFIC_RECORD));

		// test serialization schema
		final SerializationSchema<?> actual1 = TableFormatFactoryService
			.find(SerializationSchemaFactory.class, props)
			.createSerializationSchema(props);
		final SerializationSchema<?> expected1 = new AvroRowSerializationSchema(AVRO_SPECIFIC_RECORD);
		assertEquals(expected1, actual1);

		// test deserialization schema
		final DeserializationSchema<?> actual2 = TableFormatFactoryService
			.find(DeserializationSchemaFactory.class, props)
			.createDeserializationSchema(props);
		final AvroRowDeserializationSchema expected2 = new AvroRowDeserializationSchema(AVRO_SPECIFIC_RECORD);
		assertEquals(expected2, actual2);
	}

	@Test
	public void testAvroSchema() {
		final Map<String, String> props = toMap(new Avro().avroSchema(AVRO_SCHEMA));

		// test serialization schema
		final SerializationSchema<?> actual1 = TableFormatFactoryService
			.find(SerializationSchemaFactory.class, props)
			.createSerializationSchema(props);
		final SerializationSchema<?> expected1 = new AvroRowSerializationSchema(AVRO_SCHEMA);
		assertEquals(expected1, actual1);

		// test deserialization schema
		final DeserializationSchema<?> actual2 = TableFormatFactoryService
			.find(DeserializationSchemaFactory.class, props)
			.createDeserializationSchema(props);
		final AvroRowDeserializationSchema expected2 = new AvroRowDeserializationSchema(AVRO_SCHEMA);
		assertEquals(expected2, actual2);
	}

	private Map<String, String> toMap(Descriptor... desc) {
		final DescriptorProperties props = new DescriptorProperties(true);
		for (Descriptor d : desc) {
			d.addProperties(props);
		}
		return props.asMap();
	}
}
