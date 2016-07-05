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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.streaming.util.serialization.AvroSerializationSchema;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import static javafx.scene.input.KeyCode.T;
import static org.junit.Assert.assertEquals;

public class AvroSerializationSchemaTest {
//	@Test
//	public void serializeValue() throws IOException {
//		Schema schema = ReflectData.get().getSchema(TestType.class);
//		AvroSerializationSchema<TestType> serializationSchema = new AvroSerializationSchema<TestType>(TestType.class);
//
//		TestType inputValue = new TestType("key", "value");
//		byte[] serializedKey = serializationSchema.serialize(inputValue);
//		TestType resultValue = deserialize(schema, serializedKey);
//		assertEquals(inputValue, resultValue);
//	}

	private TestType deserialize(Schema schema, byte[] serializedKey) throws IOException {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedKey);
		ReflectDatumReader<TestType> reflectDatumReader = new ReflectDatumReader<TestType>(schema);
		DataFileStream<TestType> reader = new DataFileStream<TestType>(inputStream, reflectDatumReader);
		TestType key = reader.next();
		reader.close();
		inputStream.close();
		return key;
	}

	@Test
	public void test() throws IOException {

		TestObject testObject = new TestObject();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<TestObject> writer = new ReflectDatumWriter<TestObject>(TestObject.class);
		final Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

		writer.write(testObject, encoder);
		out.close();
		byte[] res = out.toByteArray();
	}
}

class TestObject {
//	Object[] arr = new Object[] {1, "str", false};
	String str = "str";
}

class TestType {
	private String key;
	private String value;

	// To make Avro happy
	public TestType() {}

	public TestType(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TestType testType = (TestType) o;
		return Objects.equals(key, testType.key) &&
			Objects.equals(value, testType.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}
}
