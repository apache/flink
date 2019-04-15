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

package org.apache.flink.formats.avro.registry.confluent.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Confluent compatible avro utils.
 */
public class AvroTestUtils {
	public static Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> getSpecificTestData() {
		final Address addr = Address.newBuilder()
			.setNum(42)
			.setStreet("Main Street 42")
			.setCity("Test City")
			.setState("Test State")
			.setZip("12345")
			.build();

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		return new Tuple3<>(Address.class, addr, rowAddr);
	}

	public static Tuple3<GenericRecord, Row, Schema> getGenericTestData() {
		String schemaString = "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"org.apache.flink.formats.avro.generated\",\"fields\":" +
			"[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"street\",\"type\":\"string\"}," +
			"{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"state\",\"type\":\"string\"}," +
			"{\"name\":\"zip\",\"type\":\"string\"}]}";

		final Schema schema = new Schema.Parser().parse(schemaString);

		GenericRecord addr = new GenericData.Record(schema);
		addr.put("num", 42);
		addr.put("street", "Main Street 42");
		addr.put("city", "Test City");
		addr.put("state", "Test State");
		addr.put("zip", "12345");

		final Row rowAddr = new Row(5);
		rowAddr.setField(0, 42);
		rowAddr.setField(1, "Main Street 42");
		rowAddr.setField(2, "Test City");
		rowAddr.setField(3, "Test State");
		rowAddr.setField(4, "12345");

		return new Tuple3<>(addr, rowAddr, schema);
	}

	public static byte[] serialize(IndexedRecord record, int version) throws IOException {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DataOutputStream dataStream = new DataOutputStream(stream);
		BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(dataStream, null);
		GenericDatumWriter<Object> datumWriter = new GenericDatumWriter<>(record.getSchema());

		dataStream.write(0);
		dataStream.writeInt(version);
		datumWriter.write(record, encoder);

		encoder.flush();
		return stream.toByteArray();
	}
}
