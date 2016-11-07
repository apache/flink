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
package org.apache.flink.streaming.util.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.flink.streaming.connectors.kafka.internals.TypeUtil.createRowAvroSchema;

/**
 * Serialization schema that serializes an object into a Avro bytes.
 * <p>
 */
public class AvroRowSerializationSchema implements SerializationSchema<Row> {

	/** Field names in a Row */
	private final String[] fieldNames;
	/** Avro serialization schema */
	private final Schema schema;
	/** Writer to serialize Avro GeneralRecord into a byte array */
	private final DatumWriter<GenericRecord> datumWriter;
	/** Output stream to serialize records into byte array */
	private final ByteArrayOutputStream arrayOutputStream =  new ByteArrayOutputStream();
	/** Low level class for serialization of Avro values */
	private final Encoder encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
	/** Record that Avro serializes into a byte array */
	private final GenericRecord record;

	public AvroRowSerializationSchema(String[] fieldNames, TypeInformation[] types) {
		this.fieldNames = fieldNames;
		this.schema = createRowAvroSchema(fieldNames, types);
		this.datumWriter = new GenericDatumWriter<>(schema);
		this.record = new GenericData.Record(schema);
	}

	@Override
	public byte[] serialize(Row row) {
		copyToRecord(row);
		return convertRecordToBytes();
	}

	private byte[] convertRecordToBytes() {
		try {
			arrayOutputStream.reset();
			datumWriter.write(record, encoder);
			encoder.flush();
			return arrayOutputStream.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize Row", e);
		}
	}

	private void copyToRecord(Row row) {
		for (int i = 0; i < fieldNames.length; i++) {
			record.put(fieldNames[i], row.productElement(i));
		}
	}
}
