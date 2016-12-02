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
package org.apache.flink.streaming.util.serialization;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Deserialization schema from Avro to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages in Avro format and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class AvroRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

	/** Field names in a row */
	private final String[] fieldNames;
	/** Types to parse fields as. Indices match fieldNames indices. */
	private final TypeInformation[] fieldTypes;
	/** Avro deserialization schema */
	private final Schema schema;
	/** Reader that deserializes byte array into a record */
	private final DatumReader<GenericRecord> datumReader;
	/** Record to deserialize byte array to */
	private final GenericRecord record;

	/**
	 * Creates a Avro deserializtion schema for the given type classes.
	 *
	 * @param fieldNames Field names to parse Avro fields as.
	 * @param fieldTypes Type classes to parse Avro fields as.
	 * @param schema Avro schema used to deserialize Row record
	 */
	public AvroRowDeserializationSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes, Schema schema) {
		this.schema = schema;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.datumReader = new ReflectDatumReader<>(schema);
		this.record = new GenericData.Record(schema);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		readRecord(message);
		return convertRecordToRow();
	}

	private void readRecord(byte[] message) throws IOException {
		ByteArrayInputStream arrayInputStream =  new ByteArrayInputStream(message);
		Decoder decoder = DecoderFactory.get().directBinaryDecoder(arrayInputStream, null);
		datumReader.read(record, decoder);
	}

	private Row convertRecordToRow() {
		Row row = new Row(fieldNames.length);

		for (int i = 0; i < fieldNames.length; i++) {
			Object val = record.get(fieldNames[i]);
			// Avro deserializes strings into Utf8 type
			if (fieldTypes[i].getTypeClass().equals(String.class) && val != null) {
				val = val.toString();
			}
			row.setField(i, val);
		}

		return row;
	}
}
