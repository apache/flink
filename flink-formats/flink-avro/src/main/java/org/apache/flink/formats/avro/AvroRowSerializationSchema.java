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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Serialization schema that serializes {@link Row} over {@link SpecificRecord} into a Avro bytes.
 */
public class AvroRowSerializationSchema implements SerializationSchema<Row> {

	/**
	 * Avro record class.
	 */
	private Class<? extends SpecificRecord> recordClazz;

	/**
	 * Avro serialization schema.
	 */
	private transient Schema schema;

	/**
	 * Writer to serialize Avro record into a byte array.
	 */
	private transient DatumWriter<GenericRecord> datumWriter;

	/**
	 * Output stream to serialize records into byte array.
	 */
	private transient ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();

	/**
	 * Low-level class for serialization of Avro values.
	 */
	private transient Encoder encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);

	/**
	 * Creates a Avro serialization schema for the given schema.
	 *
	 * @param recordClazz Avro record class used to deserialize Avro's record to Flink's row
	 */
	public AvroRowSerializationSchema(Class<? extends SpecificRecord> recordClazz) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.datumWriter = new SpecificDatumWriter<>(schema);
	}

	@Override
	@SuppressWarnings("unchecked")
	public byte[] serialize(Row row) {
		// convert to record
		final Object record = convertToRecord(schema, row);

		// write
		try {
			arrayOutputStream.reset();
			datumWriter.write((GenericRecord) record, encoder);
			encoder.flush();
			return arrayOutputStream.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize Row.", e);
		}
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.writeObject(recordClazz);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		this.recordClazz = (Class<? extends SpecificRecord>) ois.readObject();
		this.schema = SpecificData.get().getSchema(recordClazz);
		this.datumWriter = new SpecificDatumWriter<>(schema);
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().binaryEncoder(arrayOutputStream, null);
	}

	/**
	 * Converts a (nested) Flink Row into Avro's {@link GenericRecord}.
	 * Strings are converted into Avro's {@link Utf8} fields.
	 */
	private static Object convertToRecord(Schema schema, Object rowObj) {
		if (rowObj instanceof Row) {
			// records can be wrapped in a union
			if (schema.getType() == Schema.Type.UNION) {
				final List<Schema> types = schema.getTypes();
				if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL && types.get(1).getType() == Schema.Type.RECORD) {
					schema = types.get(1);
				}
				else if (types.size() == 2 && types.get(0).getType() == Schema.Type.RECORD && types.get(1).getType() == Schema.Type.NULL) {
					schema = types.get(0);
				}
				else {
					throw new RuntimeException("Currently we only support schemas of the following form: UNION[null, RECORD] or UNION[RECORD, NULL] Given: " + schema);
				}
			} else if (schema.getType() != Schema.Type.RECORD) {
				throw new RuntimeException("Record type for row type expected. But is: " + schema);
			}
			final List<Schema.Field> fields = schema.getFields();
			final GenericRecord record = new GenericData.Record(schema);
			final Row row = (Row) rowObj;
			for (int i = 0; i < fields.size(); i++) {
				final Schema.Field field = fields.get(i);
				record.put(field.pos(), convertToRecord(field.schema(), row.getField(i)));
			}
			return record;
		} else if (rowObj instanceof String) {
			return new Utf8((String) rowObj);
		} else {
			return rowObj;
		}
	}
}
