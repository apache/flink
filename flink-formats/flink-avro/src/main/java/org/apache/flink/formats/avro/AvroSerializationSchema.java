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
import org.apache.flink.util.Preconditions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Serialization schema that serializes from Avro binary format.
 *
 * @param <T> type of record it produces
 */
public class AvroSerializationSchema<T> implements SerializationSchema<T> {

	/**
	 * Creates {@link AvroSerializationSchema} that produces {@link SpecificRecord} using provided schema.
	 *
	 * @param schemaId   id of schema registry
	 * @return topic record in form of {@link SpecificRecord}
	 */
	public static <T extends SpecificRecord> AvroSerializationSchema<T> forSpecific(Class<T> tClass,
																					int schemaId) {
		return new AvroSerializationSchema<>(tClass, schemaId);
	}

	private static final long serialVersionUID = -8766681879020862312L;

	/**
	 * Class to serialize from.
	 */
	private Class<T> recordClazz;

	/**
	 * Write the serializes byte array into a record.
	 */
	private transient GenericDatumWriter<T> datumWriter;

	/**
	 * Output stream to write message to.
	 */
	private transient ByteArrayOutputStream arrayOutputStream;

	/**
	 * Avro decoder that decodes binary data.
	 */
	private transient BinaryEncoder encoder;

	/**
	 * schema id.
	 */
	private int schemaId;

	/**
	 * Creates Avro Serialization schema.
	 *
	 * @param recordClazz         class to which deserialize which is
	 *                            {@link SpecificRecord}.
	 * @param schemaId   id of schema registry to connect
	 */

	AvroSerializationSchema(Class<T> recordClazz, int schemaId) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.schemaId = schemaId;

	}

	BinaryEncoder getEncoder() {
		return encoder;
	}

	GenericDatumWriter<T> getDatumWriter() {
		return datumWriter;
	}

	ByteArrayOutputStream getOutputStream() {
		return arrayOutputStream;
	}

	int getSchemaId(){
		return schemaId;
	}

	@Override
	public byte[] serialize(T object) {
		checkAvroInitialized();

		if (object == null) {
			return null;
		} else {
			try {
				arrayOutputStream.write(0);
				arrayOutputStream.write(ByteBuffer.allocate(4).putInt(schemaId).array());
				datumWriter.write(object, encoder);
				encoder.flush();
				byte[] bytes = arrayOutputStream.toByteArray();
				arrayOutputStream.close();
				return bytes;
			} catch (RuntimeException | IOException e) {
				throw new RuntimeException("Failed to serialize schema registry.", e);
			}

		}
	}

	void checkAvroInitialized() {
		if (datumWriter != null) {
			return;
		}
		if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			Schema schema = SpecificData.get().getSchema(recordClazz);
			this.datumWriter = new SpecificDatumWriter<>(schema);

		} else {
			this.datumWriter = new GenericDatumWriter<>();
		}
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
	}
}
