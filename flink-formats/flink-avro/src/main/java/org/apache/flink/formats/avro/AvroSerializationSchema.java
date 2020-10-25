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
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Serialization schema that serializes to Avro binary format.
 *
 * @param <T> the type  to be serialized
 */
public class AvroSerializationSchema<T> implements SerializationSchema<T> {

	/**
	 * Creates {@link AvroSerializationSchema} that serializes {@link SpecificRecord} using provided schema.
	 *
	 * @param tClass the type to be serialized
	 * @return serialized record in form of byte array
	 */
	public static <T extends SpecificRecord> AvroSerializationSchema<T> forSpecific(Class<T> tClass) {
		return new AvroSerializationSchema<>(tClass, null);
	}

	/**
	 * Creates {@link AvroSerializationSchema} that serializes {@link GenericRecord} using provided schema.
	 *
	 * @param schema the schema that will be used for serialization
	 * @return serialized record in form of byte array
	 */
	public static AvroSerializationSchema<GenericRecord> forGeneric(Schema schema) {
		return new AvroSerializationSchema<>(GenericRecord.class, schema);
	}

	private static final long serialVersionUID = -8766681879020862312L;

	/**
	 * Class to serialize to.
	 */
	private Class<T> recordClazz;

	private String schemaString;
	private transient Schema schema;
	/**
	 * Writer that writes the serialized record to {@link ByteArrayOutputStream}.
	 */
	private transient GenericDatumWriter<T> datumWriter;

	/**
	 * Output stream to write message to.
	 */
	private transient ByteArrayOutputStream arrayOutputStream;

	/**
	 * Avro encoder that encodes binary data.
	 */
	private transient BinaryEncoder encoder;

	/**
	 * Creates an Avro deserialization schema.
	 *
	 * @param recordClazz class to serialize. Should be one of:
	 *                    {@link org.apache.avro.specific.SpecificRecord},
	 *                    {@link org.apache.avro.generic.GenericRecord}.
	 * @param schema      writer Avro schema. Should be provided if recordClazz is
	 *                    {@link GenericRecord}
	 */
	protected AvroSerializationSchema(Class<T> recordClazz, @Nullable Schema schema) {
		Preconditions.checkNotNull(recordClazz, "Avro record class must not be null.");
		this.recordClazz = recordClazz;
		this.schema = schema;
		if (schema != null) {
			this.schemaString = schema.toString();
		} else {
			this.schemaString = null;
		}
	}

	public Schema getSchema() {
		return schema;
	}

	protected BinaryEncoder getEncoder() {
		return encoder;
	}

	protected GenericDatumWriter<T> getDatumWriter() {
		return datumWriter;
	}

	protected ByteArrayOutputStream getOutputStream() {
		return arrayOutputStream;
	}

	@Override
	public byte[] serialize(T object) {
		checkAvroInitialized();

		if (object == null) {
			return null;
		} else {
			try {
				datumWriter.write(object, encoder);
				encoder.flush();
				byte[] bytes = arrayOutputStream.toByteArray();
				arrayOutputStream.reset();
				return bytes;
			} catch (IOException e) {
				throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
			}

		}
	}

	protected void checkAvroInitialized() {
		if (datumWriter != null) {
			return;
		}
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
			if (SpecificRecord.class.isAssignableFrom(recordClazz)) {
			Schema schema = SpecificData.get().getSchema(recordClazz);
			this.datumWriter = new SpecificDatumWriter<>(schema);
			this.schema = schema;
		} else {
			this.schema = new Schema.Parser().parse(this.schemaString);
			GenericData genericData = new GenericData(cl);

			this.datumWriter = new GenericDatumWriter<>(schema, genericData);
		}
		this.arrayOutputStream = new ByteArrayOutputStream();
		this.encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AvroSerializationSchema<?> that = (AvroSerializationSchema<?>) o;
		return recordClazz.equals(that.recordClazz) &&
				Objects.equals(schema, that.schema);
	}

	@Override
	public int hashCode() {
		return Objects.hash(recordClazz, schema);
	}
}
