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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.utils.DataInputDecoder;
import org.apache.flink.formats.avro.utils.DataOutputEncoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Creates Avro {@link DatumReader} and {@link DatumWriter}.
 *
 * @param <T> The type to be serialized.
 */
@Internal
final class AvroFactory<T> {

	private static final Logger LOG = LoggerFactory.getLogger(AvroFactory.class);

	private final DataOutputEncoder encoder = new DataOutputEncoder();
	private final DataInputDecoder decoder = new DataInputDecoder();

	private final GenericData avroData;
	private final Schema schema;
	private final DatumWriter<T> writer;
	private final DatumReader<T> reader;

	/**
	 * Creates Avro Writer and Reader for a specific type.
	 *
	 * <p>Given an input type, and possible the current schema, and a previously known schema (also known as writer
	 * schema) create will deduce the best way to initalize a reader and writer according to the following rules:
	 * <ul>
	 * <li>If type is an Avro generated class (an {@link SpecificRecord} then the reader would use the
	 * previousSchema for reading (if present) otherwise it would use the schema attached to the auto generated
	 * class.
	 * <li>If the type is a GenericRecord then the reader and the writer would be created with the supplied
	 * (mandatory) schema.
	 * <li>Otherwise, we use Avro's reflection based reader and writer that would deduce the schema via reflection.
	 * If the previous schema is also present (when restoring a serializer for example) then the reader would be
	 * created with both schemas.
	 * </ul>
	 */
	static <T> AvroFactory<T> create(Class<T> type, @Nullable Schema currentSchema, @Nullable Schema previousSchema) {
		final ClassLoader cl = Thread.currentThread().getContextClassLoader();

		if (SpecificRecord.class.isAssignableFrom(type)) {
			return fromSpecific(type, cl, Optional.ofNullable(previousSchema));
		}
		if (GenericRecord.class.isAssignableFrom(type)) {
			return fromGeneric(cl, currentSchema);
		}
		return fromReflective(type, cl, Optional.ofNullable(previousSchema));
	}

	@Nullable
	static Schema parseSchemaString(@Nullable String schemaString) {
		return (schemaString == null) ? null : new Schema.Parser().parse(schemaString);
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private static <T> AvroFactory<T> fromSpecific(Class<T> type, ClassLoader cl, Optional<Schema> previousSchema) {
		SpecificData specificData = new SpecificData(cl);
		Schema newSchema = extractAvroSpecificSchema(type, specificData);

		return new AvroFactory<>(
			specificData,
			newSchema,
			new SpecificDatumReader<>(previousSchema.orElse(newSchema), newSchema, specificData),
			new SpecificDatumWriter<>(newSchema, specificData)
		);
	}

	private static <T> AvroFactory<T> fromGeneric(ClassLoader cl, Schema schema) {
		checkNotNull(schema,
			"Unable to create an AvroSerializer with a GenericRecord type without a schema");
		GenericData genericData = new GenericData(cl);

		return new AvroFactory<>(
			genericData,
			schema,
			new GenericDatumReader<>(schema, schema, genericData),
			new GenericDatumWriter<>(schema, genericData)
		);
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private static <T> AvroFactory<T> fromReflective(Class<T> type, ClassLoader cl, Optional<Schema> previousSchema) {
		ReflectData reflectData = new ReflectData(cl);
		Schema newSchema = reflectData.getSchema(type);

		return new AvroFactory<>(
			reflectData,
			newSchema,
			new ReflectDatumReader<>(previousSchema.orElse(newSchema), newSchema, reflectData),
			new ReflectDatumWriter<>(newSchema, reflectData)
		);
	}

	/**
	 * Extracts an Avro {@link Schema} from a {@link SpecificRecord}. We do this either via {@link
	 * SpecificData} or by instantiating a record and extracting the schema from the instance.
	 */
	static <T> Schema extractAvroSpecificSchema(
			Class<T> type,
			SpecificData specificData) {
		Optional<Schema> newSchemaOptional = tryExtractAvroSchemaViaInstance(type);
		return newSchemaOptional.orElseGet(() -> specificData.getSchema(type));
	}

	/**
	 * Extracts an Avro {@link Schema} from a {@link SpecificRecord}. We do this by creating an
	 * instance of the class using the zero-argument constructor and calling {@link
	 * SpecificRecord#getSchema()} on it.
	 */
	@SuppressWarnings("unchecked")
	private static Optional<Schema> tryExtractAvroSchemaViaInstance(Class<?> type) {
		try {
			SpecificRecord instance = (SpecificRecord) type.newInstance();
			return Optional.ofNullable(instance.getSchema());
		} catch (InstantiationException | IllegalAccessException e) {
			LOG.warn(
					"Could not extract schema from Avro-generated SpecificRecord class {}: {}.",
					type,
					e);
			return Optional.empty();
		}
	}

	private AvroFactory(
		GenericData avroData,
		Schema schema,
		DatumReader<T> reader,
		DatumWriter<T> writer) {

		this.avroData = checkNotNull(avroData);
		this.schema = checkNotNull(schema);
		this.writer = checkNotNull(writer);
		this.reader = checkNotNull(reader);
	}

	DataOutputEncoder getEncoder() {
		return encoder;
	}

	DataInputDecoder getDecoder() {
		return decoder;
	}

	Schema getSchema() {
		return schema;
	}

	DatumWriter<T> getWriter() {
		return writer;
	}

	DatumReader<T> getReader() {
		return reader;
	}

	GenericData getAvroData() {
		return avroData;
	}
}
