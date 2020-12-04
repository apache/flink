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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

/**
 * Deserialization schema that deserializes from Avro binary format using {@link SchemaCoder} that uses
 * Confluent Schema Registry.
 *
 * @param <T> type of record it produces
 */
public class ConfluentRegistryAvroDeserializationSchema<T> extends RegistryAvroDeserializationSchema<T> {

	private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	private static final long serialVersionUID = -1671641202177852775L;

	/**
	 * Creates a Avro deserialization schema.
	 *
	 * @param recordClazz         class to which deserialize. Should be either
	 *                            {@link SpecificRecord} or {@link GenericRecord}.
	 * @param reader              reader's Avro schema. Should be provided if recordClazz is
	 *                            {@link GenericRecord}
	 * @param schemaCoderProvider provider for schema coder that reads writer schema from Confluent Schema Registry
	 */
	private ConfluentRegistryAvroDeserializationSchema(Class<T> recordClazz, @Nullable Schema reader,
			SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(recordClazz, reader, schemaCoderProvider);
	}

	/**
	 * Creates {@link ConfluentRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
	 * using provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param schema schema of produced records
	 * @param url    url of schema registry to connect
	 * @return deserialized record in form of {@link GenericRecord}
	 */
	public static ConfluentRegistryAvroDeserializationSchema<GenericRecord> forGeneric(Schema schema, String url) {
		return forGeneric(schema, url, DEFAULT_IDENTITY_MAP_CAPACITY);
	}

	/**
	 * Creates {@link ConfluentRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
	 * using provided reader schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param schema              schema of produced records
	 * @param url                 url of schema registry to connect
	 * @param identityMapCapacity maximum number of cached schema versions (default: 1000)
	 * @return deserialized record in form of {@link GenericRecord}
	 */
	public static ConfluentRegistryAvroDeserializationSchema<GenericRecord> forGeneric(Schema schema, String url,
			int identityMapCapacity) {
		return new ConfluentRegistryAvroDeserializationSchema<>(
			GenericRecord.class,
			schema,
			new CachedSchemaCoderProvider(url, identityMapCapacity));
	}

	/**
	 * Creates {@link AvroDeserializationSchema} that produces classes that were generated from avro
	 * schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param tClass class of record to be produced
	 * @param url    url of schema registry to connect
	 * @return deserialized record
	 */
	public static <T extends SpecificRecord> ConfluentRegistryAvroDeserializationSchema<T> forSpecific(Class<T> tClass,
			String url) {
		return forSpecific(tClass, url, DEFAULT_IDENTITY_MAP_CAPACITY);
	}

	/**
	 * Creates {@link AvroDeserializationSchema} that produces classes that were generated from avro
	 * schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param tClass              class of record to be produced
	 * @param url                 url of schema registry to connect
	 * @param identityMapCapacity maximum number of cached schema versions (default: 1000)
	 * @return deserialized record
	 */
	public static <T extends SpecificRecord> ConfluentRegistryAvroDeserializationSchema<T> forSpecific(Class<T> tClass,
			String url, int identityMapCapacity) {
		return new ConfluentRegistryAvroDeserializationSchema<>(
			tClass,
			null,
			new CachedSchemaCoderProvider(url, identityMapCapacity)
		);
	}
}
