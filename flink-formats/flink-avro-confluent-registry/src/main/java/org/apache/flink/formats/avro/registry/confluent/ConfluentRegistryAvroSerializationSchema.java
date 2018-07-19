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

import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;

/**
 * Serialization schema that serializes from Avro binary format that uses
 * Confluent Schema Registry.
 *
 * @param <T> type of record it produces
 */
public class ConfluentRegistryAvroSerializationSchema<T> extends RegistryAvroSerializationSchema<T> {

	private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

	private static final long serialVersionUID = -1771641202177852775L;

	/**
	 * Creates a Avro Serialization schema.
	 *
	 * @param recordClazz         class to which serialize which is
	 *                            {@link SpecificRecord}.
	 * @param schemaCoderProvider  provider for schema coder that writes schema from Confluent Schema Registry
	 */
	private ConfluentRegistryAvroSerializationSchema(Class<T> recordClazz, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(recordClazz, schemaCoderProvider);
	}

	/**
	 * Creates {@link AvroSerializationSchema} that produces classes that were generated from avro
	 * schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param tClass              class of record to be produced
	 * @param subject             subject of schema registry to produce
	 * @param schemaRegistryUrl   url of schema registry to connect
	 *
	 * @return Serialized record
	 */
	public static <T extends SpecificRecord> ConfluentRegistryAvroSerializationSchema<T> forSpecific(Class<T> tClass, String subject, String schemaRegistryUrl) {
		return new ConfluentRegistryAvroSerializationSchema<>(
			tClass,
			new CachedSchemaCoderProvider(subject, schemaRegistryUrl, DEFAULT_IDENTITY_MAP_CAPACITY)
		);
	}

	private static class CachedSchemaCoderProvider implements SchemaCoder.SchemaCoderProvider {

		private static final long serialVersionUID = 4023134423033312666L;
		private final String subject;
		private final String url;
		private final int identityMapCapacity;

		CachedSchemaCoderProvider(String subject, String url, int identityMapCapacity) {
			this.subject = subject;
			this.url = url;
			this.identityMapCapacity = identityMapCapacity;
		}

		@Override
		public SchemaCoder get() {
			return new ConfluentSchemaRegistryCoder(subject, new CachedSchemaRegistryClient(
				url,
				identityMapCapacity));
		}

	}
}
