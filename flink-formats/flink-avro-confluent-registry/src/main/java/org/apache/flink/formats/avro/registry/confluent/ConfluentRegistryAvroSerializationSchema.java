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
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;

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
	 * @param recordClazz         class to which Serialize which is
	 *                            {@link SpecificRecord}.
	 * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder} that will be used for
	 *                            schema writing
	 */
	private ConfluentRegistryAvroSerializationSchema(Class<T> recordClazz, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(recordClazz, schemaCoderProvider);
	}

	/**
	 * Creates {@link AvroSerializationSchema} that produces classes that were generated from avro
	 * schema and looks up writer schema in Confluent Schema Registry.
	 *
	 * @param tClass              class of record to be produced
	 * @param schemaRegistryUrl   url of schema registry to connect
	 * @param subject subject of schema registry to produce
	 * @return Serialized record
	 */
	public static <T extends SpecificRecord> ConfluentRegistryAvroSerializationSchema<T> forSpecific(Class<T> tClass, String subject, String schemaRegistryUrl) {
		return new ConfluentRegistryAvroSerializationSchema<>(
			tClass,
			new CachedSchemaCoderProvider(tClass, subject, schemaRegistryUrl, DEFAULT_IDENTITY_MAP_CAPACITY)
		);
	}

	private static class CachedSchemaCoderProvider implements SchemaCoder.SchemaCoderProvider {

		private static final long serialVersionUID = 4023134423033312666L;
		private final String url;
		private final int identityMapCapacity;
		private final Class tClass;
		private final String subject;

		CachedSchemaCoderProvider(Class tClass, String subject, String url, int identityMapCapacity) {
			this.url = url;
			this.identityMapCapacity = identityMapCapacity;
			this.tClass = tClass;
			this.subject = subject;
		}

		@Override
		public SchemaCoder get() {
			return new ConfluentSchemaRegistryCoder(new CachedSchemaRegistryClient(
				url,
				identityMapCapacity));
		}

		@Override
		public Object getSchemaId() {
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(url, identityMapCapacity);
			int schemaId;
			try {
				schemaId = cachedSchemaRegistryClient.register(subject, SpecificData.get().getSchema(tClass));
			} catch (IOException | RestClientException e) {
				throw new RuntimeException("Failed to serialize schema registry.", e);
			}
			return schemaId;
		}

	}
}
