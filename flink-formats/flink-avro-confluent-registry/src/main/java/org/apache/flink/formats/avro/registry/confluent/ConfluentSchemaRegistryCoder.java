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

import org.apache.flink.formats.avro.SchemaCoder;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * Reads schema using Confluent Schema Registry protocol.
 */
public class ConfluentSchemaRegistryCoder implements SchemaCoder {

	private final SchemaRegistryClient schemaRegistryClient;

	/**
	 * Creates {@link SchemaCoder} that uses provided {@link SchemaRegistryClient} to connect to
	 * schema registry.
	 *
	 * @param schemaRegistryClient client to connect schema registry
	 */
	public ConfluentSchemaRegistryCoder(SchemaRegistryClient schemaRegistryClient) {
		this.schemaRegistryClient = schemaRegistryClient;
	}

	@Override
	public Schema readSchema(InputStream in) throws Exception {
		DataInputStream dataInputStream = new DataInputStream(in);

		if (dataInputStream.readByte() != 0) {
			throw new RuntimeException("Unknown data format. Magic number does not match");
		} else {
			int schemaId = dataInputStream.readInt();

			return schemaRegistryClient.getById(schemaId);
		}
	}

}
