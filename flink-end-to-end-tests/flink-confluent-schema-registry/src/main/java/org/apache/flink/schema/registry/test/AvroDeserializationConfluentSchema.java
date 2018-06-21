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

package org.apache.flink.schema.registry.test;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.io.IOException;

/**
 * The deserialization schema for the Avro type.
 */
public class AvroDeserializationConfluentSchema<T> implements DeserializationSchema<T> {

	private static final long serialVersionUID = 1L;
	private final Class<T> avroType;
	private final String schemaRegistryUrl;
	private final int identityMapCapacity;
	private transient KafkaAvroDeserializer kafaAvroDeserializer;
	public AvroDeserializationConfluentSchema(Class<T> avroType, String schemaRegistyUrl) {
		this(avroType, schemaRegistyUrl, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
	}

	public AvroDeserializationConfluentSchema(Class<T> avroType, String schemaRegistryUrl, int identityMapCapacity) {
		this.avroType = avroType;
		this.schemaRegistryUrl = schemaRegistryUrl;
		this.identityMapCapacity = identityMapCapacity;
	}

	@Override
	public T deserialize(byte[] message) throws IOException {
		if (kafaAvroDeserializer == null) {
			SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(this.schemaRegistryUrl, this.identityMapCapacity);
			this.kafaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
		}

		Object messageBytes = this.kafaAvroDeserializer.deserialize("payload", message);
		return (T) messageBytes;
	}

	@Override
	public boolean isEndOfStream(T avroRequest) {
		return false;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return TypeInformation.of(avroType);
	}
}
