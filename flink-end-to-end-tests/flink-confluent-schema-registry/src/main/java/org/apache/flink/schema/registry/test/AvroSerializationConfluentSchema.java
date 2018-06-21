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

import org.apache.flink.api.common.serialization.SerializationSchema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * The serialization schema for the Avro type.
 */
public class AvroSerializationConfluentSchema<T> implements SerializationSchema<T> {

	private static final long serialVersionUID = 1L;
	private final String schemaRegistryUrl;
	private final int identityMapCapacity;
	private transient KafkaAvroSerializer kafkaAvroSerializer;
	private final String topic;
	private SchemaRegistryClient schemaRegistryClient;
	public AvroSerializationConfluentSchema(String schemaRegistryUrl, String topic) {
		this(schemaRegistryUrl, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT, topic);
	}

	public AvroSerializationConfluentSchema(String schemaRegistryUrl, int identityMapCapacity, String topic) {
		this.schemaRegistryUrl = schemaRegistryUrl;
		this.identityMapCapacity = identityMapCapacity;
		this.topic = topic;
	}

	@Override
	public byte[] serialize(T obj) {
		byte[] serializedBytes = null;

		try {
			if (kafkaAvroSerializer == null) {
				this.schemaRegistryClient = new CachedSchemaRegistryClient(this.schemaRegistryUrl, this.identityMapCapacity);
				this.kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
			}
			serializedBytes = kafkaAvroSerializer.serialize(topic, obj);

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return serializedBytes;
	}
}
