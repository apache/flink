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

import org.apache.flink.api.common.serialization.SerializationSchema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * The serialization schema for the Avro type.
 */
public class AvroSerializationConfluentSchema<T> implements SerializationSchema<T> {

	private static final long serialVersionUID = 1L;

	private final String schemaRegistryUrl;

	private final int identityMapCapacity;

	private KafkaAvroSerializer kafkaAvroSerializer;

	private String topicName;

	private SchemaRegistryClient schemaRegistryClient;

	private JsonAvroConverter jsonAvroConverter;

	private final Class<T> avroType;

	public AvroSerializationConfluentSchema(Class<T> avroType, String schemaRegistryUrl, String topicName) {
		this(avroType, schemaRegistryUrl, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT, topicName);
	}

	public AvroSerializationConfluentSchema(Class<T> avroType, String schemaRegistryUrl, int identityMapCapacity, String topicName) {
		this.schemaRegistryUrl = schemaRegistryUrl;
		this.identityMapCapacity = identityMapCapacity;
		this.topicName = topicName;
		this.avroType = avroType;
	}

	@Override
	public byte[] serialize(T obj) {
		byte[] serializedBytes = null;

		try {
			if (kafkaAvroSerializer == null) {
				this.schemaRegistryClient = new CachedSchemaRegistryClient(this.schemaRegistryUrl, this.identityMapCapacity);
				this.kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
			}

			String schema = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value").getSchema();

			if (jsonAvroConverter == null) {
				jsonAvroConverter = new JsonAvroConverter();
			}

			//System.out.println("Schema fetched from Schema Registry for topic :" + topicName + " = " + schema);
			GenericData.Record record = jsonAvroConverter.convertToGenericDataRecord(obj.toString().getBytes(), new Schema.Parser().parse(schema));

			if (GenericData.get().validate(new Schema.Parser().parse(schema), record)) {
				serializedBytes = kafkaAvroSerializer.serialize(topicName, record);

			} else {
				System.out.println("Error :Invalid message : Doesn't follow the avro schema : Message not published to the topic, message = " + record.toString());

			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return serializedBytes;
	}
}
