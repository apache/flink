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


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Serialization schema that serializes instances of {@link GenericRecord} to Avro binary format using {@link KafkaAvroSerializer} that uses
 * Confluent Schema Registry.
 */
public class ConfluentAvroSerializationSchemaForGenericRecord
        implements SerializationSchema<GenericRecord> {

    private final String topic;
    private final File schemaFile;
    private final String registryURL;
    private transient Schema externalSchema;
    private transient KafkaAvroSerializer encoder;

    /**
     * Creates a Avro serialization schema.
     * The constructor takes files instead of {@link Schema} because {@link Schema} is not serializable.
     *
     * @param topic             Kafka topic to write to
     * @param registryURL       url of schema registry to connect
     * @param schemaFile     file of the Avro writer schema
     */
    public ConfluentAvroSerializationSchemaForGenericRecord(String topic, String registryURL, File schemaFile) {
        this.topic = topic;
        this.registryURL =registryURL;
        this.schemaFile = schemaFile;
    }

    /**
     * Serializes the input record.
     *
     * @param element   input record
     * @return          byte array of the serialized value
     */
    @Override
    public byte[] serialize(GenericRecord element) {
        if (this.encoder == null) {
            Schema.Parser parser = new Schema.Parser();
            try {
                this.externalSchema = parser.parse(schemaFile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot parse external Avro reader schema file: " + schemaFile, e);
            }
            CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(registryURL, 100);
            this.encoder = new KafkaAvroSerializer(schemaRegistry);
        }
        List<Schema.Field> filds = externalSchema.getFields();
        GenericRecord reconstructedRecord = new GenericData.Record(externalSchema);
        for(int i=0; i<filds.size(); i++) {
            String key = filds.get(i).name();
            reconstructedRecord.put(key, element.get(key));
        }
        return encoder.serialize(topic, reconstructedRecord);
    }

}

