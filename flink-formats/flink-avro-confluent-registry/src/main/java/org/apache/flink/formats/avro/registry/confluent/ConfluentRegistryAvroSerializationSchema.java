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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Serialization schema that serializes to Avro binary format that uses Confluent Schema Registry.
 *
 * @param <T> the type to be serialized
 */
public class ConfluentRegistryAvroSerializationSchema<T>
        extends RegistryAvroSerializationSchema<T> {

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private static final long serialVersionUID = -1771641202177852775L;

    /**
     * Creates a Avro serialization schema.
     *
     * @param recordClazz class to serialize. Should be either {@link SpecificRecord} or {@link
     *     GenericRecord}.
     * @param schema writer's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider provider for schema coder that writes the writer schema to
     *     Confluent Schema Registry
     */
    private ConfluentRegistryAvroSerializationSchema(
            Class<T> recordClazz,
            Schema schema,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, schema, schemaCoderProvider);
    }

    /**
     * Creates {@link AvroSerializationSchema} that produces byte arrays that were generated from
     * Avro schema and writes the writer schema to Confluent Schema Registry.
     *
     * @param tClass the type to be serialized
     * @param subject subject of schema registry to produce
     * @param schemaRegistryUrl URL of schema registry to connect
     * @return serialized record
     */
    public static <T extends SpecificRecord>
            ConfluentRegistryAvroSerializationSchema<T> forSpecific(
                    Class<T> tClass, String subject, String schemaRegistryUrl) {
        return forSpecific(tClass, subject, schemaRegistryUrl, null);
    }

    /**
     * Creates {@link AvroSerializationSchema} that produces byte arrays that were generated from
     * Avro schema and writes the writer schema to Confluent Schema Registry.
     *
     * @param tClass the type to be serialized
     * @param subject subject of schema registry to produce
     * @param schemaRegistryUrl URL of schema registry to connect
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return serialized record
     */
    public static <T extends SpecificRecord>
            ConfluentRegistryAvroSerializationSchema<T> forSpecific(
                    Class<T> tClass,
                    String subject,
                    String schemaRegistryUrl,
                    @Nullable Map<String, ?> registryConfigs) {
        return new ConfluentRegistryAvroSerializationSchema<>(
                tClass,
                null,
                new CachedSchemaCoderProvider(
                        subject,
                        schemaRegistryUrl,
                        DEFAULT_IDENTITY_MAP_CAPACITY,
                        registryConfigs));
    }

    /**
     * Creates {@link AvroSerializationSchema} that produces byte arrays that were generated from
     * Avro schema and writes the writer schema to Confluent Schema Registry.
     *
     * @param subject subject of schema registry to produce
     * @param schema schema that will be used for serialization
     * @param schemaRegistryUrl URL of schema registry to connect
     * @return serialized record
     */
    public static ConfluentRegistryAvroSerializationSchema<GenericRecord> forGeneric(
            String subject, Schema schema, String schemaRegistryUrl) {
        return forGeneric(subject, schema, schemaRegistryUrl, null);
    }

    /**
     * Creates {@link AvroSerializationSchema} that produces byte arrays that were generated from
     * Avro schema and writes the writer schema to Confluent Schema Registry.
     *
     * @param subject subject of schema registry to produce
     * @param schema schema that will be used for serialization
     * @param schemaRegistryUrl URL of schema registry to connect
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return serialized record
     */
    public static ConfluentRegistryAvroSerializationSchema<GenericRecord> forGeneric(
            String subject,
            Schema schema,
            String schemaRegistryUrl,
            @Nullable Map<String, ?> registryConfigs) {
        return new ConfluentRegistryAvroSerializationSchema<>(
                GenericRecord.class,
                schema,
                new CachedSchemaCoderProvider(
                        subject,
                        schemaRegistryUrl,
                        DEFAULT_IDENTITY_MAP_CAPACITY,
                        registryConfigs));
    }
}
