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

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * Deserialization schema that deserializes from Avro binary format using {@link SchemaCoder} that
 * uses Apicurio Registry.
 *
 * @param <T> type of record it produces
 */
public class ApicurioRegistryAvroDeserializationSchema<T>
        extends RegistryAvroDeserializationSchema<T> {

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private static final long serialVersionUID = -1671641202177852775L;

    /**
     * Creates a Avro deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be either {@link SpecificRecord} or
     *     {@link GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider provider for schema coder that reads writer schema from Apicurio
     *     Schema Registry
     */
    private ApicurioRegistryAvroDeserializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, reader, schemaCoderProvider);
    }

    @Override
    public T deserialize(@Nullable byte[] message) throws IOException {
        return super.deserialize(message);
    }

    /**
     * Creates {@link ApicurioRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
     * using the provided reader schema and looks up the writer schema in the Apicurio Schema
     * Registry.
     *
     * <p>By default, this method supports up to 1000 cached schema versions.
     *
     * @param schema schema of produced records
     * @param url url of schema registry to connect
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static ApicurioRegistryAvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema, String url) {
        return forGeneric(schema, url, DEFAULT_IDENTITY_MAP_CAPACITY);
    }

    /**
     * Creates {@link ApicurioRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
     * using the provided reader schema and looks up the writer schema in the Apicurio Schema
     * Registry.
     *
     * @param schema schema of produced records
     * @param url url of schema registry to connect
     * @param identityMapCapacity maximum number of cached schema versions
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static ApicurioRegistryAvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema, String url, int identityMapCapacity) {
        return forGeneric(schema, url, identityMapCapacity, null);
    }

    /**
     * Creates {@link ApicurioRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
     * using the provided reader schema and looks up the writer schema in the Apicurio Schema
     * Registry.
     *
     * <p>By default, this method supports up to 1000 cached schema versions.
     *
     * @param schema schema of produced records
     * @param url URL of schema registry to connect
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static ApicurioRegistryAvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema, String url, @Nullable Map<String, ?> registryConfigs) {
        return forGeneric(schema, url, DEFAULT_IDENTITY_MAP_CAPACITY, registryConfigs);
    }

    /**
     * Creates {@link ApicurioRegistryAvroDeserializationSchema} that produces {@link GenericRecord}
     * using the provided reader schema and looks up the writer schema in the Apicurio Schema
     * Registry.
     *
     * @param schema schema of produced records
     * @param url URL of schema registry to connect
     * @param identityMapCapacity maximum number of cached schema versions
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static ApicurioRegistryAvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema,
            String url,
            int identityMapCapacity,
            @Nullable Map<String, ?> registryConfigs) {
        return new ApicurioRegistryAvroDeserializationSchema<>(
                GenericRecord.class,
                schema,
                new CachedSchemaCoderProvider(url, identityMapCapacity, registryConfigs));
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from Avro
     * schema and looks up the writer schema in the Apicurio Registry.
     *
     * <p>By default, this method supports up to 1000 cached schema versions.
     *
     * @param tClass class of record to be produced
     * @param url url of schema registry to connect
     * @return deserialized record
     */
    public static <T extends SpecificRecord>
            ApicurioRegistryAvroDeserializationSchema<T> forSpecific(Class<T> tClass, String url) {
        return forSpecific(tClass, url, DEFAULT_IDENTITY_MAP_CAPACITY, null);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from Avro
     * schema and looks up the writer schema in the Apicurio Registry.
     *
     * @param tClass class of record to be produced
     * @param url url of schema registry to connect
     * @param identityMapCapacity maximum number of cached schema versions
     * @return deserialized record
     */
    public static <T extends SpecificRecord>
            ApicurioRegistryAvroDeserializationSchema<T> forSpecific(
                    Class<T> tClass, String url, int identityMapCapacity) {
        return forSpecific(tClass, url, identityMapCapacity, null);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from Avro
     * schema and looks up the writer schema in the Apicurio Registry.
     *
     * <p>By default, this method supports up to 1000 cached schema versions.
     *
     * @param tClass class of record to be produced
     * @param url URL of schema registry to connect
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return deserialized record
     */
    public static <T extends SpecificRecord>
            ApicurioRegistryAvroDeserializationSchema<T> forSpecific(
                    Class<T> tClass, String url, @Nullable Map<String, ?> registryConfigs) {
        return forSpecific(tClass, url, DEFAULT_IDENTITY_MAP_CAPACITY, registryConfigs);
    }

    /**
     * Creates {@link AvroDeserializationSchema} that produces classes that were generated from Avro
     * schema and looks up the writer schema in the Apicurio Registry.
     *
     * @param tClass class of record to be produced
     * @param url URL of schema registry to connect
     * @param identityMapCapacity maximum number of cached schema versions
     * @param registryConfigs map with additional schema registry configs (for example SSL
     *     properties)
     * @return deserialized record
     */
    public static <T extends SpecificRecord>
            ApicurioRegistryAvroDeserializationSchema<T> forSpecific(
                    Class<T> tClass,
                    String url,
                    int identityMapCapacity,
                    @Nullable Map<String, ?> registryConfigs) {
        return new ApicurioRegistryAvroDeserializationSchema<>(
                tClass,
                null,
                new CachedSchemaCoderProvider(url, identityMapCapacity, registryConfigs));
    }
}
