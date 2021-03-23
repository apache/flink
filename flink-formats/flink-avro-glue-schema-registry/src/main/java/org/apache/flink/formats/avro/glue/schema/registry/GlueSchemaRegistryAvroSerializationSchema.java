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

package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * AWS Glue Schema Registry Serialization schema to serialize to Avro binary format for Flink
 * Producer user.
 *
 * @param <T> the type to be serialized
 */
public class GlueSchemaRegistryAvroSerializationSchema<T>
        extends RegistryAvroSerializationSchema<T> {
    /**
     * Creates an Avro serialization schema.
     *
     * @param recordClazz class to serialize. Should be one of: {@link SpecificRecord}, {@link
     *     GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider schema coder provider which reads writer schema from AWS Glue
     *     Schema Registry
     */
    private GlueSchemaRegistryAvroSerializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, reader, schemaCoderProvider);
    }

    @VisibleForTesting
    protected GlueSchemaRegistryAvroSerializationSchema(
            Class<T> recordClazz, @Nullable Schema reader, SchemaCoder schemaCoder) {
        // Pass null schema coder provider
        super(recordClazz, reader, null);
        this.schemaCoder = schemaCoder;
    }

    /**
     * Creates {@link GlueSchemaRegistryAvroSerializationSchema} that serializes {@link
     * GenericRecord} using provided schema.
     *
     * @param schema the schema that will be used for serialization
     * @param transportName topic name or stream name etc.
     * @param configs configuration map of AWS Glue Schema Registry
     * @return serialized record in form of byte array
     */
    public static GlueSchemaRegistryAvroSerializationSchema<GenericRecord> forGeneric(
            Schema schema, String transportName, Map<String, Object> configs) {
        return new GlueSchemaRegistryAvroSerializationSchema<>(
                GenericRecord.class,
                schema,
                new GlueSchemaRegistryAvroSchemaCoderProvider(transportName, configs));
    }

    /**
     * Creates {@link GlueSchemaRegistryAvroSerializationSchema} that serializes {@link
     * SpecificRecord} using provided schema.
     *
     * @param clazz the type to be serialized
     * @param transportName topic name or stream name etc.
     * @param configs configuration map of Amazon Schema Registry
     * @return serialized record in form of byte array
     */
    public static <T extends SpecificRecord>
            GlueSchemaRegistryAvroSerializationSchema<T> forSpecific(
                    Class<T> clazz, String transportName, Map<String, Object> configs) {
        return new GlueSchemaRegistryAvroSerializationSchema<>(
                clazz, null, new GlueSchemaRegistryAvroSchemaCoderProvider(transportName, configs));
    }

    /**
     * Serializes the incoming element to a byte array containing bytes of AWS Glue Schema registry
     * information.
     *
     * @param object The incoming element to be serialized
     * @return The serialized bytes.
     */
    @Override
    public byte[] serialize(T object) {
        checkAvroInitialized();

        if (object == null) {
            return null;
        } else {
            try {
                ByteArrayOutputStream outputStream = getOutputStream();
                outputStream.reset();
                Encoder encoder = getEncoder();
                getDatumWriter().write(object, encoder);
                schemaCoder.writeSchema(getSchema(), outputStream);
                encoder.flush();

                return outputStream.toByteArray();
            } catch (IOException e) {
                throw new WrappingRuntimeException("Failed to serialize schema registry.", e);
            }
        }
    }
}
