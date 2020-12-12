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

import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.SchemaCoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * AWS Glue Schema Registry Deserialization schema to de-serialize Avro binary format for Flink
 * Consumer user.
 *
 * @param <T> type of record it produces
 */
public class GlueSchemaRegistryAvroDeserializationSchema<T>
        extends RegistryAvroDeserializationSchema<T> {

    /**
     * Creates a Avro deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of: {@link SpecificRecord},
     *     {@link GenericRecord}.
     * @param reader reader's Avro schema. Should be provided if recordClazz is {@link
     *     GenericRecord}
     * @param schemaCoderProvider schema coder provider which reads writer schema from AWS Glue
     *     Schema Registry
     */
    private GlueSchemaRegistryAvroDeserializationSchema(
            Class<T> recordClazz,
            @Nullable Schema reader,
            SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
        super(recordClazz, reader, schemaCoderProvider);
    }

    /**
     * Creates {@link GlueSchemaRegistryAvroDeserializationSchema} that produces {@link
     * GenericRecord} using provided schema.
     *
     * @param schema schema of produced records
     * @param configs configuration map of AWS Glue Schema Registry
     * @return deserialized record in form of {@link GenericRecord}
     */
    public static GlueSchemaRegistryAvroDeserializationSchema<GenericRecord> forGeneric(
            Schema schema, Map<String, Object> configs) {
        return new GlueSchemaRegistryAvroDeserializationSchema<>(
                GenericRecord.class,
                schema,
                new GlueSchemaRegistryAvroSchemaCoderProvider(configs));
    }

    /**
     * Creates {@link GlueSchemaRegistryAvroDeserializationSchema} that produces classes that were
     * generated from avro schema.
     *
     * @param clazz class of record to be produced
     * @param configs configuration map of AWS Glue Schema Registry
     * @return deserialized record
     */
    public static <T extends SpecificRecord>
            GlueSchemaRegistryAvroDeserializationSchema<T> forSpecific(
                    Class<T> clazz, Map<String, Object> configs) {
        return new GlueSchemaRegistryAvroDeserializationSchema<>(
                clazz, null, new GlueSchemaRegistryAvroSchemaCoderProvider(configs));
    }
}
