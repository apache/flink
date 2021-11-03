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

package org.apache.flink.formats.json.glue.schema.registry;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Map;

/**
 * AWS Glue Schema Registry Serialization schema to serialize to JSON Schema binary format for Flink
 * Producer user.
 *
 * @param <T> the type to be serialized
 */
@PublicEvolving
public class GlueSchemaRegistryJsonSerializationSchema<T> implements SerializationSchema<T> {

    private final GlueSchemaRegistryJsonSchemaCoderProvider
            glueSchemaRegistryJsonSchemaCoderProvider;
    protected GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder;

    /**
     * Creates a JSON Schema serialization schema.
     *
     * @param transportName topic name or stream name etc.
     * @param configs configuration map of AWS Glue Schema Registry
     */
    public GlueSchemaRegistryJsonSerializationSchema(
            String transportName, Map<String, Object> configs) {
        this.glueSchemaRegistryJsonSchemaCoderProvider =
                new GlueSchemaRegistryJsonSchemaCoderProvider(transportName, configs);
    }

    @VisibleForTesting
    protected GlueSchemaRegistryJsonSerializationSchema(
            GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder) {
        this.glueSchemaRegistryJsonSchemaCoderProvider = null;
        this.glueSchemaRegistryJsonSchemaCoder = glueSchemaRegistryJsonSchemaCoder;
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
        if (object == null) {
            return null;
        }

        if (glueSchemaRegistryJsonSchemaCoder == null) {
            glueSchemaRegistryJsonSchemaCoder = glueSchemaRegistryJsonSchemaCoderProvider.get();
        }
        return glueSchemaRegistryJsonSchemaCoder.registerSchemaAndSerialize(object);
    }
}
