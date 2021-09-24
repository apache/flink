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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;

/**
 * AWS Glue Schema Registry Deserialization schema to de-serialize JSON Schema binary format for
 * Flink Consumer user.
 *
 * @param <T> type of record it produces
 */
@PublicEvolving
public class GlueSchemaRegistryJsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private final Class<T> recordClazz;
    private final GlueSchemaRegistryJsonSchemaCoderProvider
            glueSchemaRegistryJsonSchemaCoderProvider;
    protected GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder;

    /**
     * Creates a JSON Schema deserialization schema.
     *
     * @param recordClazz class to which deserialize. Should be one of: {@link
     *     com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema}, or user
     *     defined POJO.
     * @param transportName topic name or stream name etc.
     * @param configs configuration map of AWS Glue Schema Registry
     */
    public GlueSchemaRegistryJsonDeserializationSchema(
            Class<T> recordClazz, String transportName, Map<String, Object> configs) {
        this.recordClazz = recordClazz;
        this.glueSchemaRegistryJsonSchemaCoderProvider =
                new GlueSchemaRegistryJsonSchemaCoderProvider(transportName, configs);
    }

    @VisibleForTesting
    protected GlueSchemaRegistryJsonDeserializationSchema(
            Class<T> recordClazz,
            GlueSchemaRegistryJsonSchemaCoder glueSchemaRegistryJsonSchemaCoder) {
        this.recordClazz = recordClazz;
        this.glueSchemaRegistryJsonSchemaCoderProvider = null;
        this.glueSchemaRegistryJsonSchemaCoder = glueSchemaRegistryJsonSchemaCoder;
    }

    /**
     * Deserializes the incoming byte array which contains bytes of AWS Glue Schema registry
     * information back to the original object.
     *
     * @param bytes The incoming byte array to be deserialized
     * @return The deserialized object.
     */
    @Override
    public T deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (glueSchemaRegistryJsonSchemaCoder == null) {
            glueSchemaRegistryJsonSchemaCoder = glueSchemaRegistryJsonSchemaCoderProvider.get();
        }
        return (T) glueSchemaRegistryJsonSchemaCoder.deserialize(bytes);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(recordClazz);
    }
}
