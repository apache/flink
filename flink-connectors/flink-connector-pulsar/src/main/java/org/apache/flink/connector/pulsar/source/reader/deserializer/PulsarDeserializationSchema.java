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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;

import java.io.Serializable;

/**
 * A schema bridge for deserializing the pulsar's {@code Message<byte[]>} into a flink managed
 * instance. We support both the pulsar's self managed schema and flink managed schema.
 *
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
public interface PulsarDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(InitializationContext context) throws Exception {
        // Nothing to do here for the default implementation.
    }

    /**
     * Deserializes the pulsar message. This message could be a raw byte message or some parsed
     * message which decoded by pulsar schema.
     *
     * <p>You can output multiple message by using the {@link Collector}. Note that number and size
     * of the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param message The message decoded by pulsar.
     * @param out The collector to put the resulting messages.
     */
    void deserialize(Message<byte[]> message, Collector<T> out) throws Exception;

    /**
     * Create a PulsarDeserializationSchema by using the flink's {@link DeserializationSchema}. It
     * would consume the pulsar message as byte array and decode the message by using flink's logic.
     */
    static <T> PulsarDeserializationSchema<T> flinkSchema(
            DeserializationSchema<T> deserializationSchema) {
        return new PulsarDeserializationSchemaWrapper<>(deserializationSchema);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">primitive
     * types</a> here.
     */
    static <T> PulsarDeserializationSchema<T> pulsarSchema(Schema<T> schema) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#struct">struct types</a> here.
     */
    static <T> PulsarDeserializationSchema<T> pulsarSchema(Schema<T> schema, Class<T> typeClass) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema, typeClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarDeserializationSchema by using the Pulsar {@link Schema} instance. The message
     * bytes must be encoded by pulsar Schema.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#keyvalue">keyvalue types</a> here.
     */
    static <K, V> PulsarDeserializationSchema<KeyValue<K, V>> pulsarSchema(
            Schema<KeyValue<K, V>> schema, Class<K> keyClass, Class<V> valueClass) {
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarDeserializationSchema by using the given {@link TypeInformation}. This method
     * is only used for treating message that was written into pulsar by {@link TypeInformation}.
     */
    static <T> PulsarDeserializationSchema<T> flinkTypeInfo(
            TypeInformation<T> information, ExecutionConfig config) {
        return new PulsarTypeInformationWrapper<>(information, config);
    }
}
