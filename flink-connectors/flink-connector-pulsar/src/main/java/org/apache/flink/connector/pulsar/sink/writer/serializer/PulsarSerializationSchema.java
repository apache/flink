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

package org.apache.flink.connector.pulsar.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessageBuilder;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.schema.KeyValue;

import java.io.Serializable;

/**
 * The serialization schema for how to serialize records into Pulsar.
 *
 * @param <IN> The message type send to Pulsar.
 */
@PublicEvolving
public interface PulsarSerializationSchema<IN> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, PulsarSinkContext)} and thus suitable for one-time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as registering user metrics.
     *
     * @param initializationContext Contextual information that can be used during initialization.
     * @param sinkContext Runtime information i.e. partitions, subtaskId.
     * @param sinkConfiguration All the configure options for the Pulsar sink. You can add custom
     *     options.
     */
    default void open(
            InitializationContext initializationContext,
            PulsarSinkContext sinkContext,
            SinkConfiguration sinkConfiguration)
            throws Exception {
        // Nothing to do by default.
    }

    /**
     * Serializes the given element into bytes and {@link Schema#BYTES}. Or you can convert it to a
     * new type of instance with a {@link Schema}. The return value {@link PulsarMessage} can be
     * built by {@link PulsarMessageBuilder}. All the methods provided in the {@link
     * PulsarMessageBuilder} is just equals to the {@link TypedMessageBuilder}.
     *
     * @param element Element to be serialized.
     * @param sinkContext Context to provide extra information.
     */
    PulsarMessage<?> serialize(IN element, PulsarSinkContext sinkContext);

    /**
     * Create a PulsarSerializationSchema by using the flink's {@link SerializationSchema}. It would
     * serialize the message into byte array and send it to Pulsar with {@link Schema#BYTES}.
     */
    static <T> PulsarSerializationSchema<T> flinkSchema(
            SerializationSchema<T> serializationSchema) {
        return new PulsarSerializationSchemaWrapper<>(serializationSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. We can send
     * message with the given schema to Pulsar, this would be enabled by {@link
     * PulsarSinkBuilder#enableSchemaEvolution()}. We would serialize the message into bytes and
     * send it as {@link Schema#BYTES} by default.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#primitive-type">primitive
     * types</a> here.
     */
    static <T> PulsarSerializationSchema<T> pulsarSchema(Schema<T> schema) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. We can send
     * message with the given schema to Pulsar, this would be enabled by {@link
     * PulsarSinkBuilder#enableSchemaEvolution()}. We would serialize the message into bytes and
     * send it as {@link Schema#BYTES} by default.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#struct">struct types</a> here.
     */
    static <T> PulsarSerializationSchema<T> pulsarSchema(Schema<T> schema, Class<T> typeClass) {
        PulsarSchema<T> pulsarSchema = new PulsarSchema<>(schema, typeClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }

    /**
     * Create a PulsarSerializationSchema by using the Pulsar {@link Schema} instance. We can send
     * message with the given schema to Pulsar, this would be enabled by {@link
     * PulsarSinkBuilder#enableSchemaEvolution()}. We would serialize the message into bytes and
     * send it as {@link Schema#BYTES} by default.
     *
     * <p>We only support <a
     * href="https://pulsar.apache.org/docs/en/schema-understand/#keyvalue">keyvalue types</a> here.
     */
    static <K, V> PulsarSerializationSchema<KeyValue<K, V>> pulsarSchema(
            Schema<KeyValue<K, V>> schema, Class<K> keyClass, Class<V> valueClass) {
        PulsarSchema<KeyValue<K, V>> pulsarSchema =
                new PulsarSchema<>(schema, keyClass, valueClass);
        return new PulsarSchemaWrapper<>(pulsarSchema);
    }
}
