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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.Serializable;

/**
 * A schema bridge for deserializing the pulsar's <code>Message<?></code> into a flink managed
 * instance. We support both the pulsar's self managed schema and flink managed schema.
 *
 * @param <M> The decode message type from pulsar client, which would create a message {@link
 *     SchemaInfo} from this type.
 * @param <T> The output message type for sinking to downstream flink operator.
 */
@PublicEvolving
public interface PulsarDeserializationSchema<M, T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
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
    void deserialize(Message<M> message, Collector<T> out) throws Exception;

    /**
     * Pulsar's {@link Schema} implementation is not serializable, So the implementation for this
     * method should return a new schema instance everytime we call this method.
     *
     * @return A pulsar managed schema for decoding pulsar message.
     */
    Schema<M> createPulsarSchema();

    /**
     * Create a PulsarDeserializationSchema by using custom PulsarSchemaFactory,
     * PulsarMessageDeserializer and TypeInformation.
     *
     * <p>Using this method if you want the decode the message by using pulsar's schema first. Then
     * convert the message into one or multiple results and send to downstream operator.
     */
    static <P, F> PulsarDeserializationSchema<P, F> of(
            PulsarSchemaFactory<P> schemaFactory,
            PulsarMessageDeserializer<P, F> pulsarDeserializer,
            TypeInformation<F> typeInformation) {

        return new PulsarDeserializationSchemaBase<>(
                schemaFactory, pulsarDeserializer, typeInformation);
    }

    /**
     * Create a PulsarDeserializationSchema by using the PulsarSchemaFactory class, we would create
     * a PulsarSchemaFactory instance by using the default constructor. The message bytes would be
     * decoded by pulsar client.
     *
     * @param <F> This type could be some primitive type or POJO type. Flink would auto extract the
     *     {@link TypeInformation} by using {@link TypeExtractor#createTypeInfo(Class, Class, int,
     *     TypeInformation, TypeInformation)}.
     */
    static <F> PulsarDeserializationSchema<F, F> pulsarSchema(
            Class<? extends PulsarSchemaFactory<F>> clazz) {
        return new PulsarSchemaWrapper<>(clazz);
    }

    /**
     * Create a PulsarDeserializationSchema by using the PulsarSchemaFactory instance. The message
     * bytes would be decoded by pulsar client and directly hanover to flink.
     *
     * @param <F> This type could be some primitive type or POJO type. Flink would auto extract the
     *     {@link TypeInformation} by using {@link TypeExtractor#createTypeInfo(Class, Class, int,
     *     TypeInformation, TypeInformation)}.
     */
    static <F> PulsarDeserializationSchema<F, F> pulsarSchema(
            PulsarSchemaFactory<F> schemaFactory) {
        return new PulsarSchemaWrapper<>(schemaFactory);
    }

    /**
     * Create a PulsarDeserializationSchema by using the PulsarSchemaFactory instance. You can
     * provide the extra {@link TypeInformation} if the instance couldn't be recognized by flink
     * directly.
     */
    static <F> PulsarDeserializationSchema<F, F> pulsarSchema(
            PulsarSchemaFactory<F> schemaFactory, TypeInformation<F> typeInformation) {
        return new PulsarSchemaWrapper<>(schemaFactory, typeInformation);
    }

    /**
     * Create a PulsarDeserializationSchema by using the flink's DeserializationSchema. It would
     * consume the pulsar message as byte array and decode the message by using flink's logic.
     */
    static <F> PulsarDeserializationSchema<byte[], F> flinkSchema(
            DeserializationSchema<F> deserializationSchema) {
        return new PulsarDeserializationSchemaWrapper<>(deserializationSchema);
    }
}
