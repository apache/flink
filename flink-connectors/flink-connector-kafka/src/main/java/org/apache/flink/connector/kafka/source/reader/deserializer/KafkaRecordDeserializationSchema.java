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

package org.apache.flink.connector.kafka.source.reader.deserializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/** An interface for the deserialization of Kafka records. */
public interface KafkaRecordDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    @PublicEvolving
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param record The ConsumerRecord to deserialize.
     * @param out The collector to put the resulting messages.
     */
    @PublicEvolving
    void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out) throws IOException;

    /**
     * Wraps a legacy {@link KafkaDeserializationSchema} as the deserializer of the {@link
     * ConsumerRecord ConsumerRecords}.
     *
     * <p>Note that the {@link KafkaDeserializationSchema#isEndOfStream(Object)} method will no
     * longer be used to determin the end of the stream.
     *
     * @param kafkaDeserializationSchema the legacy {@link KafkaDeserializationSchema} to use.
     * @param <V> the return type of the deserialized record.
     * @return A {@link KafkaRecordDeserializationSchema} that uses the given {@link
     *     KafkaDeserializationSchema} to deserialize the {@link ConsumerRecord ConsumerRecords}.
     */
    static <V> KafkaRecordDeserializationSchema<V> of(
            KafkaDeserializationSchema<V> kafkaDeserializationSchema) {
        return new KafkaDeserializationSchemaWrapper<>(kafkaDeserializationSchema);
    }

    /**
     * Wraps a {@link DeserializationSchema} as the value deserialization schema of the {@link
     * ConsumerRecord ConsumerRecords}. The other fields such as key, headers, timestamp are
     * ignored.
     *
     * <p>Note that the {@link DeserializationSchema#isEndOfStream(Object)} method will no longer be
     * used to determine the end of the stream.
     *
     * @param valueDeserializationSchema the {@link DeserializationSchema} used to deserialized the
     *     value of a {@link ConsumerRecord}.
     * @param <V> the type of the deserialized record.
     * @return A {@link KafkaRecordDeserializationSchema} that uses the given {@link
     *     DeserializationSchema} to deserialize a {@link ConsumerRecord} from its value.
     */
    static <V> KafkaRecordDeserializationSchema<V> valueOnly(
            DeserializationSchema<V> valueDeserializationSchema) {
        return new KafkaValueOnlyDeserializationSchemaWrapper<>(valueDeserializationSchema);
    }

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link KafkaRecordDeserializationSchema}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param <V> the value type.
     * @return A {@link KafkaRecordDeserializationSchema} that deserialize the value with the given
     *     deserializer.
     */
    static <V> KafkaRecordDeserializationSchema<V> valueOnly(
            Class<? extends Deserializer<V>> valueDeserializerClass) {
        return new KafkaValueOnlyDeserializerWrapper<>(
                valueDeserializerClass, Collections.emptyMap());
    }

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link KafkaRecordDeserializationSchema}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param config the configuration of the value deserializer, only valid when the deserializer
     *     is an implementation of {@code Configurable}.
     * @param <V> the value type.
     * @param <D> the type of the deserializer.
     * @return A {@link KafkaRecordDeserializationSchema} that deserialize the value with the given
     *     deserializer.
     */
    static <V, D extends Configurable & Deserializer<V>>
            KafkaRecordDeserializationSchema<V> valueOnly(
                    Class<D> valueDeserializerClass, Map<String, String> config) {
        return new KafkaValueOnlyDeserializerWrapper<>(valueDeserializerClass, config);
    }
}
