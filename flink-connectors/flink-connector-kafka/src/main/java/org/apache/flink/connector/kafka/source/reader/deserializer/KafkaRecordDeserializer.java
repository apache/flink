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

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/** An interface for the deserialization of Kafka records. */
public interface KafkaRecordDeserializer<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize a consumer record into the given collector.
     *
     * @param record the {@code ConsumerRecord} to deserialize.
     * @throws Exception if the deserialization failed.
     */
    void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> collector)
            throws Exception;

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link KafkaRecordDeserializer}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param <V> the value type.
     * @return A {@link KafkaRecordDeserializer} that deserialize the value with the given
     *     deserializer.
     */
    static <V> KafkaRecordDeserializer<V> valueOnly(
            Class<? extends Deserializer<V>> valueDeserializerClass) {
        return new ValueDeserializerWrapper<>(valueDeserializerClass, Collections.emptyMap());
    }

    /**
     * Wraps a Kafka {@link Deserializer} to a {@link KafkaRecordDeserializer}.
     *
     * @param valueDeserializerClass the deserializer class used to deserialize the value.
     * @param config the configuration of the value deserializer, only valid when the deserializer
     *     is an implementation of {@code Configurable}.
     * @param <V> the value type.
     * @param <D> the type of the deserializer.
     * @return A {@link KafkaRecordDeserializer} that deserialize the value with the given
     *     deserializer.
     */
    static <V, D extends Configurable & Deserializer<V>> KafkaRecordDeserializer<V> valueOnly(
            Class<D> valueDeserializerClass, Map<String, String> config) {
        return new ValueDeserializerWrapper<>(valueDeserializerClass, config);
    }
}
