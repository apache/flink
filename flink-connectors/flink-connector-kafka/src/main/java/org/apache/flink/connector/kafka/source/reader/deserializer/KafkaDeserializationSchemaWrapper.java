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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * A wrapper class that wraps a {@link
 * org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema} to deserialize {@link
 * ConsumerRecord ConsumerRecords}.
 *
 * @param <T> the type of the deserialized records.
 */
class KafkaDeserializationSchemaWrapper<T> implements KafkaRecordDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final KafkaDeserializationSchema<T> kafkaDeserializationSchema;

    KafkaDeserializationSchemaWrapper(KafkaDeserializationSchema<T> kafkaDeserializationSchema) {
        this.kafkaDeserializationSchema = kafkaDeserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        kafkaDeserializationSchema.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<T> out)
            throws IOException {
        try {
            kafkaDeserializationSchema.deserialize(message, out);
        } catch (Exception exception) {
            throw new IOException(
                    String.format("Failed to deserialize consumer record %s.", message), exception);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return kafkaDeserializationSchema.getProducedType();
    }
}
