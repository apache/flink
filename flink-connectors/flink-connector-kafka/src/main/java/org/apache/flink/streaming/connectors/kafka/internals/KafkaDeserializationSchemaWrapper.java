/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A simple wrapper for using the DeserializationSchema with the KafkaDeserializationSchema
 * interface.
 *
 * @param <T> The type created by the deserialization schema.
 */
@Internal
public class KafkaDeserializationSchemaWrapper<T> implements KafkaDeserializationSchema<T> {

    private static final long serialVersionUID = 2651665280744549932L;

    private final DeserializationSchema<T> deserializationSchema;

    public KafkaDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.deserializationSchema.open(context);
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<T> out)
            throws Exception {
        deserializationSchema.deserialize(message.value(), out);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return deserializationSchema.isEndOfStream(nextElement);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
