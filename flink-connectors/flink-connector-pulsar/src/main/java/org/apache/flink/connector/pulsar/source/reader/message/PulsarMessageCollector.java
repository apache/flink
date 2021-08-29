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

package org.apache.flink.connector.pulsar.source.reader.message;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

/**
 * This collector supplier is providing the {@link Collector} for accepting the deserialized {@link
 * PulsarMessage} from pulsar {@link PulsarDeserializationSchema}.
 *
 * @param <T> The deserialized pulsar message type, aka the source message type.
 */
public class PulsarMessageCollector<T> implements Collector<T> {

    private final String splitId;
    private final RecordsBySplits.Builder<PulsarMessage<T>> builder;
    private Message<?> message;

    public PulsarMessageCollector(
            String splitId, RecordsBySplits.Builder<PulsarMessage<T>> builder) {
        this.splitId = splitId;
        this.builder = builder;
    }

    public void setMessage(Message<?> message) {
        this.message = message;
    }

    @Override
    public void collect(T t) {
        PulsarMessage<T> result =
                new PulsarMessage<>(message.getMessageId(), t, message.getEventTime());
        builder.add(splitId, result);
    }

    @Override
    public void close() {
        // Nothing to do for this collector.
    }
}
