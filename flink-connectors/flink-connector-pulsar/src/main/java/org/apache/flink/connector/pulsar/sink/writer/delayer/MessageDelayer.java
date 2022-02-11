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

package org.apache.flink.connector.pulsar.sink.writer.delayer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;

import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;
import java.time.Duration;

/**
 * A delayer for Pulsar broker passing the sent message to the downstream consumer. This is only
 * works in {@link SubscriptionType#Shared} subscription.
 *
 * <p>Read <a
 * href="https://pulsar.apache.org/docs/en/next/concepts-messaging/#delayed-message-delivery">delayed
 * message delivery</a> for better understanding this feature.
 */
@PublicEvolving
public interface MessageDelayer<IN> extends Serializable {

    /**
     * Return the send time for this message. You should calculate the timestamp by using {@link
     * PulsarSinkContext#processTime()} and the non-positive value indicate this message should be
     * sent immediately.
     */
    long deliverAt(IN message, PulsarSinkContext sinkContext);

    /** Implement this method if you have some non-serializable field. */
    default void open(SinkConfiguration sinkConfiguration) {
        // Nothing to do by default.
    }

    /** All the messages should be consumed immediately. */
    static <IN> FixedMessageDelayer<IN> never() {
        return new FixedMessageDelayer<>(-1L);
    }

    /** All the messages should be consumed in a fixed duration. */
    static <IN> FixedMessageDelayer<IN> fixed(Duration duration) {
        return new FixedMessageDelayer<>(duration.toMillis());
    }
}
