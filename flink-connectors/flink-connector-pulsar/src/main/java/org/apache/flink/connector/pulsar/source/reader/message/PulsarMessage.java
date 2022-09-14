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

package org.apache.flink.connector.pulsar.source.reader.message;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * The message instance that contains the required information which would be used for committing
 * the consuming status.
 */
@Internal
public class PulsarMessage<T> {

    /**
     * The id of a given message. This id could be same for multiple {@link PulsarMessage}, although
     * it is unique for every {@link Message}.
     */
    private final MessageId id;

    /** The value which deserialized by {@link PulsarDeserializationSchema}. */
    private final T value;

    /** The produce time for this message, it's a event time. */
    private final long eventTime;

    public PulsarMessage(MessageId id, T value, long eventTime) {
        this.id = id;
        this.value = value;
        this.eventTime = eventTime;
    }

    public MessageId getId() {
        return id;
    }

    public T getValue() {
        return value;
    }

    public long getEventTime() {
        return eventTime;
    }

    @Override
    public String toString() {
        return "PulsarMessage{"
                + "id="
                + id
                + ", value="
                + value
                + ", eventTime="
                + eventTime
                + '}';
    }
}
