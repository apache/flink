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

package org.apache.flink.connector.pulsar.source.enumerator.cursor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.annotation.Nullable;

import java.io.Serializable;

/** The class for defining the start or stop position. */
@PublicEvolving
public final class CursorPosition implements Serializable {
    private static final long serialVersionUID = -802405183307684549L;

    private final Type type;

    private final MessageId messageId;

    private final Long timestamp;

    public CursorPosition(@Nullable MessageId messageId) {
        this.type = Type.MESSAGE_ID;
        this.messageId = messageId;
        this.timestamp = null;
    }

    public CursorPosition(@Nullable Long timestamp) {
        this.type = Type.TIMESTAMP;
        this.messageId = null;
        this.timestamp = timestamp;
    }

    @VisibleForTesting
    public MessageId getMessageId() {
        return messageId;
    }

    /** Pulsar consumer could be subscribed by the position. */
    public void seekPosition(Consumer<?> consumer) throws PulsarClientException {
        if (type == Type.MESSAGE_ID) {
            consumer.seek(messageId);
        } else {
            if (timestamp != null) {
                consumer.seek(timestamp);
            } else {
                consumer.seek(System.currentTimeMillis());
            }
        }
    }

    @Override
    public String toString() {
        if (type == Type.TIMESTAMP) {
            return "timestamp: " + timestamp;
        } else {
            return "message id: " + messageId;
        }
    }

    /**
     * The position type for reader to choose whether timestamp or message id as the start position.
     */
    public enum Type {
        TIMESTAMP,

        MESSAGE_ID
    }
}
