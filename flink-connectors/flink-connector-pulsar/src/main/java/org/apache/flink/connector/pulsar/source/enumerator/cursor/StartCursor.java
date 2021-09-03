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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.start.MessageIdStartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.start.TimestampStartCursor;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;

/**
 * A interface for users to specify the start position of a pulsar subscription. Since it would be
 * serialized into split. The implementation for this interface should be well considered. I don't
 * recommend adding extra internal state for this implementation.
 *
 * <p>This class would be used only for {@link SubscriptionType#Exclusive} and {@link
 * SubscriptionType#Failover}.
 */
@PublicEvolving
@FunctionalInterface
public interface StartCursor extends Serializable {

    CursorPosition position(String topic, int partitionId);

    /** Helper method for seek the right position for given pulsar consumer. */
    default void seekPosition(String topic, int partitionId, Consumer<?> consumer)
            throws PulsarClientException {
        CursorPosition position = position(topic, partitionId);
        position.seekPosition(consumer);
    }

    // --------------------------- Static Factory Methods -----------------------------

    static StartCursor defaultStartCursor() {
        return earliest();
    }

    static StartCursor earliest() {
        return fromMessageId(MessageId.earliest);
    }

    static StartCursor latest() {
        return fromMessageId(MessageId.latest);
    }

    static StartCursor fromMessageId(MessageId messageId) {
        return fromMessageId(messageId, true);
    }

    /**
     * @param messageId Find the available message id and start consuming from it.
     * @param inclusive {@code ture} would include the given message id.
     */
    static StartCursor fromMessageId(MessageId messageId, boolean inclusive) {
        return new MessageIdStartCursor(messageId, inclusive);
    }

    static StartCursor fromMessageTime(long timestamp) {
        return new TimestampStartCursor(timestamp);
    }
}
