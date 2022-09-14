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

import org.apache.pulsar.client.api.MessageId;
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

    /**
     * Find the available message id and start consuming from it. The given message is included in
     * the consuming result by default if you provide a specified message id instead of {@link
     * MessageId#earliest} or {@link MessageId#latest}.
     */
    static StartCursor fromMessageId(MessageId messageId) {
        return fromMessageId(messageId, true);
    }

    /**
     * @param messageId Find the available message id and start consuming from it.
     * @param inclusive {@code true} would include the given message id if it's not the {@link
     *     MessageId#earliest} or {@link MessageId#latest}.
     */
    static StartCursor fromMessageId(MessageId messageId, boolean inclusive) {
        return new MessageIdStartCursor(messageId, inclusive);
    }

    /**
     * This method is designed for seeking message from event time. But Pulsar didn't support
     * seeking from message time, instead, it would seek the position from publish time. We only
     * keep this method for backward compatible.
     *
     * @deprecated Use {@link #fromPublishTime(long)} instead.
     */
    @Deprecated
    static StartCursor fromMessageTime(long timestamp) {
        return new TimestampStartCursor(timestamp, true);
    }

    /** Seek the start position by using message publish time. */
    static StartCursor fromPublishTime(long timestamp) {
        return new TimestampStartCursor(timestamp, true);
    }
}
