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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.LatestMessageStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.MessageIdStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.NeverStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.TimestampStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.io.Serializable;

/**
 * A interface for users to specify the stop position of a pulsar subscription. Since it would be
 * serialized into split. The implementation for this interface should be well considered. I don't
 * recommend adding extra internal state for this implementation.
 */
@PublicEvolving
@FunctionalInterface
public interface StopCursor extends Serializable {

    /** The open method for the cursor initializer. This method could be executed multiple times. */
    default void open(PulsarAdmin admin, TopicPartition partition) {}

    /**
     * Determine whether to pause consumption on the current message by the returned boolean value.
     * The message presented in method argument wouldn't be consumed if the return result is true.
     */
    boolean shouldStop(Message<?> message);

    // --------------------------- Static Factory Methods -----------------------------

    static StopCursor defaultStopCursor() {
        return never();
    }

    static StopCursor never() {
        return new NeverStopCursor();
    }

    static StopCursor latest() {
        return new LatestMessageStopCursor();
    }

    static StopCursor atMessageId(MessageId messageId) {
        return new MessageIdStopCursor(messageId);
    }

    static StopCursor afterMessageId(MessageId messageId) {
        return new MessageIdStopCursor(messageId, false);
    }

    static StopCursor atEventTime(long timestamp) {
        return new TimestampStopCursor(timestamp);
    }
}
