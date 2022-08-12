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
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.EventTimestampStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.LatestMessageStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.MessageIdStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.NeverStopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.PublishTimestampStopCursor;
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

    /** Determine whether to pause consumption on the current message by the returned enum. */
    StopCondition shouldStop(Message<?> message);

    /** The conditional for control the stop behavior of the pulsar source. */
    @PublicEvolving
    enum StopCondition {

        /** This message should be included in the result. */
        CONTINUE,
        /** This message should be included in the result and stop consuming. */
        EXACTLY,
        /** Stop consuming, the given message wouldn't be included in the result. */
        TERMINATE;

        /**
         * Common methods for comparing the message id.
         *
         * @param desired The stop goal of the message id.
         * @param current The upcoming message id.
         * @param inclusive Should the desired message be included in the consuming result.
         */
        public static StopCondition compare(
                MessageId desired, MessageId current, boolean inclusive) {
            if (current.compareTo(desired) < 0) {
                return StopCondition.CONTINUE;
            } else if (current.compareTo(desired) == 0) {
                return inclusive ? StopCondition.EXACTLY : StopCondition.TERMINATE;
            } else {
                return StopCondition.TERMINATE;
            }
        }

        /**
         * Common methods for comparing the message time.
         *
         * @param desired The stop goal of the message time.
         * @param current The upcoming message time.
         * @param inclusive Should the desired message be included in the consuming result.
         */
        public static StopCondition compare(long desired, long current, boolean inclusive) {
            if (current < desired) {
                return StopCondition.CONTINUE;
            } else if (current == desired) {
                return inclusive ? StopCondition.EXACTLY : StopCondition.TERMINATE;
            } else {
                return StopCondition.TERMINATE;
            }
        }
    }

    // --------------------------- Static Factory Methods -----------------------------

    static StopCursor defaultStopCursor() {
        return never();
    }

    static StopCursor never() {
        return new NeverStopCursor();
    }

    static StopCursor latest() {
        return new LatestMessageStopCursor(true);
    }

    /**
     * Stop consuming when the messageId is equal or greater than the specified messageId. Message
     * that is equal to the specified messageId will not be consumed.
     */
    static StopCursor atMessageId(MessageId messageId) {
        if (MessageId.latest.equals(messageId)) {
            return new LatestMessageStopCursor(false);
        } else {
            return new MessageIdStopCursor(messageId, false);
        }
    }

    /**
     * Stop consuming when the messageId is greater than the specified messageId. Message that is
     * equal to the specified messageId will be consumed.
     */
    static StopCursor afterMessageId(MessageId messageId) {
        if (MessageId.latest.equals(messageId)) {
            return new LatestMessageStopCursor(true);
        } else {
            return new MessageIdStopCursor(messageId, true);
        }
    }

    /** Stop consuming when message eventTime is greater than or equals the specified timestamp. */
    static StopCursor atEventTime(long timestamp) {
        return new EventTimestampStopCursor(timestamp, false);
    }

    /** Stop consuming when message eventTime is greater than the specified timestamp. */
    static StopCursor afterEventTime(long timestamp) {
        return new EventTimestampStopCursor(timestamp, true);
    }

    /**
     * Stop consuming when message publishTime is greater than or equals the specified timestamp.
     */
    static StopCursor atPublishTime(long timestamp) {
        return new PublishTimestampStopCursor(timestamp, false);
    }

    /** Stop consuming when message publishTime is greater than the specified timestamp. */
    static StopCursor afterPublishTime(long timestamp) {
        return new PublishTimestampStopCursor(timestamp, true);
    }
}
