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

package org.apache.flink.connector.pulsar.source.enumerator.cursor.start;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import static org.apache.flink.connector.pulsar.source.enumerator.cursor.MessageIdUtils.nextMessageId;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.MessageIdUtils.unwrapMessageId;

/** This cursor would left pulsar start consuming from a specific message id. */
public class MessageIdStartCursor implements StartCursor {
    private static final long serialVersionUID = -8057345435887170111L;

    private final MessageId messageId;

    /**
     * The default {@code inclusive} behavior should be controlled in {@link
     * ConsumerBuilder#startMessageIdInclusive}. But pulsar has a bug and don't support this
     * currently. We have to use {@code entry + 1} policy for consuming the next available message.
     * If the message id entry is not valid. Pulsar would automatically find next valid message id.
     * Please referer <a
     * href="https://github.com/apache/pulsar/blob/36d5738412bb1ed9018178007bf63d9202b675db/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedCursorImpl.java#L1151">this
     * code</a> for understanding pulsar internal logic.
     *
     * @param messageId The message id for start position.
     * @param inclusive Whether we include the start message id in consuming result. This works only
     *     if we provide a specified message id instead of {@link MessageId#earliest} or {@link
     *     MessageId#latest}.
     */
    public MessageIdStartCursor(MessageId messageId, boolean inclusive) {
        MessageIdImpl idImpl = unwrapMessageId(messageId);
        if (MessageId.earliest.equals(idImpl) || MessageId.latest.equals(idImpl) || inclusive) {
            this.messageId = idImpl;
        } else {
            this.messageId = nextMessageId(idImpl);
        }
    }

    @Override
    public CursorPosition position(String topic, int partitionId) {
        return new CursorPosition(messageId);
    }
}
