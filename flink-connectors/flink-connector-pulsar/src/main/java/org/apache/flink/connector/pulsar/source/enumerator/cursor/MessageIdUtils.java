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

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

import static org.apache.flink.util.Preconditions.checkArgument;

/** The helper class for Pulsar's message id. */
@Internal
public final class MessageIdUtils {

    private MessageIdUtils() {
        // No public constructor.
    }

    /**
     * The implementation from <a
     * href="https://github.com/apache/pulsar/blob/7c8dc3201baad7d02d886dbc26db5c03abce77d6/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/PositionImpl.java#L85">this
     * code snippet</a> to get next message id.
     */
    public static MessageId nextMessageId(MessageId messageId) {
        MessageIdImpl idImpl = unwrapMessageId(messageId);

        if (idImpl.getEntryId() < 0) {
            return newMessageId(idImpl.getLedgerId(), 0, idImpl.getPartitionIndex());
        } else {
            return newMessageId(
                    idImpl.getLedgerId(), idImpl.getEntryId() + 1, idImpl.getPartitionIndex());
        }
    }

    /**
     * Convert the message id interface to its backend implementation. And check if it's a batch
     * message id. We don't support the batch message for its low performance now.
     */
    public static MessageIdImpl unwrapMessageId(MessageId messageId) {
        MessageIdImpl idImpl = MessageIdImpl.convertToMessageIdImpl(messageId);
        if (idImpl instanceof BatchMessageIdImpl) {
            int batchSize = ((BatchMessageIdImpl) idImpl).getBatchSize();
            checkArgument(batchSize == 1, "We only support normal message id currently.");
        }

        return idImpl;
    }

    /** Hide the message id implementation. */
    public static MessageId newMessageId(long ledgerId, long entryId, int partitionIndex) {
        return new MessageIdImpl(ledgerId, entryId, partitionIndex);
    }
}
