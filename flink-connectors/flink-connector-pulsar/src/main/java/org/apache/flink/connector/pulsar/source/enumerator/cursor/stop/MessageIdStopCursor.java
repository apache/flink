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

package org.apache.flink.connector.pulsar.source.enumerator.cursor.stop;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import static org.apache.flink.connector.pulsar.source.enumerator.cursor.MessageIdUtils.unwrapMessageId;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.client.api.MessageId.earliest;
import static org.apache.pulsar.client.api.MessageId.latest;

/**
 * Stop consuming message at a given message id. We use the {@link MessageId#compareTo(Object)} for
 * compare the consuming message with the given message id.
 */
public class MessageIdStopCursor implements StopCursor {
    private static final long serialVersionUID = -3990454110809274542L;

    private final MessageId messageId;

    private final boolean inclusive;

    public MessageIdStopCursor(MessageId messageId, boolean inclusive) {
        MessageIdImpl idImpl = unwrapMessageId(messageId);
        checkArgument(!earliest.equals(idImpl), "MessageId.earliest is not supported.");
        checkArgument(
                !latest.equals(idImpl),
                "MessageId.latest is not supported, use LatestMessageStopCursor instead.");

        this.messageId = idImpl;
        this.inclusive = inclusive;
    }

    @Override
    public StopCondition shouldStop(Message<?> message) {
        MessageId current = message.getMessageId();
        return StopCondition.compare(messageId, current, inclusive);
    }
}
