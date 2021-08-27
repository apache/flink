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

/**
 * Stop consuming message at a given message id. We use the {@link MessageId#compareTo(Object)} for
 * compare the consuming message with the given message id.
 */
public class MessageIdStopCursor implements StopCursor {
    private static final long serialVersionUID = -3990454110809274542L;

    private final MessageId messageId;

    private final boolean exclusive;

    public MessageIdStopCursor(MessageId messageId) {
        this(messageId, true);
    }

    public MessageIdStopCursor(MessageId messageId, boolean exclusive) {
        this.messageId = messageId;
        this.exclusive = exclusive;
    }

    @Override
    public boolean shouldStop(Message<?> message) {
        MessageId id = message.getMessageId();
        if (exclusive) {
            return id.compareTo(messageId) > 0;
        } else {
            return id.compareTo(messageId) >= 0;
        }
    }
}
