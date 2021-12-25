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
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;

/**
 * A stop cursor that initialize the position to the latest message id. The offsets initialization
 * are taken care of by the {@code PulsarPartitionSplitReaderBase} instead of by the {@code
 * PulsarSourceEnumerator}.
 */
public class LatestMessageStopCursor implements StopCursor {

    private MessageId messageId;

    @Override
    public void open(PulsarAdmin admin, TopicPartition partition) {
        if (messageId == null) {
            String topic = partition.getFullTopicName();
            messageId = sneakyAdmin(() -> admin.topics().getLastMessageId(topic));
        }
    }

    @Override
    public boolean shouldStop(Message<?> message) {
        MessageId id = message.getMessageId();
        return id.compareTo(messageId) >= 0;
    }
}
