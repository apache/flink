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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.connector.pulsar.testutils.extension.SubType;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.TestTemplate;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyAdmin;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;
import static org.apache.pulsar.client.api.Schema.STRING;

/** Unit tests for {@link PulsarOrderedPartitionSplitReaderTest}. */
class PulsarOrderedPartitionSplitReaderTest extends PulsarPartitionSplitReaderTestBase {

    @SubType SubscriptionType subscriptionType = SubscriptionType.Failover;

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesWithoutSeek(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseLatestStartCursorWithoutSeek(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0, MessageId.latest);
        fetchedMessages(splitReader, 0, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseEarliestStartCursorWithoutSeek(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        handleSplit(splitReader, topicName, 0, MessageId.earliest);
        fetchedMessages(splitReader, NUM_RECORDS_PER_PARTITION, true);
    }

    @TestTemplate
    void consumeMessageCreatedBeforeHandleSplitsChangesAndUseSecondLastMessageWithoutSeek(
            PulsarPartitionSplitReaderBase<String> splitReader) {
        String topicName = randomAlphabetic(10);
        operator().setupTopic(topicName, STRING, () -> randomAlphabetic(10));
        MessageIdImpl lastMessageId =
                (MessageIdImpl)
                        sneakyAdmin(
                                () ->
                                        operator()
                                                .admin()
                                                .topics()
                                                .getLastMessageId(
                                                        topicNameWithPartition(topicName, 0)));
        // when recover, use exclusive startCursor
        handleSplit(
                splitReader,
                topicName,
                0,
                new MessageIdImpl(
                        lastMessageId.getLedgerId(),
                        lastMessageId.getEntryId() - 1,
                        lastMessageId.getPartitionIndex()));
        fetchedMessages(splitReader, 1, true);
    }
}
