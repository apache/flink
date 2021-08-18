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

package org.apache.flink.connector.pulsar.source.split;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;

import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor.defaultStartCursor;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.defaultStopCursor;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarPartitionSplit}. */
class PulsarPartitionSplitTest {

    @Test
    void partitionSplitWouldHaveANewStartCursorWhenCheckpointing() {
        TopicPartition partition = new TopicPartition(randomAlphabetic(10), 1, createFullRange());

        MessageIdImpl messageId = new MessageIdImpl(12, 1, 3);
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        partition, defaultStartCursor(), defaultStopCursor(), messageId, null);

        StartCursor cursor = split.getStartCursor();
        CursorPosition position = cursor.position(split);
        MessageIdImpl newMessageId = (MessageIdImpl) position.getMessageId();

        assertEquals(messageId.getEntryId() + 1, newMessageId.getEntryId());
        assertEquals(messageId.getPartitionIndex(), newMessageId.getPartitionIndex());
        assertEquals(messageId.getLedgerId(), newMessageId.getLedgerId());
    }
}
