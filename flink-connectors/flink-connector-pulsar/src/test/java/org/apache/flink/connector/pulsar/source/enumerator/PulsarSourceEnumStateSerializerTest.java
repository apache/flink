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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumStateSerializer.INSTANCE;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link PulsarSourceEnumStateSerializer}. */
class PulsarSourceEnumStateSerializerTest {

    @Test
    void version2SerializeAndDeserialize() throws Exception {
        Set<TopicPartition> partitions =
                Sets.newHashSet(
                        new TopicPartition(
                                randomAlphabetic(10), 2, singletonList(new TopicRange(1, 30))),
                        new TopicPartition(
                                randomAlphabetic(10), 1, singletonList(createFullRange())));

        PulsarSourceEnumState state = new PulsarSourceEnumState(partitions);

        byte[] bytes = INSTANCE.serialize(state);
        PulsarSourceEnumState state1 = INSTANCE.deserialize(2, bytes);

        assertEquals(state.getAppendedPartitions(), state1.getAppendedPartitions());
        assertNotSame(state, state1);
    }

    @Test
    void version1Deserialize() throws Exception {
        // Serialize in version 1 logic.
        DataOutputSerializer serializer = new DataOutputSerializer(4096);
        serializer.writeInt(2);
        serializer.writeUTF("topic1");
        serializer.writeInt(0);
        serializer.writeInt(300);
        serializer.writeInt(4000);
        serializer.writeUTF("topic2");
        serializer.writeInt(4);
        serializer.writeInt(600);
        serializer.writeInt(8000);
        byte[] bytes = serializer.getSharedBuffer();

        PulsarSourceEnumState state = INSTANCE.deserialize(1, bytes);
        Set<TopicPartition> partitions = state.getAppendedPartitions();
        Set<TopicPartition> expectedPartitions =
                Sets.newHashSet(
                        new TopicPartition("topic1", 0, singletonList(new TopicRange(300, 4000))),
                        new TopicPartition("topic2", 4, singletonList(new TopicRange(600, 8000))));

        assertEquals(partitions, expectedPartitions);
    }

    @Test
    void version0Deserialize() throws Exception {
        DataOutputSerializer serializer = new DataOutputSerializer(4096);
        // Serialize in version 0 logic.
        serializer.writeInt(2);
        serializer.writeUTF("topic3");
        serializer.writeInt(5);
        serializer.writeInt(600);
        serializer.writeInt(2000);
        serializer.writeUTF("topic4");
        serializer.writeInt(8);
        serializer.writeInt(300);
        serializer.writeInt(1000);
        serializeVersion0SplitSet(serializer);
        serializer.writeInt(1);
        serializer.writeInt(3);
        serializeVersion0SplitSet(serializer);
        serializer.writeInt(1);
        serializer.writeInt(1);
        serializer.writeInt(1);
        serializer.writeUTF("some-topic");
        serializer.writeBoolean(true);
        byte[] bytes = serializer.getSharedBuffer();

        PulsarSourceEnumState state = INSTANCE.deserialize(0, bytes);
        Set<TopicPartition> partitions = state.getAppendedPartitions();
        Set<TopicPartition> expectedPartitions =
                Sets.newHashSet(
                        new TopicPartition("topic3", 5, singletonList(new TopicRange(600, 2000))),
                        new TopicPartition("topic4", 8, singletonList(new TopicRange(300, 1000))));

        assertEquals(partitions, expectedPartitions);
    }

    private void serializeVersion0SplitSet(DataOutputSerializer serializer) throws IOException {
        serializer.writeInt(1);
        serializer.writeUTF("topic" + randomNumeric(2));
        serializer.writeInt(2);
        serializer.writeInt(400);
        serializer.writeInt(5000);
        byte[] stopCursorBytes = InstantiationUtil.serializeObject(StopCursor.latest());
        serializer.writeInt(stopCursorBytes.length);
        serializer.write(stopCursorBytes);
        serializer.writeBoolean(true);
        byte[] messageIdBytes = MessageId.latest.toByteArray();
        serializer.writeInt(messageIdBytes.length);
        serializer.write(messageIdBytes);
        serializer.writeBoolean(true);
        serializer.writeLong(1000);
        serializer.writeLong(2000);
    }
}
