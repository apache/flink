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

import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator.KeySharedMode.JOIN;
import static org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer.INSTANCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link PulsarPartitionSplitSerializer}. */
class PulsarPartitionSplitSerializerTest {

    @Test
    void version1SerializeAndDeserialize() throws Exception {
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        new TopicPartition(
                                randomAlphabetic(10),
                                10,
                                singletonList(new TopicRange(400, 5000)),
                                JOIN),
                        StopCursor.defaultStopCursor());

        byte[] bytes = INSTANCE.serialize(split);
        PulsarPartitionSplit split1 = INSTANCE.deserialize(1, bytes);

        assertEquals(split, split1);
        assertNotSame(split, split1);
    }

    @Test
    void version0Deserialize() throws Exception {
        DataOutputSerializer serializer = new DataOutputSerializer(4096);
        // Serialize in version 0 logic.
        serializer.writeUTF("topic44");
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
        byte[] bytes = serializer.getSharedBuffer();

        PulsarPartitionSplit split = INSTANCE.deserialize(0, bytes);
        PulsarPartitionSplit expectedSplit =
                new PulsarPartitionSplit(
                        new TopicPartition("topic44", 2, singletonList(new TopicRange(400, 5000))),
                        StopCursor.latest(),
                        MessageId.latest,
                        new TxnID(1000, 2000));

        assertEquals(split, expectedSplit);
        assertNotSame(split, expectedSplit);
    }
}
