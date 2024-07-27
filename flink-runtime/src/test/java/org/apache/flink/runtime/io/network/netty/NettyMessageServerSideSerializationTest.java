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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes
 * sent from client side to server side.
 */
class NettyMessageServerSideSerializationTest {

    private final Random random = new Random();

    private EmbeddedChannel channel;

    @BeforeEach
    void setup() {
        channel =
                new EmbeddedChannel(
                        new NettyMessage.NettyMessageEncoder(), // For outbound messages
                        new NettyMessage.NettyMessageDecoder()); // For inbound messages
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.close();
        }
    }

    @Test
    void testPartitionRequest() {
        int queueIndex = random.nextInt(Integer.MAX_VALUE);
        NettyMessage.PartitionRequest expected =
                new NettyMessage.PartitionRequest(
                        new ResultPartitionID(),
                        new ResultSubpartitionIndexSet(queueIndex),
                        new InputChannelID(),
                        random.nextInt());

        NettyMessage.PartitionRequest actual = encodeAndDecode(expected, channel);

        assertThat(actual.partitionId).isEqualTo(expected.partitionId);
        assertThat(actual.queueIndexSet).isEqualTo(expected.queueIndexSet);
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
        assertThat(actual.credit).isEqualTo(expected.credit);
    }

    @Test
    void testTaskEventRequest() {
        NettyMessage.TaskEventRequest expected =
                new NettyMessage.TaskEventRequest(
                        new IntegerTaskEvent(random.nextInt()),
                        new ResultPartitionID(),
                        new InputChannelID());
        NettyMessage.TaskEventRequest actual = encodeAndDecode(expected, channel);

        assertThat(actual.event).isEqualTo(expected.event);
        assertThat(actual.partitionId).isEqualTo(expected.partitionId);
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    @Test
    void testCancelPartitionRequest() {
        NettyMessage.CancelPartitionRequest expected =
                new NettyMessage.CancelPartitionRequest(new InputChannelID());
        NettyMessage.CancelPartitionRequest actual = encodeAndDecode(expected, channel);

        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    @Test
    void testCloseRequest() {
        NettyMessage.CloseRequest expected = new NettyMessage.CloseRequest();
        NettyMessage.CloseRequest actual = encodeAndDecode(expected, channel);

        assertThat(actual).isExactlyInstanceOf(expected.getClass());
    }

    @Test
    void testAddCredit() {
        NettyMessage.AddCredit expected =
                new NettyMessage.AddCredit(
                        random.nextInt(Integer.MAX_VALUE) + 1, new InputChannelID());
        NettyMessage.AddCredit actual = encodeAndDecode(expected, channel);

        assertThat(actual.credit).isEqualTo(expected.credit);
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    @Test
    void testResumeConsumption() {
        NettyMessage.ResumeConsumption expected =
                new NettyMessage.ResumeConsumption(new InputChannelID());
        NettyMessage.ResumeConsumption actual = encodeAndDecode(expected, channel);

        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    @Test
    void testAckAllUserRecordsProcessed() {
        NettyMessage.AckAllUserRecordsProcessed expected =
                new NettyMessage.AckAllUserRecordsProcessed(new InputChannelID());
        NettyMessage.AckAllUserRecordsProcessed actual = encodeAndDecode(expected, channel);

        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    @Test
    void testNewBufferSize() {
        NettyMessage.NewBufferSize expected =
                new NettyMessage.NewBufferSize(
                        random.nextInt(Integer.MAX_VALUE), new InputChannelID());
        NettyMessage.NewBufferSize actual = encodeAndDecode(expected, channel);

        assertThat(actual.bufferSize).isEqualTo(expected.bufferSize);
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }
}
