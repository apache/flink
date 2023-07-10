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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingNettyConnectionWriter;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MemoryTierSubpartitionProducerAgent}. */
class MemoryTierSubpartitionProducerAgentTest {

    @Test
    void testConnectionEstablished() {
        MemoryTierSubpartitionProducerAgent subpartitionProducerAgent =
                createSubpartitionProducerAgent();

        assertThatThrownBy(
                        () ->
                                subpartitionProducerAgent.addFinishedBuffer(
                                        BufferBuilderTestUtils.buildSomeBuffer()))
                .isInstanceOf(NullPointerException.class);
        subpartitionProducerAgent.connectionEstablished(
                new TestingNettyConnectionWriter.Builder().build());
        assertThatCode(
                        () ->
                                subpartitionProducerAgent.addFinishedBuffer(
                                        BufferBuilderTestUtils.buildSomeBuffer()))
                .doesNotThrowAnyException();
    }

    @Test
    void testAddFinishedBuffer() {
        MemoryTierSubpartitionProducerAgent subpartitionProducerAgent =
                createSubpartitionProducerAgent();

        AtomicReference<NettyPayload> received = new AtomicReference<>();
        TestingNettyConnectionWriter connectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                buffer -> {
                                    received.set(buffer);
                                    return null;
                                })
                        .build();
        subpartitionProducerAgent.connectionEstablished(connectionWriter);

        Buffer sentBuffer = BufferBuilderTestUtils.buildSomeBuffer();
        subpartitionProducerAgent.addFinishedBuffer(sentBuffer);
        Optional<Buffer> receivedBuffer = received.get().getBuffer();

        assertThat(receivedBuffer).isPresent();
        assertThat(receivedBuffer.get()).isEqualTo(sentBuffer);
    }

    @Test
    void testUpdateSegmentId() {
        int segmentId = 1;

        MemoryTierSubpartitionProducerAgent subpartitionProducerAgent =
                createSubpartitionProducerAgent();

        AtomicReference<NettyPayload> received = new AtomicReference<>();
        TestingNettyConnectionWriter connectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                buffer -> {
                                    received.set(buffer);
                                    return null;
                                })
                        .build();
        subpartitionProducerAgent.connectionEstablished(connectionWriter);
        subpartitionProducerAgent.updateSegmentId(segmentId);

        assertThat(received.get().getSegmentId()).isEqualTo(segmentId);
    }

    @Test
    void testRelease() {
        MemoryTierSubpartitionProducerAgent subpartitionProducerAgent =
                createSubpartitionProducerAgent();

        AtomicBoolean isClosed = new AtomicBoolean(false);
        TestingNettyConnectionWriter connectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setCloseFunction(
                                throwable -> {
                                    isClosed.set(true);
                                    return null;
                                })
                        .build();
        subpartitionProducerAgent.connectionEstablished(connectionWriter);
        subpartitionProducerAgent.release();

        assertThat(isClosed).isTrue();
    }

    private static MemoryTierSubpartitionProducerAgent createSubpartitionProducerAgent() {
        return new MemoryTierSubpartitionProducerAgent(0);
    }
}
