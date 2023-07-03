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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TieredStorageResultSubpartitionView}. */
public class TieredStorageResultSubpartitionViewTest {

    private static final int TIER_NUMBER = 2;

    private CompletableFuture<Void> availabilityListener;

    private List<Queue<NettyPayload>> nettyPayloadQueues;

    private List<CompletableFuture<NettyConnectionId>> connectionBrokenConsumers;

    private TieredStorageResultSubpartitionView tieredStorageResultSubpartitionView;

    @BeforeEach
    void before() {
        availabilityListener = new CompletableFuture<>();
        nettyPayloadQueues = createNettyPayloadQueues();
        connectionBrokenConsumers =
                Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>());
        tieredStorageResultSubpartitionView =
                new TieredStorageResultSubpartitionView(
                        createBufferAvailabilityListener(availabilityListener),
                        nettyPayloadQueues,
                        createNettyConnectionIds(),
                        createNettyServiceProducers(connectionBrokenConsumers));
    }

    @Test
    void testGetNextBuffer() throws IOException {
        checkBufferAndBacklog(tieredStorageResultSubpartitionView.getNextBuffer(), 1);
        checkBufferAndBacklog(tieredStorageResultSubpartitionView.getNextBuffer(), 0);
        tieredStorageResultSubpartitionView.notifyRequiredSegmentId(1);
        assertThat(availabilityListener).isDone();
        checkBufferAndBacklog(tieredStorageResultSubpartitionView.getNextBuffer(), 1);
        checkBufferAndBacklog(tieredStorageResultSubpartitionView.getNextBuffer(), 0);
        assertThat(tieredStorageResultSubpartitionView.getNextBuffer()).isNull();
    }

    @Test
    void testGetNextBufferFailed() {
        Throwable expectedError = new IOException();
        nettyPayloadQueues = createNettyPayloadQueuesWithError(expectedError);
        tieredStorageResultSubpartitionView =
                new TieredStorageResultSubpartitionView(
                        createBufferAvailabilityListener(availabilityListener),
                        nettyPayloadQueues,
                        createNettyConnectionIds(),
                        createNettyServiceProducers(connectionBrokenConsumers));
        assertThatThrownBy(tieredStorageResultSubpartitionView::getNextBuffer)
                .hasCause(expectedError);
        assertThat(connectionBrokenConsumers.get(0)).isDone();
    }

    @Test
    void testGetAvailabilityAndBacklog() {
        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog1 =
                tieredStorageResultSubpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog1.getBacklog()).isEqualTo(3);
        assertThat(availabilityAndBacklog1.isAvailable()).isEqualTo(false);
        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog2 =
                tieredStorageResultSubpartitionView.getAvailabilityAndBacklog(2);
        assertThat(availabilityAndBacklog2.getBacklog()).isEqualTo(3);
        assertThat(availabilityAndBacklog2.isAvailable()).isEqualTo(true);
    }

    @Test
    void testNotifyRequiredSegmentId() {
        tieredStorageResultSubpartitionView.notifyRequiredSegmentId(1);
        assertThat(availabilityListener).isDone();
    }

    @Test
    void testReleaseAllResources() throws IOException {
        tieredStorageResultSubpartitionView.releaseAllResources();
        assertThat(nettyPayloadQueues.get(0)).hasSize(0);
        assertThat(nettyPayloadQueues.get(1)).hasSize(0);
        assertThat(connectionBrokenConsumers.get(0)).isDone();
        assertThat(connectionBrokenConsumers.get(1)).isDone();
        assertThat(tieredStorageResultSubpartitionView.isReleased()).isTrue();
    }

    @Test
    void testGetNumberOfQueuedBuffers() {
        assertThat(tieredStorageResultSubpartitionView.getNumberOfQueuedBuffers()).isEqualTo(3);
        assertThat(tieredStorageResultSubpartitionView.unsynchronizedGetNumberOfQueuedBuffers())
                .isEqualTo(3);
    }

    private static void checkBufferAndBacklog(BufferAndBacklog bufferAndBacklog, int backlog) {
        assertThat(bufferAndBacklog).isNotNull();
        assertThat(bufferAndBacklog.buffer()).isNotNull();
        assertThat(bufferAndBacklog.buffersInBacklog()).isEqualTo(backlog);
    }

    private static BufferAvailabilityListener createBufferAvailabilityListener(
            CompletableFuture<Void> notifier) {
        return () -> notifier.complete(null);
    }

    private static List<Queue<NettyPayload>> createNettyPayloadQueues() {
        List<Queue<NettyPayload>> nettyPayloadQueues = new ArrayList<>();
        for (int index = 0; index < TIER_NUMBER; ++index) {
            Queue<NettyPayload> queue = new ArrayDeque<>();
            queue.add(NettyPayload.newSegment(index));
            queue.add(NettyPayload.newBuffer(BufferBuilderTestUtils.buildSomeBuffer(0), 0, index));
            queue.add(
                    NettyPayload.newBuffer(
                            new NetworkBuffer(
                                    MemorySegmentFactory.allocateUnpooledSegment(0),
                                    FreeingBufferRecycler.INSTANCE,
                                    END_OF_SEGMENT),
                            1,
                            index));
            nettyPayloadQueues.add(queue);
        }
        return nettyPayloadQueues;
    }

    private static List<Queue<NettyPayload>> createNettyPayloadQueuesWithError(Throwable error) {
        List<Queue<NettyPayload>> nettyPayloadQueues = new ArrayList<>();
        for (int index = 0; index < TIER_NUMBER; ++index) {
            Queue<NettyPayload> queue = new ArrayDeque<>();
            queue.add(NettyPayload.newSegment(index));
            queue.add(NettyPayload.newError(error));
            nettyPayloadQueues.add(queue);
        }
        return nettyPayloadQueues;
    }

    private static List<NettyConnectionId> createNettyConnectionIds() {
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();
        for (int index = 0; index < TIER_NUMBER; ++index) {
            nettyConnectionIds.add(NettyConnectionId.newId());
        }
        return nettyConnectionIds;
    }

    private static List<NettyServiceProducer> createNettyServiceProducers(
            List<CompletableFuture<NettyConnectionId>> connectionBrokenConsumers) {
        List<NettyServiceProducer> nettyServiceProducers = new ArrayList<>();
        for (int index = 0; index < connectionBrokenConsumers.size(); ++index) {
            int indexNumber = index;
            nettyServiceProducers.add(
                    new TestingNettyServiceProducer.Builder()
                            .setConnectionBrokenConsumer(
                                    connectionId ->
                                            connectionBrokenConsumers
                                                    .get(indexNumber)
                                                    .complete(connectionId))
                            .build());
        }
        return nettyServiceProducers;
    }
}
