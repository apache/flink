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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingNettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskIOScheduler;
import org.apache.flink.util.ExceptionUtils;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool.NUM_BYTES_PER_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DiskIOScheduler}. */
class DiskIOSchedulerTest {

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    private static final int TOTAL_BYTES = 2 * NUM_BYTES_PER_REQUEST;

    private static final int BUFFER_SIZE = NUM_BYTES_PER_REQUEST / 4;

    private static final int MAX_REQUEST_BUFFER = Integer.MAX_VALUE;

    private static final Duration DEFAULT_BUFFER_REQUEST_TIMEOUT = Duration.ofMinutes(5);

    private BatchShuffleReadBufferPool bufferPool;

    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    private CompletableFuture<Integer> segmentIdFuture;

    private CompletableFuture<Void> readerReleaseFuture;

    private DiskIOScheduler diskIOScheduler;

    private List<Map<Integer, Integer>> firstBufferIndexInSegment;

    @BeforeEach
    void before() {
        this.ioExecutor = new TestingScheduledExecutorService();
        this.bufferPool = new BatchShuffleReadBufferPool(TOTAL_BYTES, BUFFER_SIZE);
        this.bufferPool.initialize();
        this.segmentIdFuture = new CompletableFuture<>();
        this.readerReleaseFuture = new CompletableFuture<>();
        this.firstBufferIndexInSegment = createFirstBufferIndexInSegment();
        this.diskIOScheduler =
                new DiskIOScheduler(
                        DEFAULT_PARTITION_ID,
                        bufferPool,
                        ioExecutor,
                        MAX_REQUEST_BUFFER,
                        DEFAULT_BUFFER_REQUEST_TIMEOUT,
                        (subpartitionId, bufferIndex) ->
                                firstBufferIndexInSegment.get(subpartitionId).get(bufferIndex),
                        new TestingPartitionFileReader.Builder()
                                .setReadBufferSupplier(
                                        (bufferIndex, segmentId) -> {
                                            if (segmentIdFuture.isDone()) {
                                                return new PartitionFileReader.ReadBufferResult(
                                                        Collections.emptyList(), true, null);
                                            }
                                            segmentIdFuture.complete(segmentId);
                                            return new PartitionFileReader.ReadBufferResult(
                                                    Collections.singletonList(
                                                            BufferBuilderTestUtils.buildSomeBuffer(
                                                                    0)),
                                                    true,
                                                    null);
                                        })
                                .setReleaseNotifier(() -> readerReleaseFuture.complete(null))
                                .setPrioritySupplier(subpartitionId -> (long) subpartitionId)
                                .build());
    }

    @AfterEach
    void after() {
        bufferPool.destroy();
    }

    @Test
    void testConnectionEstablished() {
        CompletableFuture<NettyPayload> bufferWriteNotifier = new CompletableFuture<>();
        TestingNettyConnectionWriter nettyConnectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    if (nettyPayload.getSegmentId() == -1) {
                                        bufferWriteNotifier.complete(nettyPayload);
                                    }
                                    return null;
                                })
                        .build();
        diskIOScheduler.connectionEstablished(DEFAULT_SUBPARTITION_ID, nettyConnectionWriter);
        assertThat(segmentIdFuture).isNotDone();
        assertThat(bufferWriteNotifier).isNotDone();
        ioExecutor.trigger();
        assertThat(segmentIdFuture).isCompletedWithValue(0);
        assertThat(bufferWriteNotifier).isDone();
    }

    @Test
    void testSequenceReading() {
        CompletableFuture<NettyPayload> bufferWriteNotifier1 = new CompletableFuture<>();
        CompletableFuture<NettyPayload> bufferWriteNotifier2 = new CompletableFuture<>();
        TestingNettyConnectionWriter nettyConnectionWriter1 =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    if (nettyPayload.getSegmentId() == -1) {
                                        bufferWriteNotifier1.complete(nettyPayload);
                                    }
                                    return null;
                                })
                        .build();
        TestingNettyConnectionWriter nettyConnectionWriter2 =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    if (nettyPayload.getSegmentId() == -1) {
                                        bufferWriteNotifier2.complete(nettyPayload);
                                    }
                                    return null;
                                })
                        .build();
        diskIOScheduler.connectionEstablished(
                new TieredStorageSubpartitionId(1), nettyConnectionWriter2);
        diskIOScheduler.connectionEstablished(
                new TieredStorageSubpartitionId(0), nettyConnectionWriter1);
        assertThat(bufferWriteNotifier1).isNotDone();
        assertThat(bufferWriteNotifier2).isNotDone();
        ioExecutor.trigger();
        assertThat(bufferWriteNotifier1).isDone();
        assertThat(bufferWriteNotifier2).isNotDone();
    }

    @Test
    void testConnectionBroken() {
        CompletableFuture<NettyPayload> bufferWriteNotifier = new CompletableFuture<>();
        NettyConnectionId nettyConnectionId = NettyConnectionId.newId();
        TestingNettyConnectionWriter nettyConnectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    if (nettyPayload.getSegmentId() == -1) {
                                        bufferWriteNotifier.complete(nettyPayload);
                                    }
                                    return null;
                                })
                        .setNettyConnectionIdSupplier(() -> nettyConnectionId)
                        .build();
        diskIOScheduler.connectionEstablished(DEFAULT_SUBPARTITION_ID, nettyConnectionWriter);
        diskIOScheduler.connectionBroken(nettyConnectionId);
        ioExecutor.trigger();
        assertThat(segmentIdFuture).isNotDone();
        assertThat(bufferWriteNotifier).isNotDone();
    }

    @Test
    void testRelease() {
        CompletableFuture<NettyPayload> bufferWriteNotifier = new CompletableFuture<>();
        NettyConnectionId nettyConnectionId = NettyConnectionId.newId();
        TestingNettyConnectionWriter nettyConnectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    bufferWriteNotifier.complete(nettyPayload);
                                    return null;
                                })
                        .setNettyConnectionIdSupplier(() -> nettyConnectionId)
                        .build();
        diskIOScheduler.connectionEstablished(DEFAULT_SUBPARTITION_ID, nettyConnectionWriter);
        diskIOScheduler.release();
        assertThat(readerReleaseFuture).isDone();
        assertThatThrownBy(
                        () ->
                                diskIOScheduler.connectionEstablished(
                                        DEFAULT_SUBPARTITION_ID, nettyConnectionWriter))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * The {@link DiskIOScheduler} shouldn't hold the lock when sending {@link NettyPayload} with
     * segment id to {@link NettyConnectionWriter}, otherwise there may happen a deadlock when the
     * downstream is trying to request the lock in {@link DiskIOScheduler}.
     */
    @Test
    void testDeadLock() {
        CompletableFuture<NettyPayload> waitFuture1 = new CompletableFuture<>();
        CompletableFuture<NettyPayload> waitFuture2 = new CompletableFuture<>();
        TestingNettyConnectionWriter nettyConnectionWriter =
                new TestingNettyConnectionWriter.Builder()
                        .setWriteBufferFunction(
                                nettyPayload -> {
                                    try {
                                        waitFuture2.complete(null);
                                        waitFuture1.get();
                                    } catch (InterruptedException | ExecutionException e) {
                                        ExceptionUtils.rethrow(e);
                                    }
                                    return null;
                                })
                        .build();
        // Test if consumer thread can get the lock correctly.
        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        waitFuture2.get();
                        // Get the lock in disk io scheduler.
                        diskIOScheduler.release();
                        waitFuture1.complete(null);
                    }
                };
        consumerThread.start();
        diskIOScheduler.connectionEstablished(
                new TieredStorageSubpartitionId(0), nettyConnectionWriter);
        ioExecutor.trigger();
        assertThat(waitFuture1).isDone();
        assertThat(waitFuture2).isDone();
    }

    /**
     * The executor service for batch shuffle read shouldn't throw {@link
     * RejectedExecutionException} if the worker thread is attempting to add new jobs when the
     * service is already shutdown. This behavior is important to ensure graceful handling of
     * scenarios where worker threads couldn't be immediately aware of the TaskManager's shutdown
     * status.
     */
    @Test
    void testRejectedExecutionIsIgnoredOnShutdown() {
        TestingNettyConnectionWriter nettyConnectionWriter =
                new TestingNettyConnectionWriter.Builder().build();
        diskIOScheduler.connectionEstablished(DEFAULT_SUBPARTITION_ID, nettyConnectionWriter);
        assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);
        assertThatNoException().isThrownBy(() -> ioExecutor.trigger());
        assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);
        ioExecutor.shutdown();
        assertThatNoException().isThrownBy(() -> ioExecutor.trigger());
    }

    private List<Map<Integer, Integer>> createFirstBufferIndexInSegment() {
        Map<Integer, Integer> firstBufferIndexInSegment0 = new HashMap<>();
        Map<Integer, Integer> firstBufferIndexInSegment1 = new HashMap<>();
        firstBufferIndexInSegment0.put(0, 0);
        firstBufferIndexInSegment1.put(0, 0);
        List<Map<Integer, Integer>> list = new ArrayList<>();
        list.add(firstBufferIndexInSegment0);
        list.add(firstBufferIndexInSegment1);
        return list;
    }

    /**
     * This manually triggered executor service will throw {@link RejectedExecutionException} if the
     * new job is added when the service shuts down.
     */
    private static class TestingScheduledExecutorService
            extends ManuallyTriggeredScheduledExecutorService {
        @Override
        @NotNull
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            if (super.isTerminated()) {
                throw new RejectedExecutionException();
            }
            return super.schedule(command, delay, unit);
        }
    }
}
