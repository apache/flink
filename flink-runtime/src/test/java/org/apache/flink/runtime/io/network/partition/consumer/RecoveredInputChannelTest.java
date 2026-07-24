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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecoveredInputChannel}. */
class RecoveredInputChannelTest {

    private NetworkBufferPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.destroy();
            pool = null;
        }
    }

    @Test
    void testRequestPartitionsImpossible() {
        assertThatThrownBy(() -> buildChannel(false).requestSubpartitions())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCheckpointStartImpossible() {
        assertThatThrownBy(
                        () ->
                                buildChannel(false)
                                        .checkpointStarted(
                                                new CheckpointBarrier(
                                                        0L,
                                                        0L,
                                                        unaligned(
                                                                CheckpointType.CHECKPOINT,
                                                                getDefault()))))
                .isInstanceOf(CheckpointException.class);
    }

    @Test
    void testToInputChannelRejectedWhileRecoveredStateUnconsumed() throws IOException {
        // Conversion is rejected while recovered state is still queued: finishReadRecoveredState()
        // enqueues the EndOfInputChannelStateEvent sentinel, so receivedBuffers is non-empty until
        // it is consumed. The empty-queue check thus also guarantees stateConsumedFuture is done.
        TestableRecoveredInputChannel channel = buildTestableChannel(false);

        channel.finishReadRecoveredState();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion fails because the sentinel is still queued.
        assertThatThrownBy(() -> channel.toInputChannel(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Received buffer should be empty");

        // Consuming the EndOfInputChannelStateEvent should complete the future.
        // getNextBuffer() returns empty when it encounters the event internally.
        assertThat(channel.getNextBuffer()).isNotPresent();
        assertThat(channel.getStateConsumedFuture()).isDone();

        // Now conversion should succeed
        InputChannel converted = channel.toInputChannel(true);
        assertThat(converted).isNotNull();
    }

    @Test
    void testToInputChannelRequiresEmptyRecoveredBuffers() throws IOException {
        TestableRecoveredInputChannel channel = buildTestableChannel(true);

        channel.onRecoveredStateBuffer(BufferBuilderTestUtils.buildSomeBuffer());
        channel.finishReadRecoveredState();

        assertThatThrownBy(() -> channel.toInputChannel(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Received buffer should be empty");
    }

    @Test
    void testStateConsumedFutureCompletesAfterLegacySentinelIsConsumed() throws IOException {
        RecoveredInputChannel channel = buildChannel(false);

        assertThat(channel.getStateConsumedFuture()).isNotDone();

        channel.finishReadRecoveredState();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        assertThat(channel.getNextBuffer()).isNotPresent();
        assertThat(channel.getStateConsumedFuture()).isDone();
    }

    private RecoveredInputChannel buildChannel(boolean checkpointingDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate = new SingleInputGateBuilder().build();
            return new RecoveredInputChannel(
                    inputGate,
                    0,
                    new ResultPartitionID(),
                    new ResultSubpartitionIndexSet(0),
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    10) {
                @Override
                protected InputChannel toInputChannelInternal(boolean needsRecovery) {
                    throw new AssertionError("channel conversion succeeded");
                }
            };
        } catch (Exception e) {
            throw new AssertionError("channel creation failed", e);
        }
    }

    private TestableRecoveredInputChannel buildTestableChannel(
            boolean checkpointingDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate = new SingleInputGateBuilder().build();
            return new TestableRecoveredInputChannel(inputGate);
        } catch (Exception e) {
            throw new AssertionError("channel creation failed", e);
        }
    }

    @Test
    void testBufferPoolExhaustedBlocksRatherThanHeapAllocate() throws Exception {
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);
        RecoveredInputChannel channel = buildPooledChannel(pool, totalSegments);

        for (int i = 0; i < totalSegments; i++) {
            channel.requestBufferBlocking();
        }

        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Buffer> result = new AtomicReference<>();
        Thread blocker =
                new Thread(
                        () -> {
                            try {
                                entered.countDown();
                                result.set(channel.requestBufferBlocking());
                            } catch (Exception ignored) {
                                // Thread will be interrupted at teardown.
                            }
                        },
                        "blocking-requester");
        blocker.start();

        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);
        assertThat(result.get()).as("buffer should not have been allocated").isNull();

        blocker.interrupt();
        blocker.join(5_000);
    }

    @Test
    void testFilterOnPathTakesSameRouteAsFilterOff() throws Exception {
        int exclusivePerChannel = 1;
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);

        Buffer filterOnBuf = buildPooledChannel(pool, exclusivePerChannel).requestBufferBlocking();
        Buffer filterOffBuf = buildPooledChannel(pool, exclusivePerChannel).requestBufferBlocking();

        // Both must come from the pool — the BufferManager-owned recycler, not the
        // FreeingBufferRecycler the heap-fallback used.
        assertThat(filterOnBuf.getMemorySegment()).isNotNull();
        assertThat(filterOffBuf.getMemorySegment()).isNotNull();
        assertThat(filterOnBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");
        assertThat(filterOffBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");

        filterOnBuf.recycleBuffer();
        filterOffBuf.recycleBuffer();
    }

    private RecoveredInputChannel buildPooledChannel(
            NetworkBufferPool segmentProvider, int exclusivePerChannel) {
        SingleInputGate inputGate =
                new SingleInputGateBuilder().setSegmentProvider(segmentProvider).build();
        return new RecoveredInputChannel(
                inputGate,
                0,
                new ResultPartitionID(),
                new ResultSubpartitionIndexSet(0),
                0,
                0,
                new SimpleCounter(),
                new SimpleCounter(),
                exclusivePerChannel) {
            @Override
            protected InputChannel toInputChannelInternal(boolean needsRecovery) {
                throw new AssertionError("not expected during this test");
            }
        };
    }

    /**
     * A RecoveredInputChannel that returns a TestInputChannel when converted, for testing purposes.
     */
    private static class TestableRecoveredInputChannel extends RecoveredInputChannel {
        TestableRecoveredInputChannel(SingleInputGate inputGate) {
            super(
                    inputGate,
                    0,
                    new ResultPartitionID(),
                    new ResultSubpartitionIndexSet(0),
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    10);
        }

        @Override
        protected InputChannel toInputChannelInternal(boolean needsRecovery) {
            return new TestInputChannel(inputGate, 0);
        }
    }
}
