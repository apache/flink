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
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecoveredInputChannel}. */
class RecoveredInputChannelTest {

    @Test
    void testConversionOnlyPossibleAfterBufferFilteringComplete() {
        // toInputChannel() always checks bufferFilteringCompleteFuture regardless of config
        for (boolean configEnabled : new boolean[] {true, false}) {
            assertThatThrownBy(() -> buildChannel(configEnabled).toInputChannel())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("buffer filtering is not complete");
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
    void testToInputChannelAllowedWhenBufferFilteringCompleteAndConfigEnabled() throws IOException {
        // When config is enabled, conversion is allowed when bufferFilteringCompleteFuture is done
        TestableRecoveredInputChannel channel = buildTestableChannel(true);

        // Initially, conversion should fail
        assertThatThrownBy(() -> channel.toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("buffer filtering is not complete");

        // After finishReadRecoveredState(), bufferFilteringCompleteFuture should be done
        synchronized (channel.inputGate.getGateLock()) {
            channel.finishReadRecoveredState();
        }
        assertThat(channel.getBufferFilteringCompleteFuture()).isDone();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should now succeed (no exception)
        InputChannel converted = channel.toInputChannel();
        assertThat(converted).isNotNull();
    }

    @Test
    void testToInputChannelAllowedWhenStateConsumedAndConfigDisabled() throws IOException {
        // When config is disabled, conversion requires both bufferFilteringCompleteFuture
        // and stateConsumedFuture to be done
        TestableRecoveredInputChannel channel = buildTestableChannel(false);

        // Initially, conversion should fail (buffer filtering not complete)
        assertThatThrownBy(() -> channel.toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("buffer filtering is not complete");

        // After finishReadRecoveredState(), bufferFilteringCompleteFuture is done
        // but stateConsumedFuture is not
        synchronized (channel.inputGate.getGateLock()) {
            channel.finishReadRecoveredState();
        }
        assertThat(channel.getBufferFilteringCompleteFuture()).isDone();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should still fail because stateConsumedFuture is not done
        assertThatThrownBy(() -> channel.toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovered state is not fully consumed");

        // Consume the EndOfInputChannelStateEvent to complete stateConsumedFuture
        synchronized (channel.inputGate.getGateLock()) {
            assertThat(channel.getNextBuffer()).isNotPresent();
        }
        assertThat(channel.getStateConsumedFuture()).isDone();

        // Now conversion should succeed
        InputChannel converted = channel.toInputChannel();
        assertThat(converted).isNotNull();
    }

    @Test
    void testBufferFilteringCompleteFutureAlwaysCompletes() throws IOException {
        // finishReadRecoveredState() unconditionally completes bufferFilteringCompleteFuture
        for (boolean configEnabled : new boolean[] {true, false}) {
            RecoveredInputChannel channel = buildChannel(configEnabled);
            assertThat(channel.getBufferFilteringCompleteFuture()).isNotDone();
            synchronized (channel.inputGate.getGateLock()) {
                channel.finishReadRecoveredState();
            }
            assertThat(channel.getBufferFilteringCompleteFuture()).isDone();
        }
    }

    @Test
    void testStateConsumedFutureCompletesAfterConsumingAllBuffers() throws IOException {
        // This test verifies that stateConsumedFuture completes after consuming
        // EndOfInputChannelStateEvent regardless of the config setting
        for (boolean configEnabled : new boolean[] {true, false}) {
            RecoveredInputChannel channel = buildChannel(configEnabled);

            assertThat(channel.getStateConsumedFuture()).isNotDone();

            synchronized (channel.inputGate.getGateLock()) {
                channel.finishReadRecoveredState();
            }
            assertThat(channel.getStateConsumedFuture()).isNotDone();

            // Consuming the EndOfInputChannelStateEvent should complete the future.
            // getNextBuffer() returns empty when it encounters the event internally.
            synchronized (channel.inputGate.getGateLock()) {
                assertThat(channel.getNextBuffer()).isNotPresent();
            }
            assertThat(channel.getStateConsumedFuture()).isDone();
        }
    }

    @Test
    void testRequestBufferNonBlockingAndBlockingHasNoHeapFallback() throws Exception {
        int numBuffers = 3;
        NettyShuffleEnvironment environment =
                new NettyShuffleEnvironmentBuilder()
                        .setNumNetworkBuffers(numBuffers)
                        .setBufferSize(MemoryManager.DEFAULT_PAGE_SIZE)
                        .build();
        try {
            SingleInputGate filteringGate =
                    new SingleInputGateBuilder()
                            .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                            .setupBufferPoolFactory(environment)
                            .setCheckpointingDuringRecoveryEnabled(true)
                            .build();
            filteringGate.setup();

            RecoveredInputChannel channel = (RecoveredInputChannel) filteringGate.getChannel(0);

            // requestBuffer() is non-blocking: drain exclusive buffers, then null is returned.
            List<Buffer> allBuffers = new ArrayList<>();
            while (true) {
                Buffer b = channel.requestBuffer();
                if (b == null) {
                    break;
                }
                allBuffers.add(b);
            }
            assertThat(channel.requestBuffer()).isNull();

            // Also drain the gate's floating buffer pool so requestBufferBlocking() has nothing
            // left and is forced to block.
            BufferPool bufferPool = filteringGate.getBufferPool();
            while (true) {
                Buffer b = bufferPool.requestBuffer();
                if (b == null) {
                    break;
                }
                allBuffers.add(b);
            }

            // requestBufferBlocking() must block (not fall back to heap) when the pool is empty.
            CompletableFuture<Buffer> blockingFuture = new CompletableFuture<>();
            Thread blockingThread =
                    new Thread(
                            () -> {
                                try {
                                    blockingFuture.complete(channel.requestBufferBlocking());
                                } catch (Exception e) {
                                    blockingFuture.completeExceptionally(e);
                                }
                            });
            blockingThread.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (blockingThread.getState() != Thread.State.WAITING
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(blockingFuture.isDone()).isFalse();

            // Recycle one buffer; blocking thread then gets a pool buffer.
            allBuffers.remove(0).recycleBuffer();
            Buffer poolBuffer = blockingFuture.get(5, TimeUnit.SECONDS);
            assertThat(poolBuffer).isNotNull();
            poolBuffer.recycleBuffer();

            for (Buffer b : allBuffers) {
                b.recycleBuffer();
            }
            blockingThread.join(5000);
            filteringGate.close();
        } finally {
            environment.close();
        }
    }

    private RecoveredInputChannel buildChannel(boolean checkpointingDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setCheckpointingDuringRecoveryEnabled(
                                    checkpointingDuringRecoveryEnabled)
                            .build();
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
                protected InputChannel toInputChannelInternal(
                        RecoveredBufferStoreImpl recoveredStore) {
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
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setCheckpointingDuringRecoveryEnabled(
                                    checkpointingDuringRecoveryEnabled)
                            .build();
            return new TestableRecoveredInputChannel(inputGate);
        } catch (Exception e) {
            throw new AssertionError("channel creation failed", e);
        }
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
        protected InputChannel toInputChannelInternal(RecoveredBufferStoreImpl recoveredStore) {
            return new TestInputChannel(inputGate, 0);
        }
    }
}
