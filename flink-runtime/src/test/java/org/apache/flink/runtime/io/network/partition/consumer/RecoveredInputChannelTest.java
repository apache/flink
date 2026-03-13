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
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecoveredInputChannel}. */
class RecoveredInputChannelTest {

    @Test
    void testConversionOnlyPossibleAfterConsumedWhenConfigDisabled() {
        // When config is disabled, toInputChannel() checks stateConsumedFuture
        assertThatThrownBy(() -> buildChannel(false).toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovered state is not fully consumed");
    }

    @Test
    void testConversionOnlyPossibleAfterFilteringWhenConfigEnabled() {
        // When config is enabled, toInputChannel() checks bufferFilteringCompleteFuture
        assertThatThrownBy(() -> buildChannel(true).toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("buffer filtering is not complete");
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
        channel.finishReadRecoveredState();
        assertThat(channel.getBufferFilteringCompleteFuture()).isDone();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should now succeed (no exception)
        InputChannel converted = channel.toInputChannel();
        assertThat(converted).isNotNull();
    }

    @Test
    void testToInputChannelAllowedWhenStateConsumedAndConfigDisabled() throws IOException {
        // When config is disabled, conversion is allowed when stateConsumedFuture is done
        TestableRecoveredInputChannel channel = buildTestableChannel(false);

        // Initially, conversion should fail
        assertThatThrownBy(() -> channel.toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovered state is not fully consumed");

        // After finishReadRecoveredState(), bufferFilteringCompleteFuture should NOT be done
        // because config is disabled
        channel.finishReadRecoveredState();
        assertThat(channel.getBufferFilteringCompleteFuture()).isNotDone();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should still fail because stateConsumedFuture is not done
        assertThatThrownBy(() -> channel.toInputChannel())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovered state is not fully consumed");

        // Consume the EndOfInputChannelStateEvent to complete stateConsumedFuture.
        // getNextBuffer() returns empty when it encounters the event internally.
        assertThat(channel.getNextBuffer()).isNotPresent();
        assertThat(channel.getStateConsumedFuture()).isDone();

        // Now conversion should succeed
        InputChannel converted = channel.toInputChannel();
        assertThat(converted).isNotNull();
    }

    @Test
    void testBufferFilteringCompleteFutureOnlyCompletesWhenConfigEnabled() throws IOException {
        // Config enabled: finishReadRecoveredState() completes bufferFilteringCompleteFuture
        RecoveredInputChannel channelEnabled = buildChannel(true);
        assertThat(channelEnabled.getBufferFilteringCompleteFuture()).isNotDone();
        channelEnabled.finishReadRecoveredState();
        assertThat(channelEnabled.getBufferFilteringCompleteFuture()).isDone();

        // Config disabled: finishReadRecoveredState() does NOT complete
        // bufferFilteringCompleteFuture
        RecoveredInputChannel channelDisabled = buildChannel(false);
        assertThat(channelDisabled.getBufferFilteringCompleteFuture()).isNotDone();
        channelDisabled.finishReadRecoveredState();
        assertThat(channelDisabled.getBufferFilteringCompleteFuture()).isNotDone();
    }

    @Test
    void testStateConsumedFutureCompletesAfterConsumingAllBuffers() throws IOException {
        // This test verifies that stateConsumedFuture completes after consuming
        // EndOfInputChannelStateEvent regardless of the config setting
        for (boolean configEnabled : new boolean[] {true, false}) {
            RecoveredInputChannel channel = buildChannel(configEnabled);

            assertThat(channel.getStateConsumedFuture()).isNotDone();

            channel.finishReadRecoveredState();
            assertThat(channel.getStateConsumedFuture()).isNotDone();

            // Consuming the EndOfInputChannelStateEvent should complete the future.
            // getNextBuffer() returns empty when it encounters the event internally.
            assertThat(channel.getNextBuffer()).isNotPresent();
            assertThat(channel.getStateConsumedFuture()).isDone();
        }
    }

    private RecoveredInputChannel buildChannel(boolean unalignedDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setUnalignedDuringRecoveryEnabled(unalignedDuringRecoveryEnabled)
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
                protected InputChannel toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers) {
                    throw new AssertionError("channel conversion succeeded");
                }
            };
        } catch (Exception e) {
            throw new AssertionError("channel creation failed", e);
        }
    }

    private TestableRecoveredInputChannel buildTestableChannel(
            boolean unalignedDuringRecoveryEnabled) {
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder()
                            .setUnalignedDuringRecoveryEnabled(unalignedDuringRecoveryEnabled)
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
        protected InputChannel toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers) {
            return new TestInputChannel(inputGate, 0);
        }
    }
}
