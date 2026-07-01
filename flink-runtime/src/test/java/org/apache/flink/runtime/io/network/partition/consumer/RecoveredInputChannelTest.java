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
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RecoveredInputChannel}. */
class RecoveredInputChannelTest {

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
        // When config is enabled, conversion is allowed after finishReadRecoveredState()
        // without requiring stateConsumedFuture to be done.
        TestableRecoveredInputChannel channel = buildTestableChannel(true);

        channel.finishReadRecoveredState();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should now succeed (no exception)
        InputChannel converted = channel.toInputChannel(true);
        assertThat(converted).isNotNull();
    }

    @Test
    void testToInputChannelAllowedWhenStateConsumedAndConfigDisabled() throws IOException {
        // When config is disabled, conversion requires stateConsumedFuture to be done.
        TestableRecoveredInputChannel channel = buildTestableChannel(false);

        channel.finishReadRecoveredState();
        assertThat(channel.getStateConsumedFuture()).isNotDone();

        // Conversion should fail because stateConsumedFuture is not done
        assertThatThrownBy(() -> channel.toInputChannel(true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recovered state is not fully consumed");

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

    @Test
    void testStateConsumedFutureDoesNotCompleteWithoutLegacySentinel() throws IOException {
        RecoveredInputChannel channel = buildChannel(true);

        channel.finishReadRecoveredState();

        assertThat(channel.getNextBuffer()).isNotPresent();
        assertThat(channel.getStateConsumedFuture()).isNotDone();
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
        protected InputChannel toInputChannelInternal(boolean needsRecovery) {
            return new TestInputChannel(inputGate, 0);
        }
    }
}
