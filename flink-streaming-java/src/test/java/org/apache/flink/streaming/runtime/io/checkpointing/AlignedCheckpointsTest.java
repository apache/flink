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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.streaming.runtime.io.MockInputGate;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.runtime.io.checkpointing.UnalignedCheckpointsTest.addSequence;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the behavior of aligned checkpoints. */
class AlignedCheckpointsTest {

    protected static final int PAGE_SIZE = 512;

    private static final Random RND = new Random();

    private static int sizeCounter = 1;

    CheckpointedInputGate inputGate;

    static long testStartTimeNanos;

    private MockInputGate mockInputGate;

    @BeforeEach
    void setUp() {
        testStartTimeNanos = System.nanoTime();
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels, AbstractInvokable toNotify) throws IOException {
        final NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build();
        SingleInputGate gate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfChannels)
                        .setupBufferPoolFactory(environment)
                        .build();
        gate.setInputChannels(
                IntStream.range(0, numberOfChannels)
                        .mapToObj(
                                channelIndex ->
                                        InputChannelBuilder.newBuilder()
                                                .setChannelIndex(channelIndex)
                                                .setupFromNettyShuffleEnvironment(environment)
                                                .setConnectionManager(
                                                        new TestingConnectionManager())
                                                .buildRemoteChannel(gate))
                        .toArray(RemoteInputChannel[]::new));

        gate.setup();
        gate.requestPartitions();

        return createCheckpointedInputGate(gate, toNotify);
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels,
            BufferOrEvent[] sequence,
            AbstractInvokable toNotify,
            boolean enableCheckpointsAfterTasksFinish) {
        mockInputGate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
        return createCheckpointedInputGate(
                mockInputGate, toNotify, enableCheckpointsAfterTasksFinish);
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels, BufferOrEvent[] sequence, AbstractInvokable toNotify) {
        mockInputGate = new MockInputGate(numberOfChannels, Arrays.asList(sequence));
        return createCheckpointedInputGate(mockInputGate, toNotify);
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            int numberOfChannels, BufferOrEvent[] sequence) {
        return createCheckpointedInputGate(
                numberOfChannels, sequence, new DummyCheckpointInvokable());
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate gate, AbstractInvokable toNotify) {
        return createCheckpointedInputGate(gate, toNotify, true);
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate gate,
            AbstractInvokable toNotify,
            boolean enableCheckpointsAfterTasksFinish) {
        return new CheckpointedInputGate(
                gate,
                SingleCheckpointBarrierHandler.aligned(
                        "Testing",
                        toNotify,
                        SystemClock.getInstance(),
                        gate.getNumberOfInputChannels(),
                        (callable, duration) -> () -> {},
                        enableCheckpointsAfterTasksFinish,
                        gate),
                new SyncMailboxExecutor());
    }

    @AfterEach
    void ensureEmpty() throws Exception {
        assertThat(inputGate.pollNext()).isNotPresent();
        assertThat(inputGate.isFinished()).isTrue();

        inputGate.close();
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for a single
     * input channel.
     */
    @Test
    void testSingleChannelNoBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0), createBuffer(0),
            createBuffer(0), createEndOfPartition(0)
        };
        inputGate = createCheckpointedInputGate(1, sequence);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
    }

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for an input with
     * multiple input channels.
     */
    @Test
    void testMultiChannelNoBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(2),
            createBuffer(2),
            createBuffer(0),
            createBuffer(1),
            createBuffer(0),
            createEndOfPartition(0),
            createBuffer(3),
            createBuffer(1),
            createEndOfPartition(3),
            createBuffer(1),
            createEndOfPartition(1),
            createBuffer(2),
            createEndOfPartition(2)
        };
        inputGate = createCheckpointedInputGate(4, sequence);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
    }

    /**
     * Validates that the buffer preserved the order of elements for a input with a single input
     * channel, and checkpoint events.
     */
    @Test
    void testSingleChannelWithBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(1, 0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(2, 0),
            createBarrier(3, 0),
            createBuffer(0),
            createBuffer(0),
            createBarrier(4, 0),
            createBarrier(5, 0),
            createBarrier(6, 0),
            createBuffer(0),
            createEndOfPartition(0)
        };
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(1, sequence, handler);

        handler.setNextExpectedCheckpointId(1L);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }
    }

    /**
     * Validates that the buffer correctly aligns the streams for inputs with multiple input
     * channels.
     */
    @Test
    void testMultiChannelWithBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            // checkpoint with data from multi channels
            createBuffer(0),
            createBuffer(2),
            createBuffer(0),
            createBarrier(1, 1),
            createBarrier(1, 2),
            createBuffer(0),
            createBarrier(1, 0),

            // another checkpoint
            createBuffer(0),
            createBuffer(0),
            createBuffer(1),
            createBuffer(1),
            createBuffer(2),
            createBarrier(2, 0),
            createBarrier(2, 1),
            createBarrier(2, 2),

            // checkpoint with data only from one channel
            createBuffer(2),
            createBuffer(2),
            createBarrier(3, 2),
            createBuffer(0),
            createBuffer(0),
            createBarrier(3, 0),
            createBarrier(3, 1),

            // empty checkpoint
            createBarrier(4, 1),
            createBarrier(4, 2),
            createBarrier(4, 0),

            // some trailing data
            createBuffer(0),
            createEndOfPartition(0),
            createEndOfPartition(1),
            createEndOfPartition(2)
        };
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(3, sequence, handler);

        handler.setNextExpectedCheckpointId(1L);

        // pre checkpoint 1
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isOne();

        long startTs = System.nanoTime();

        // checkpoint 1 done
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isEqualTo(2L);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // pre checkpoint 2
        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isEqualTo(2L);

        // checkpoint 2 barriers come together
        startTs = System.nanoTime();
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isEqualTo(3L);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 3
        check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isEqualTo(4L);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // checkpoint 4
        check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[23], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(handler.getNextExpectedCheckpointId()).isEqualTo(5L);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // remaining data
        check(sequence[25], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);
    }

    /**
     * Validates that the buffer skips over the current checkpoint if it receives a barrier from a
     * later checkpoint on a non-blocked input.
     */
    @Test
    void testMultiChannelJumpingOverCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            // checkpoint 1
            /* 0 */ createBuffer(0),
            /* 1 */ createBuffer(2),
            /* 2 */ createBuffer(0),
            /* 3 */ createBarrier(1, 1),
            /* 4 */ createBarrier(1, 2),
            /* 5 */ createBuffer(0),
            /* 6 */ createBarrier(1, 0),
            /* 7 */ createBuffer(1),
            /* 8 */ createBuffer(0),

            // checkpoint 2 will not complete: pre-mature barrier from checkpoint 3
            /* 9 */ createBarrier(2, 1),
            /* 10 */ createBuffer(2),
            /* 11 */ createBarrier(2, 0),
            /* 12 */ createBuffer(2),
            /* 13 */ createBarrier(3, 2),
            /* 14 */ createBuffer(1),
            /* 15 */ createBuffer(0),
            /* 16 */ createBarrier(3, 0),
            /* 17 */ createBarrier(4, 1),
            /* 18 */ createBuffer(2),
            /* 19 */ createBuffer(0),
            /* 20 */ createEndOfPartition(0),
            /* 21 */ createBuffer(2),
            /* 22 */ createEndOfPartition(2),
            /* 23 */ createBuffer(1),
            /* 24 */ createEndOfPartition(1)
        };
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(3, sequence, handler, false);

        handler.setNextExpectedCheckpointId(1L);

        // pre checkpoint 1
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 1
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isOne();
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

        // alignment of checkpoint 2
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(2L);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 2 aborted, checkpoint 3 started
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(0, 1);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(3L);
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(2);
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 3 aborted, checkpoint 4 started
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(0, 2);
        check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(4L);
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(1);
        check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 4 aborted (due to end of partition)
        check(sequence[20], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[23], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);

        assertThat(handler.getTriggeredCheckpointCounter()).isOne();
        assertThat(handler.getAbortedCheckpointCounter()).isEqualTo(3L);
    }

    @Test
    void testMetrics() throws Exception {
        List<BufferOrEvent> output = new ArrayList<>();
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler();
        int numberOfChannels = 3;
        inputGate = createCheckpointedInputGate(numberOfChannels, handler);
        int[] sequenceNumbers = new int[numberOfChannels];

        int bufferSize = 100;
        long checkpointId = 1;
        long sleepTime = 10;

        long checkpointBarrierCreation = System.currentTimeMillis();

        Thread.sleep(sleepTime);

        long alignmentStartNanos = System.nanoTime();

        addSequence(
                inputGate,
                output,
                sequenceNumbers,
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 1, checkpointBarrierCreation),
                createBuffer(0, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 0),
                createBuffer(2, bufferSize));

        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                output,
                sequenceNumbers,
                createBarrier(checkpointId, 2),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createEndOfPartition(0),
                createEndOfPartition(1),
                createEndOfPartition(2));

        long startDelay = System.currentTimeMillis() - checkpointBarrierCreation;
        long alignmentDuration = System.nanoTime() - alignmentStartNanos;

        assertThat(inputGate.getCheckpointBarrierHandler().getLatestCheckpointId())
                .isEqualTo(checkpointId);
        assertThat(inputGate.getCheckpointStartDelayNanos() / 1_000_000)
                .isBetween(sleepTime, startDelay);

        assertThat(handler.lastAlignmentDurationNanos).isDone();
        assertThat(handler.lastAlignmentDurationNanos.get() / 1_000_000)
                .isGreaterThanOrEqualTo(sleepTime);
        assertThat(handler.lastAlignmentDurationNanos.get()).isLessThanOrEqualTo(alignmentDuration);

        assertThat(handler.lastBytesProcessedDuringAlignment).isCompletedWithValue(3L * bufferSize);
    }

    @Test
    void testMissingCancellationBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1L, 0),
            createCancellationBarrier(3L, 1),
            createCancellationBarrier(2L, 0),
            createCancellationBarrier(3L, 0),
            createBuffer(0)
        };
        AbstractInvokable validator = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(2, sequence, validator);

        for (BufferOrEvent boe : sequence) {
            assertThat(inputGate.pollNext()).hasValue(boe);
        }

        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
    }

    @Test
    void testStartAlignmentWithClosedChannels() throws Exception {
        BufferOrEvent[] sequence = {
            // close some channels immediately
            createEndOfPartition(2),
            createEndOfPartition(1),

            // checkpoint without blocked data
            createBuffer(0),
            createBuffer(0),
            createBuffer(3),
            createBarrier(2, 3),
            createBarrier(2, 0),

            // empty checkpoint
            createBarrier(3, 0),
            createBarrier(3, 3),

            // some data, one channel closes
            createBuffer(0),
            createBuffer(0),
            createBuffer(3),
            createEndOfPartition(0),

            // checkpoint on last remaining channel
            createBuffer(3),
            createBarrier(4, 3),
            createBuffer(3),
            createEndOfPartition(3)
        };
        inputGate = createCheckpointedInputGate(4, sequence);

        // pre checkpoint 2
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 2 alignment
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(2L);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // checkpoint 3 alignment
        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(3L);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // after checkpoint 3
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 4 alignment
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(4L);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
    }

    @Test
    void testEndOfStreamWhileCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            // one checkpoint
            createBarrier(1, 0),
            createBarrier(1, 1),
            createBarrier(1, 2),

            // some buffers
            createBuffer(0),
            createBuffer(0),
            createBuffer(2),

            // start the checkpoint that will be incomplete
            createBarrier(2, 2),
            createBarrier(2, 0),
            createBuffer(1),

            // close one after the barrier one before the barrier
            createEndOfPartition(1),
            createEndOfPartition(2),
            createBuffer(0),

            // final end of stream
            createEndOfPartition(0)
        };
        inputGate = createCheckpointedInputGate(3, sequence);

        // data after first checkpoint
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isOne();

        // alignment of second checkpoint
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(2L);
        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

        // first end-of-partition encountered: checkpoint will not be completed
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
    }

    @Test
    void testSingleChannelAbortCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            createBuffer(0),
            createBarrier(1, 0),
            createBuffer(0),
            createBarrier(2, 0),
            createBuffer(0),
            createCancellationBarrier(4, 0),
            createBarrier(5, 0),
            createBuffer(0),
            createCancellationBarrier(6, 0),
            createBuffer(0)
        };
        ValidatingCheckpointHandler toNotify = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(1, sequence, toNotify);

        toNotify.setNextExpectedCheckpointId(1);
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        toNotify.setNextExpectedCheckpointId(2);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        toNotify.setNextExpectedCheckpointId(5);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[5], inputGate.pollNext().get(), 0);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(4L);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(4);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(5L);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(4);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), 0);
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(inputGate.getLatestCheckpointId()).isEqualTo(6L);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(6);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();

        assertThat(toNotify.getTriggeredCheckpointCounter()).isEqualTo(3);
        assertThat(toNotify.getAbortedCheckpointCounter()).isEqualTo(2);
    }

    @Test
    void testMultiChannelAbortCheckpoint() throws Exception {
        BufferOrEvent[] sequence = {
            // some buffers and a successful checkpoint
            /* 0 */ createBuffer(0),
            createBuffer(2),
            createBuffer(0),
            /* 3 */ createBarrier(1, 1),
            createBarrier(1, 2),
            /* 5 */ createBuffer(0),
            /* 6 */ createBarrier(1, 0),
            /* 7 */ createBuffer(0),
            createBuffer(2),

            // aborted on last barrier
            /* 9 */ createBarrier(2, 0),
            createBarrier(2, 2),
            /* 11 */ createBuffer(1),
            /* 12 */ createCancellationBarrier(2, 1),

            // successful checkpoint
            /* 13 */ createBuffer(2),
            createBuffer(1),
            /* 15 */ createBarrier(3, 1),
            createBarrier(3, 2),
            createBarrier(3, 0),

            // abort on first barrier
            /* 18 */ createBuffer(0),
            createBuffer(1),
            /* 20 */ createCancellationBarrier(4, 1),
            createBarrier(4, 2),
            /* 22 */ createBuffer(2),
            /* 23 */ createBarrier(4, 0),

            // another successful checkpoint
            /* 24 */ createBuffer(0),
            createBuffer(1),
            createBuffer(2),
            /* 27 */ createBarrier(5, 2),
            createBarrier(5, 1),
            createBarrier(5, 0),
            /* 30 */ createBuffer(0),
            createBuffer(1),

            // abort multiple cancellations and a barrier after the cancellations
            /* 32 */ createCancellationBarrier(6, 1),
            createCancellationBarrier(6, 2),
            /* 34 */ createBarrier(6, 0),

            /* 35 */ createBuffer(0)
        };
        ValidatingCheckpointHandler toNotify = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(3, sequence, toNotify);

        long startTs;

        // pre checkpoint
        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);

        // first successful checkpoint
        startTs = System.nanoTime();
        toNotify.setNextExpectedCheckpointId(1);
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).containsExactly(1);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(1, 2);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

        // alignment of second checkpoint
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).containsExactly(0);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(0, 2);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);

        // canceled checkpoint on last barrier
        check(sequence[12], inputGate.pollNext().get(), 0);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(2);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);

        // one more successful checkpoint
        startTs = System.nanoTime();
        toNotify.setNextExpectedCheckpointId(3);
        check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[17], inputGate.pollNext().get(), PAGE_SIZE);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        check(sequence[18], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[19], inputGate.pollNext().get(), PAGE_SIZE);

        // this checkpoint gets immediately canceled
        check(sequence[20], inputGate.pollNext().get(), 0);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(4);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
        check(sequence[21], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        check(sequence[22], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[23], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // some buffers
        check(sequence[24], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[25], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[26], inputGate.pollNext().get(), PAGE_SIZE);

        // a simple successful checkpoint
        startTs = System.nanoTime();
        toNotify.setNextExpectedCheckpointId(5);
        check(sequence[27], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[28], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[29], inputGate.pollNext().get(), PAGE_SIZE);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        check(sequence[30], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[31], inputGate.pollNext().get(), PAGE_SIZE);

        // this checkpoint gets immediately canceled
        check(sequence[32], inputGate.pollNext().get(), 0);
        check(sequence[33], inputGate.pollNext().get(), 0);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(6);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(inputGate.getAlignmentDurationNanos()).isZero();
        check(sequence[34], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();
        check(sequence[35], inputGate.pollNext().get(), PAGE_SIZE);

        assertThat(toNotify.getTriggeredCheckpointCounter()).isEqualTo(3);
        assertThat(toNotify.getAbortedCheckpointCounter()).isEqualTo(3);
    }

    /**
     * This tests where a checkpoint barriers meets a canceled checkpoint.
     *
     * <p>The newer checkpoint barrier must not try to cancel the already canceled checkpoint.
     */
    @Test
    void testAbortOnCanceledBarriers() throws Exception {
        BufferOrEvent[] sequence = {
            // starting a checkpoint
            /*  0 */ createBuffer(1),
            /*  1 */ createBarrier(1, 1),
            /*  2 */ createBuffer(2),
            createBuffer(0),

            // cancel the initial checkpoint
            /*  4 */ createCancellationBarrier(1, 0),

            // receiving a buffer
            /*  5 */ createBuffer(1),

            // starting a new checkpoint
            /*  6 */ createBarrier(2, 1),

            // some more buffers
            /*  7 */ createBuffer(2),
            createBuffer(0),

            // ignored barrier - already canceled and moved to next checkpoint
            /* 9 */ createBarrier(1, 2),

            // some more buffers
            /* 10 */ createBuffer(0),
            createBuffer(2),

            // complete next checkpoint regularly
            /* 12 */ createBarrier(2, 0),
            createBarrier(2, 2),

            // some more buffers
            /* 14 */ createBuffer(0),
            createBuffer(1),
            createBuffer(2)
        };
        ValidatingCheckpointHandler toNotify = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(3, sequence, toNotify);

        long startTs;

        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);

        // starting first checkpoint
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);

        // cancelled by cancellation barrier
        check(sequence[4], inputGate.pollNext().get(), 0);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(toNotify.getLastCanceledCheckpointId()).isOne();
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // the next checkpoint alignment
        startTs = System.nanoTime();
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[7], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);

        // ignored barrier and unblock channel directly
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(mockInputGate.getBlockedChannels()).containsExactly(1);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint 2 done
        toNotify.setNextExpectedCheckpointId(2);
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // trailing data
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[15], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[16], inputGate.pollNext().get(), PAGE_SIZE);

        assertThat(toNotify.getTriggeredCheckpointCounter()).isOne();
        assertThat(toNotify.getAbortedCheckpointCounter()).isOne();
    }

    /**
     * This tests the where a cancellation barrier is received for a checkpoint already canceled due
     * to receiving a newer checkpoint barrier.
     */
    @Test
    void testIgnoreCancelBarrierIfCheckpointSubsumed() throws Exception {
        BufferOrEvent[] sequence = {
            // starting a checkpoint
            /*  0 */ createBuffer(2),
            /*  1 */ createBarrier(3, 1),
            createBarrier(3, 0),
            /*  3 */ createBuffer(2),

            // newer checkpoint barrier cancels/subsumes pending checkpoint
            /*  4 */ createBarrier(5, 2),

            // some queued buffers
            /*  5 */ createBuffer(1),
            createBuffer(0),

            // cancel barrier the initial checkpoint /it is already canceled)
            /* 7 */ createCancellationBarrier(3, 0),

            // some more buffers
            /* 8 */ createBuffer(0),
            createBuffer(1),

            // complete next checkpoint regularly
            /* 10 */ createBarrier(5, 0),
            createBarrier(5, 1),

            // some more buffers
            /* 12 */ createBuffer(0),
            createBuffer(1),
            createBuffer(2)
        };
        ValidatingCheckpointHandler toNotify = new ValidatingCheckpointHandler();
        inputGate = createCheckpointedInputGate(3, sequence, toNotify);

        long startTs;

        // validate the sequence

        check(sequence[0], inputGate.pollNext().get(), PAGE_SIZE);

        // beginning of first checkpoint
        check(sequence[1], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[2], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[3], inputGate.pollNext().get(), PAGE_SIZE);

        // future barrier aborts checkpoint
        startTs = System.nanoTime();
        assertThat(mockInputGate.getBlockedChannels()).containsExactlyInAnyOrder(0, 1);
        check(sequence[4], inputGate.pollNext().get(), PAGE_SIZE);
        assertThat(toNotify.getLastCanceledCheckpointId()).isEqualTo(3);
        assertThat(toNotify.getCheckpointFailureReason())
                .isEqualTo(CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED);
        assertThat(mockInputGate.getBlockedChannels()).containsExactly(2);
        check(sequence[5], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[6], inputGate.pollNext().get(), PAGE_SIZE);

        // alignment of next checkpoint
        check(sequence[7], inputGate.pollNext().get(), 0);
        check(sequence[8], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[9], inputGate.pollNext().get(), PAGE_SIZE);

        // checkpoint finished
        toNotify.setNextExpectedCheckpointId(5);
        check(sequence[10], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[11], inputGate.pollNext().get(), PAGE_SIZE);
        validateAlignmentTime(startTs, inputGate);
        assertThat(mockInputGate.getBlockedChannels()).isEmpty();

        // remaining data
        check(sequence[12], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[13], inputGate.pollNext().get(), PAGE_SIZE);
        check(sequence[14], inputGate.pollNext().get(), PAGE_SIZE);

        // check overall notifications
        assertThat(toNotify.getTriggeredCheckpointCounter()).isOne();
        assertThat(toNotify.getAbortedCheckpointCounter()).isOne();
    }

    @Test
    void testTriggerCheckpointsWithEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            createBarrier(1, 0), createBarrier(1, 1), createEndOfPartition(2)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1L);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent bufferOrEvent : sequence) {
            check(bufferOrEvent, inputGate.pollNext().get(), PAGE_SIZE);
        }

        assertThat(validator.triggeredCheckpoints).containsExactly(1L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
        assertThat(inputGate.getCheckpointBarrierHandler().isCheckpointPending()).isFalse();
    }

    @Test
    void testDeduplicateChannelsWithBothBarrierAndEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            /* 0 */ createBarrier(2, 0),
            /* 1 */ createBarrier(2, 1),
            /* 2 */ createEndOfPartition(1),
            /* 3 */ createBarrier(2, 2)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1L);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (int i = 0; i <= 2; ++i) {
            check(sequence[i], inputGate.pollNext().get(), PAGE_SIZE);
        }

        // Here the checkpoint should not be triggered.
        assertThat(validator.getTriggeredCheckpointCounter()).isZero();
        assertThat(validator.getAbortedCheckpointCounter()).isZero();

        // The last barrier aligned the pending checkpoint 2.
        assertThat(inputGate.pollNext()).hasValue(sequence[3]);
        assertThat(validator.triggeredCheckpoints).containsExactly(2L);
        assertThat(inputGate.getCheckpointBarrierHandler().isCheckpointPending()).isFalse();
    }

    @Test
    void testTriggerCheckpointsAfterReceivedEndOfPartition() throws Exception {
        BufferOrEvent[] sequence = {
            /* 0 */ createEndOfPartition(2),
            /* 2 */ createBarrier(6, 0),
            /* 3 */ createBarrier(6, 1),
            /* 4 */ createEndOfPartition(1),
            /* 5 */ createBarrier(7, 0)
        };

        ValidatingCheckpointHandler validator = new ValidatingCheckpointHandler(-1L);
        inputGate = createCheckpointedInputGate(3, sequence, validator);

        for (BufferOrEvent bufferOrEvent : sequence) {
            check(bufferOrEvent, inputGate.pollNext().get(), PAGE_SIZE);
        }

        assertThat(validator.triggeredCheckpoints).containsExactly(6L, 7L);
        assertThat(validator.getAbortedCheckpointCounter()).isZero();
        assertThat(inputGate.getCheckpointBarrierHandler().isCheckpointPending()).isFalse();
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private static BufferOrEvent createBarrier(long checkpointId, int channel) {
        return createBarrier(checkpointId, channel, System.currentTimeMillis());
    }

    private static BufferOrEvent createBarrier(
            long checkpointId, int channel, long creationTimestamp) {
        return new BufferOrEvent(
                new CheckpointBarrier(
                        checkpointId,
                        creationTimestamp,
                        CheckpointOptions.forCheckpointWithDefaultLocation()),
                new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createCancellationBarrier(long checkpointId, int channel) {
        return new BufferOrEvent(
                new CancelCheckpointMarker(checkpointId), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createBuffer(int channel) {
        final int size = sizeCounter++;
        return createBuffer(channel, size);
    }

    static BufferOrEvent createBuffer(int channel, int size) {
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createEndOfPartition(int channel) {
        return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, new InputChannelInfo(0, channel));
    }

    private static void check(BufferOrEvent expected, BufferOrEvent present, int pageSize) {
        assertThat(expected).isNotNull();
        assertThat(present).isNotNull();
        assertThat(present.isBuffer()).isEqualTo(expected.isBuffer());

        if (expected.isBuffer()) {
            assertThat(present.getBuffer().getMaxCapacity())
                    .isEqualTo(expected.getBuffer().getMaxCapacity());
            assertThat(present.getBuffer().getSize()).isEqualTo(expected.getBuffer().getSize());
            MemorySegment expectedMem = expected.getBuffer().getMemorySegment();
            MemorySegment presentMem = present.getBuffer().getMemorySegment();
            assertThat(expectedMem.compare(presentMem, 0, 0, pageSize))
                    .as("memory contents differs")
                    .isZero();
        } else {
            assertThat(present.getEvent()).isEqualTo(expected.getEvent());
        }
    }

    private static void validateAlignmentTime(
            long alignmentStartTimestamp, CheckpointedInputGate inputGate) {
        long elapsedAlignment = System.nanoTime() - alignmentStartTimestamp;
        long elapsedTotalTime = System.nanoTime() - testStartTimeNanos;
        assertThat(inputGate.getAlignmentDurationNanos()).isLessThanOrEqualTo(elapsedAlignment);

        // Barrier lag is calculated with System.currentTimeMillis(), so we need a tolerance of 1ms
        // when comparing to time elapsed via System.nanoTime()
        long tolerance = 1_000_000;
        assertThat(inputGate.getCheckpointStartDelayNanos())
                .isLessThanOrEqualTo(elapsedTotalTime + tolerance);
    }
}
