/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.clock.SystemClock;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the behaviors of the {@link CheckpointedInputGate}. */
public class UnalignedCheckpointsTest {

    private static final long DEFAULT_CHECKPOINT_ID = 0L;

    private int sizeCounter = 1;

    private CheckpointedInputGate inputGate;

    private RecordingChannelStateWriter channelStateWriter;

    private int[] sequenceNumbers;

    private List<BufferOrEvent> output;

    @Before
    public void setUp() {
        channelStateWriter = new RecordingChannelStateWriter();
    }

    @After
    public void ensureEmpty() throws Exception {
        if (inputGate != null) {
            assertFalse(inputGate.pollNext().isPresent());
            assertTrue(inputGate.isFinished());
            inputGate.close();
        }

        if (channelStateWriter != null) {
            channelStateWriter.close();
        }
    }

    // ------------------------------------------------------------------------
    //  Tests
    // ------------------------------------------------------------------------

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for a single
     * input channel.
     */
    @Test
    public void testSingleChannelNoBarriers() throws Exception {
        inputGate = createInputGate(1, new ValidatingCheckpointHandler(1));
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence);
        assertInflightData();
    }

    /**
     * Validates that the buffer behaves correctly if no checkpoint barriers come, for an input with
     * multiple input channels.
     */
    @Test
    public void testMultiChannelNoBarriers() throws Exception {
        inputGate = createInputGate(4, new ValidatingCheckpointHandler(1));
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
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
                        createEndOfPartition(2));

        assertOutput(sequence);
        assertInflightData();
    }

    /**
     * Validates that the buffer preserved the order of elements for a input with a single input
     * channel, and checkpoint events.
     */
    @Test
    public void testSingleChannelWithBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(1, handler);
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
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
                        createEndOfPartition(0));

        assertOutput(sequence);
    }

    /**
     * Validates that the buffer correctly aligns the streams for inputs with multiple input
     * channels, by buffering and blocking certain inputs.
     */
    @Test
    public void testMultiChannelWithBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        // checkpoint with in-flight data
        BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0), // last buffer = in-flight
                        createBarrier(1, 0));

        // checkpoint 1 triggered unaligned
        assertOutput(sequence1);
        assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData(sequence1[7]);

        // checkpoint without in-flight data
        BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(2, 0),
                        createBarrier(2, 1),
                        createBarrier(2, 2));

        assertOutput(sequence2);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // checkpoint with data only from one channel
        BufferOrEvent[] sequence3 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(2),
                        createBarrier(3, 2),
                        createBuffer(2),
                        createBuffer(2),
                        createBarrier(3, 0),
                        createBarrier(3, 1));

        assertOutput(sequence3);
        assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // empty checkpoint
        addSequence(inputGate, createBarrier(4, 1), createBarrier(4, 2), createBarrier(4, 0));

        assertOutput();
        assertEquals(4L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // checkpoint with in-flight data in mixed order
        BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(5, 1),
                        createBuffer(2),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(5, 2),
                        createBuffer(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(5, 0));

        assertOutput(sequence5);
        assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData(sequence5[4], sequence5[5], sequence5[6], sequence5[10]);

        // some trailing data
        BufferOrEvent[] sequence6 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1),
                        createEndOfPartition(2));

        assertOutput(sequence6);
        assertInflightData();
    }

    @Test
    public void testMetrics() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);
        int bufferSize = 100;
        long checkpointId = 1;
        long sleepTime = 10;

        long checkpointBarrierCreation = System.currentTimeMillis();

        Thread.sleep(sleepTime);

        long alignmentStartNanos = System.nanoTime();

        addSequence(
                inputGate,
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 1, checkpointBarrierCreation),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createBarrier(checkpointId, 0),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize));

        long startDelay = System.currentTimeMillis() - checkpointBarrierCreation;
        Thread.sleep(sleepTime);

        addSequence(
                inputGate,
                createBarrier(checkpointId, 2),
                createBuffer(0, bufferSize),
                createBuffer(1, bufferSize),
                createBuffer(2, bufferSize),
                createEndOfPartition(0),
                createEndOfPartition(1),
                createEndOfPartition(2));

        long alignmentDuration = System.nanoTime() - alignmentStartNanos;

        assertEquals(checkpointId, inputGate.getCheckpointBarrierHandler().getLatestCheckpointId());
        assertThat(
                inputGate.getCheckpointStartDelayNanos() / 1_000_000,
                Matchers.greaterThanOrEqualTo(sleepTime));
        assertThat(
                inputGate.getCheckpointStartDelayNanos() / 1_000_000,
                Matchers.lessThanOrEqualTo(startDelay));

        assertTrue(handler.getLastAlignmentDurationNanos().isDone());
        assertThat(
                handler.getLastAlignmentDurationNanos().get() / 1_000_000,
                Matchers.greaterThanOrEqualTo(sleepTime));
        assertThat(
                handler.getLastAlignmentDurationNanos().get(),
                Matchers.lessThanOrEqualTo(alignmentDuration));

        assertTrue(handler.getLastBytesProcessedDuringAlignment().isDone());
        assertThat(
                handler.getLastBytesProcessedDuringAlignment().get(),
                Matchers.equalTo(6L * bufferSize));
    }

    @Test
    public void testMultiChannelTrailingInflightData() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBarrier(1, 0),
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0),
                        createBarrier(2, 1),
                        createBuffer(1),
                        createBuffer(1),
                        createEndOfPartition(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBarrier(2, 2),
                        createBuffer(2),
                        createEndOfPartition(2),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        // TODO: treat EndOfPartitionEvent as a special CheckpointBarrier?
        assertInflightData();
    }

    @Test
    public void testMissingCancellationBarriers() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(2, handler);
        final BufferOrEvent[] sequence =
                addSequence(
                        inputGate,
                        createBarrier(1L, 0),
                        createCancellationBarrier(2L, 0),
                        createCancellationBarrier(3L, 0),
                        createCancellationBarrier(3L, 1),
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1));

        assertOutput(sequence);
        assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(3L, handler.getLastCanceledCheckpointId());
        assertInflightData();
    }

    @Test
    public void testEarlyCleanup() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        // checkpoint 1
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBarrier(1, 0));
        assertOutput(sequence1);
        assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // checkpoint 2
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(1),
                        createBuffer(0),
                        createBarrier(2, 1),
                        createBuffer(1),
                        createBuffer(1),
                        createEndOfPartition(1),
                        createBuffer(0),
                        createBuffer(2),
                        createBarrier(2, 2),
                        createBuffer(2),
                        createEndOfPartition(2),
                        createBuffer(0),
                        createEndOfPartition(0));
        assertOutput(sequence2);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();
    }

    @Test
    public void testStartAlignmentWithClosedChannels() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(2);
        inputGate = createInputGate(4, handler);

        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        // close some channels immediately
                        createEndOfPartition(2),
                        createEndOfPartition(1),

                        // checkpoint without in-flight data
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(3),
                        createBarrier(2, 3),
                        createBarrier(2, 0));
        assertOutput(sequence1);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // checkpoint with in-flight data
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBuffer(3),
                        createBuffer(0),
                        createBarrier(3, 3),
                        createBuffer(3),
                        createBuffer(0),
                        createBarrier(3, 0));
        assertOutput(sequence2);
        assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData(sequence2[4]);

        // empty checkpoint
        final BufferOrEvent[] sequence3 =
                addSequence(inputGate, createBarrier(4, 0), createBarrier(4, 3));
        assertOutput(sequence3);
        assertEquals(4L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // some data, one channel closes
        final BufferOrEvent[] sequence4 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(3),
                        createEndOfPartition(0));
        assertOutput(sequence4);
        assertEquals(-1L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // checkpoint on last remaining channel
        final BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(3),
                        createBarrier(5, 3),
                        createBuffer(3),
                        createEndOfPartition(3));
        assertOutput(sequence5);
        assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();
    }

    @Test
    public void testEndOfStreamWhileCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);

        // one checkpoint
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate, createBarrier(1, 0), createBarrier(1, 1), createBarrier(1, 2));
        assertOutput(sequence1);
        assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        // some buffers
                        createBuffer(0),
                        createBuffer(0),
                        createBuffer(2),

                        // start the checkpoint that will be incomplete
                        createBarrier(2, 2),
                        createBarrier(2, 0),
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(1),

                        // close one after the barrier one before the barrier
                        createEndOfPartition(2),
                        createEndOfPartition(1),
                        createBuffer(0),

                        // final end of stream
                        createEndOfPartition(0));
        assertOutput(sequence2);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData(sequence2[7]);
    }

    @Test
    public void testNotifyAbortCheckpointBeforeCanellingAsyncCheckpoint() throws Exception {
        ValidateAsyncFutureNotCompleted handler = new ValidateAsyncFutureNotCompleted(1);
        inputGate = createInputGate(2, handler);
        handler.setInputGate(inputGate);
        addSequence(inputGate, createBarrier(1, 0), createCancellationBarrier(1, 1));

        addSequence(inputGate, createEndOfPartition(0), createEndOfPartition(1));
    }

    @Test
    public void testSingleChannelAbortCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(1, handler);
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBarrier(1, 0),
                        createBuffer(0),
                        createBarrier(2, 0),
                        createCancellationBarrier(4, 0));

        assertOutput(sequence1);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(4L, handler.getLastCanceledCheckpointId());
        assertInflightData();

        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBarrier(5, 0),
                        createBuffer(0),
                        createCancellationBarrier(6, 0),
                        createBuffer(0),
                        createEndOfPartition(0));

        assertOutput(sequence2);
        assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(6L, handler.getLastCanceledCheckpointId());
        assertInflightData();
    }

    @Test
    public void testMultiChannelAbortCheckpoint() throws Exception {
        ValidatingCheckpointHandler handler = new ValidatingCheckpointHandler(1);
        inputGate = createInputGate(3, handler);
        // some buffers and a successful checkpoint
        final BufferOrEvent[] sequence1 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(2),
                        createBuffer(0),
                        createBarrier(1, 1),
                        createBarrier(1, 2),
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(1, 0),
                        createBuffer(0),
                        createBuffer(2));

        assertOutput(sequence1);
        assertEquals(1L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // canceled checkpoint on last barrier
        final BufferOrEvent[] sequence2 =
                addSequence(
                        inputGate,
                        createBarrier(2, 0),
                        createBarrier(2, 2),
                        createBuffer(0),
                        createBuffer(2),
                        createCancellationBarrier(2, 1));

        assertOutput(sequence2);
        assertEquals(2L, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(2L, handler.getLastCanceledCheckpointId());
        assertInflightData();

        // one more successful checkpoint
        final BufferOrEvent[] sequence3 =
                addSequence(
                        inputGate,
                        createBuffer(2),
                        createBuffer(1),
                        createBarrier(3, 1),
                        createBarrier(3, 2),
                        createBarrier(3, 0));

        assertOutput(sequence3);
        assertEquals(3L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // this checkpoint gets immediately canceled, don't start a checkpoint at all
        final BufferOrEvent[] sequence4 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createCancellationBarrier(4, 1),
                        createBarrier(4, 2),
                        createBuffer(0),
                        createBarrier(4, 0));

        assertOutput(sequence4);
        assertEquals(-1, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(4L, handler.getLastCanceledCheckpointId());
        assertInflightData();

        // a simple successful checkpoint
        // another successful checkpoint
        final BufferOrEvent[] sequence5 =
                addSequence(
                        inputGate,
                        createBuffer(0),
                        createBuffer(1),
                        createBuffer(2),
                        createBarrier(5, 2),
                        createBarrier(5, 1),
                        createBarrier(5, 0),
                        createBuffer(0),
                        createBuffer(1));

        assertOutput(sequence5);
        assertEquals(5L, channelStateWriter.getLastStartedCheckpointId());
        assertInflightData();

        // abort multiple cancellations and a barrier after the cancellations, don't start a
        // checkpoint at all
        final BufferOrEvent[] sequence6 =
                addSequence(
                        inputGate,
                        createCancellationBarrier(6, 1),
                        createCancellationBarrier(6, 2),
                        createBarrier(6, 0),
                        createBuffer(0),
                        createEndOfPartition(0),
                        createEndOfPartition(1),
                        createEndOfPartition(2));

        assertOutput(sequence6);
        assertEquals(-1L, channelStateWriter.getLastStartedCheckpointId());
        assertEquals(6L, handler.getLastCanceledCheckpointId());
        assertInflightData();
    }

    /**
     * Tests {@link
     * SingleCheckpointBarrierHandler#processCancellationBarrier(CancelCheckpointMarker)} abort the
     * current pending checkpoint triggered by {@link
     * CheckpointBarrierHandler#processBarrier(CheckpointBarrier, InputChannelInfo)}.
     */
    @Test
    public void testProcessCancellationBarrierAfterProcessBarrier() throws Exception {
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(2)
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        inputGate);

        // should trigger respective checkpoint
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0));

        assertTrue(handler.isCheckpointPending());
        assertEquals(DEFAULT_CHECKPOINT_ID, handler.getLatestCheckpointId());

        testProcessCancellationBarrier(handler, invokable);
    }

    @Test
    public void testProcessCancellationBarrierBeforeProcessAndReceiveBarrier() throws Exception {
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        inputGate);

        handler.processCancellationBarrier(new CancelCheckpointMarker(DEFAULT_CHECKPOINT_ID));

        verifyTriggeredCheckpoint(handler, invokable, DEFAULT_CHECKPOINT_ID);

        // it would not trigger checkpoint since the respective cancellation barrier already
        // happened before
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0));

        verifyTriggeredCheckpoint(handler, invokable, DEFAULT_CHECKPOINT_ID);
    }

    private void testProcessCancellationBarrier(
            SingleCheckpointBarrierHandler handler, ValidatingCheckpointInvokable invokable)
            throws Exception {

        final long cancelledCheckpointId =
                new Random().nextBoolean() ? DEFAULT_CHECKPOINT_ID : DEFAULT_CHECKPOINT_ID + 1L;
        // should abort current checkpoint while processing CancelCheckpointMarker
        handler.processCancellationBarrier(new CancelCheckpointMarker(cancelledCheckpointId));
        verifyTriggeredCheckpoint(handler, invokable, cancelledCheckpointId);

        final long nextCancelledCheckpointId = cancelledCheckpointId + 1L;
        // should update current checkpoint id and abort notification while processing
        // CancelCheckpointMarker
        handler.processCancellationBarrier(new CancelCheckpointMarker(nextCancelledCheckpointId));
        verifyTriggeredCheckpoint(handler, invokable, nextCancelledCheckpointId);
    }

    private void verifyTriggeredCheckpoint(
            SingleCheckpointBarrierHandler handler,
            ValidatingCheckpointInvokable invokable,
            long currentCheckpointId) {

        assertFalse(handler.isCheckpointPending());
        assertEquals(currentCheckpointId, handler.getLatestCheckpointId());
        assertEquals(currentCheckpointId, invokable.getAbortedCheckpointId());
    }

    @Test
    public void testEndOfStreamWithPendingCheckpoint() throws Exception {
        final int numberOfChannels = 2;
        final ValidatingCheckpointInvokable invokable = new ValidatingCheckpointInvokable();
        final SingleInputGate inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalChannel)
                        .setNumberOfChannels(numberOfChannels)
                        .build();
        final SingleCheckpointBarrierHandler handler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        TestSubtaskCheckpointCoordinator.INSTANCE,
                        "test",
                        invokable,
                        SystemClock.getInstance(),
                        inputGate);

        // should trigger respective checkpoint
        handler.processBarrier(
                buildCheckpointBarrier(DEFAULT_CHECKPOINT_ID), new InputChannelInfo(0, 0));

        assertTrue(handler.isCheckpointPending());
        assertEquals(DEFAULT_CHECKPOINT_ID, handler.getLatestCheckpointId());
        assertEquals(numberOfChannels, handler.getNumOpenChannels());

        // should abort current checkpoint while processing eof
        handler.processEndOfPartition();

        assertFalse(handler.isCheckpointPending());
        assertEquals(DEFAULT_CHECKPOINT_ID, handler.getLatestCheckpointId());
        assertEquals(numberOfChannels - 1, handler.getNumOpenChannels());
        assertEquals(DEFAULT_CHECKPOINT_ID, invokable.getAbortedCheckpointId());
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    private BufferOrEvent createBarrier(long checkpointId, int channel) {
        return createBarrier(checkpointId, channel, System.currentTimeMillis());
    }

    private BufferOrEvent createBarrier(long checkpointId, int channel, long timestamp) {
        sizeCounter++;
        return new BufferOrEvent(
                new CheckpointBarrier(
                        checkpointId, timestamp, CheckpointOptions.unaligned(getDefault())),
                new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createCancellationBarrier(long checkpointId, int channel) {
        sizeCounter++;
        return new BufferOrEvent(
                new CancelCheckpointMarker(checkpointId), new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createBuffer(int channel, int size) {
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private BufferOrEvent createBuffer(int channel) {
        final int size = sizeCounter++;
        return new BufferOrEvent(
                TestBufferFactory.createBuffer(size), new InputChannelInfo(0, channel));
    }

    private static BufferOrEvent createEndOfPartition(int channel) {
        return new BufferOrEvent(EndOfPartitionEvent.INSTANCE, new InputChannelInfo(0, channel));
    }

    private CheckpointedInputGate createInputGate(int numberOfChannels, AbstractInvokable toNotify)
            throws IOException {
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
                                                .setStateWriter(channelStateWriter)
                                                .setupFromNettyShuffleEnvironment(environment)
                                                .setConnectionManager(
                                                        new TestingConnectionManager())
                                                .buildRemoteChannel(gate))
                        .toArray(RemoteInputChannel[]::new));
        sequenceNumbers = new int[numberOfChannels];

        gate.setup();
        gate.requestPartitions();

        return createCheckpointedInputGate(gate, toNotify);
    }

    private BufferOrEvent[] addSequence(CheckpointedInputGate inputGate, BufferOrEvent... sequence)
            throws Exception {
        output = new ArrayList<>();
        addSequence(inputGate, output, sequenceNumbers, sequence);
        sizeCounter = 1;
        return sequence;
    }

    static BufferOrEvent[] addSequence(
            CheckpointedInputGate inputGate,
            List<BufferOrEvent> output,
            int[] sequenceNumbers,
            BufferOrEvent... sequence)
            throws Exception {
        for (BufferOrEvent bufferOrEvent : sequence) {
            if (bufferOrEvent.isEvent()) {
                bufferOrEvent =
                        new BufferOrEvent(
                                EventSerializer.toBuffer(
                                        bufferOrEvent.getEvent(),
                                        bufferOrEvent.getEvent() instanceof CheckpointBarrier),
                                bufferOrEvent.getChannelInfo(),
                                bufferOrEvent.moreAvailable(),
                                bufferOrEvent.morePriorityEvents());
            }
            ((RemoteInputChannel)
                            inputGate.getChannel(
                                    bufferOrEvent.getChannelInfo().getInputChannelIdx()))
                    .onBuffer(
                            bufferOrEvent.getBuffer(),
                            sequenceNumbers[bufferOrEvent.getChannelInfo().getInputChannelIdx()]++,
                            0);

            while (inputGate.pollNext().map(output::add).isPresent()) {}
        }
        return sequence;
    }

    private CheckpointedInputGate createCheckpointedInputGate(
            IndexedInputGate gate, AbstractInvokable toNotify) {
        final SingleCheckpointBarrierHandler barrierHandler =
                SingleCheckpointBarrierHandler.createUnalignedCheckpointBarrierHandler(
                        new TestSubtaskCheckpointCoordinator(channelStateWriter),
                        "Test",
                        toNotify,
                        SystemClock.getInstance(),
                        gate);
        return new CheckpointedInputGate(gate, barrierHandler, new SyncMailboxExecutor());
    }

    private void assertInflightData(BufferOrEvent... expected) {
        Collection<BufferOrEvent> andResetInflightData = getAndResetInflightData();
        assertEquals(
                "Unexpected in-flight sequence: " + andResetInflightData,
                getIds(Arrays.asList(expected)),
                getIds(andResetInflightData));
    }

    private Collection<BufferOrEvent> getAndResetInflightData() {
        final List<BufferOrEvent> inflightData =
                channelStateWriter.getAddedInput().entries().stream()
                        .map(entry -> new BufferOrEvent(entry.getValue(), entry.getKey()))
                        .collect(Collectors.toList());
        channelStateWriter.reset();
        return inflightData;
    }

    private void assertOutput(BufferOrEvent... expectedSequence) {
        assertEquals(
                "Unexpected output sequence",
                getIds(Arrays.asList(expectedSequence)),
                getIds(output));
    }

    private List<Object> getIds(Collection<BufferOrEvent> buffers) {
        return buffers.stream()
                .filter(
                        boe ->
                                !boe.isEvent()
                                        || !(boe.getEvent() instanceof CheckpointBarrier
                                                || boe.getEvent()
                                                        instanceof CancelCheckpointMarker))
                .map(boe -> boe.isBuffer() ? boe.getSize() - 1 : boe.getEvent())
                .collect(Collectors.toList());
    }

    private CheckpointBarrier buildCheckpointBarrier(long id) {
        return new CheckpointBarrier(id, 0, CheckpointOptions.unaligned(getDefault()));
    }

    // ------------------------------------------------------------------------
    //  Testing Mocks
    // ------------------------------------------------------------------------

    /** The invokable handler used for triggering checkpoint and validation. */
    static class ValidatingCheckpointHandler
            extends org.apache.flink.streaming.runtime.io.checkpointing
                    .ValidatingCheckpointHandler {

        public ValidatingCheckpointHandler(long nextExpectedCheckpointId) {
            super(nextExpectedCheckpointId);
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            super.abortCheckpointOnBarrier(checkpointId, cause);
            nextExpectedCheckpointId = -1;
        }
    }

    static class ValidateAsyncFutureNotCompleted extends ValidatingCheckpointHandler {
        private @Nullable CheckpointedInputGate inputGate;

        public ValidateAsyncFutureNotCompleted(long nextExpectedCheckpointId) {
            super(nextExpectedCheckpointId);
        }

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
            super.abortCheckpointOnBarrier(checkpointId, cause);
            checkState(inputGate != null);
            assertFalse(inputGate.getAllBarriersReceivedFuture(checkpointId).isDone());
        }

        public void setInputGate(CheckpointedInputGate inputGate) {
            this.inputGate = inputGate;
        }
    }

    /**
     * Specific {@link AbstractInvokable} implementation to record and validate which checkpoint id
     * is executed and how many checkpoints are executed.
     */
    private static final class ValidatingCheckpointInvokable extends StreamTask {

        private long expectedCheckpointId;

        private int totalNumCheckpoints;

        private long abortedCheckpointId;

        ValidatingCheckpointInvokable() throws Exception {
            super(new DummyEnvironment("test", 1, 0));
        }

        @Override
        public void init() {}

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) {}

        @Override
        public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
                throws IOException {
            abortedCheckpointId = checkpointId;
        }

        public void triggerCheckpointOnBarrier(
                CheckpointMetaData checkpointMetaData,
                CheckpointOptions checkpointOptions,
                CheckpointMetricsBuilder checkpointMetrics) {
            expectedCheckpointId = checkpointMetaData.getCheckpointId();
            totalNumCheckpoints++;
        }

        long getTriggeredCheckpointId() {
            return expectedCheckpointId;
        }

        int getTotalTriggeredCheckpoints() {
            return totalNumCheckpoints;
        }

        long getAbortedCheckpointId() {
            return abortedCheckpointId;
        }
    }
}
