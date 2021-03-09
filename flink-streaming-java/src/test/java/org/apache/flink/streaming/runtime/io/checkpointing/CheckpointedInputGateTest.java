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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.MockChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.TestingPartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.apache.flink.shaded.guava18.com.google.common.io.Closer;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSomeBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link CheckpointedInputGate} test. */
public class CheckpointedInputGateTest {
    private final HashMap<Integer, Integer> channelIndexToSequenceNumber = new HashMap<>();

    @Before
    public void setUp() {
        channelIndexToSequenceNumber.clear();
    }

    @Test
    public void testUpstreamResumedUponEndOfRecovery() throws Exception {
        int numberOfChannels = 11;
        NetworkBufferPool bufferPool = new NetworkBufferPool(numberOfChannels * 3, 1024);
        try {
            ResumeCountingConnectionManager resumeCounter = new ResumeCountingConnectionManager();
            CheckpointedInputGate gate =
                    setupInputGate(numberOfChannels, bufferPool, resumeCounter);
            assertFalse(gate.pollNext().isPresent());
            for (int channelIndex = 0; channelIndex < numberOfChannels - 1; channelIndex++) {
                enqueueEndOfState(gate, channelIndex);
                assertFalse("should align (block all channels)", gate.pollNext().isPresent());
            }

            enqueueEndOfState(gate, numberOfChannels - 1);
            Optional<BufferOrEvent> polled = gate.pollNext();
            assertTrue(polled.isPresent());
            assertTrue(polled.get().isEvent());
            assertEquals(EndOfChannelStateEvent.INSTANCE, polled.get().getEvent());
            assertEquals(numberOfChannels, resumeCounter.getNumResumed());
            assertFalse(
                    "should only be a single event no matter of what is the number of channels",
                    gate.pollNext().isPresent());
        } finally {
            bufferPool.destroy();
        }
    }

    @Test
    public void testPersisting() throws Exception {
        testPersisting(false);
    }

    @Test
    public void testPersistingWithDrainingTheGate() throws Exception {
        testPersisting(true);
    }

    /**
     * This tests a scenario where an older triggered checkpoint, was cancelled and a newer
     * checkpoint was triggered very quickly after the cancellation. It can happen that a task can
     * receive first the more recent checkpoint barrier and later the obsoleted one. This can happen
     * for many reasons (for example Source tasks not running, or just a race condition with
     * notifyCheckpointAborted RPCs) and Task should be able to handle this properly. In FLINK-21104
     * the problem was that this obsoleted checkpoint barrier was causing a checkState to fail.
     */
    public void testPersisting(boolean drainGate) throws Exception {

        int numberOfChannels = 3;
        NetworkBufferPool bufferPool = new NetworkBufferPool(numberOfChannels * 3, 1024);
        try {
            long checkpointId = 2L;
            long obsoleteCheckpointId = 1L;
            ValidatingCheckpointHandler validatingHandler =
                    new ValidatingCheckpointHandler(checkpointId);
            RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
            CheckpointedInputGate gate =
                    setupInputGateWithAlternatingController(
                            numberOfChannels, bufferPool, validatingHandler, stateWriter);

            // enqueue first checkpointId before obsoleteCheckpointId, so that we never trigger
            // and also never cancel the obsoleteCheckpointId
            enqueue(gate, 0, buildSomeBuffer());
            enqueue(gate, 0, barrier(checkpointId));
            enqueue(gate, 0, buildSomeBuffer());
            enqueue(gate, 1, buildSomeBuffer());
            enqueue(gate, 1, barrier(obsoleteCheckpointId));
            enqueue(gate, 1, buildSomeBuffer());
            enqueue(gate, 2, buildSomeBuffer());

            assertEquals(0, validatingHandler.getTriggeredCheckpointCounter());
            // trigger checkpoint
            gate.pollNext();
            assertEquals(1, validatingHandler.getTriggeredCheckpointCounter());

            assertAddedInputSize(stateWriter, 0, 1);
            assertAddedInputSize(stateWriter, 1, 2);
            assertAddedInputSize(stateWriter, 2, 1);

            enqueue(gate, 0, buildSomeBuffer());
            enqueue(gate, 1, buildSomeBuffer());
            enqueue(gate, 2, buildSomeBuffer());

            while (drainGate && gate.pollNext().isPresent()) {}

            assertAddedInputSize(stateWriter, 0, 1);
            assertAddedInputSize(stateWriter, 1, 3);
            assertAddedInputSize(stateWriter, 2, 2);

            enqueue(gate, 1, barrier(checkpointId));
            enqueue(gate, 1, buildSomeBuffer());
            // Another obsoleted barrier that should be ignored
            enqueue(gate, 2, barrier(obsoleteCheckpointId));
            enqueue(gate, 2, buildSomeBuffer());

            while (drainGate && gate.pollNext().isPresent()) {}

            assertAddedInputSize(stateWriter, 0, 1);
            assertAddedInputSize(stateWriter, 1, 3);
            assertAddedInputSize(stateWriter, 2, 3);

            enqueue(gate, 2, barrier(checkpointId));
            enqueue(gate, 2, buildSomeBuffer());

            while (drainGate && gate.pollNext().isPresent()) {}

            assertAddedInputSize(stateWriter, 0, 1);
            assertAddedInputSize(stateWriter, 1, 3);
            assertAddedInputSize(stateWriter, 2, 3);
        } finally {
            bufferPool.destroy();
        }
    }

    /**
     * Tests a priority notification happening right before cancellation. The mail would be
     * processed while draining mailbox but can't pull any data anymore.
     */
    @Test
    public void testPriorityBeforeClose() throws IOException, InterruptedException {

        NetworkBufferPool bufferPool = new NetworkBufferPool(10, 1024);
        try (Closer closer = Closer.create()) {
            closer.register(bufferPool::destroy);

            for (int repeat = 0; repeat < 100; repeat++) {
                setUp();

                SingleInputGate singleInputGate =
                        new SingleInputGateBuilder()
                                .setNumberOfChannels(2)
                                .setBufferPoolFactory(
                                        bufferPool.createBufferPool(2, Integer.MAX_VALUE))
                                .setSegmentProvider(bufferPool)
                                .setChannelFactory(InputChannelBuilder::buildRemoteChannel)
                                .build();
                singleInputGate.setup();
                ((RemoteInputChannel) singleInputGate.getChannel(0)).requestSubpartition(0);

                final TaskMailboxImpl mailbox = new TaskMailboxImpl();
                MailboxExecutorImpl mailboxExecutor =
                        new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

                ValidatingCheckpointHandler validatingHandler = new ValidatingCheckpointHandler(1);
                SingleCheckpointBarrierHandler barrierHandler =
                        AlternatingControllerTest.barrierHandler(
                                singleInputGate, validatingHandler, new MockChannelStateWriter());
                CheckpointedInputGate checkpointedInputGate =
                        new CheckpointedInputGate(
                                singleInputGate,
                                barrierHandler,
                                mailboxExecutor,
                                UpstreamRecoveryTracker.forInputGate(singleInputGate));

                final int oldSize = mailbox.size();
                enqueue(checkpointedInputGate, 0, barrier(1));
                // wait for priority mail to be enqueued
                Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
                while (deadline.hasTimeLeft() && oldSize >= mailbox.size()) {
                    Thread.sleep(1);
                }

                // test the race condition
                // either priority event could be handled, then we expect a checkpoint to be
                // triggered or closing came first in which case we expect a CancelTaskException
                CountDownLatch beforeLatch = new CountDownLatch(2);
                final CheckedThread canceler =
                        new CheckedThread("Canceler") {
                            @Override
                            public void go() throws IOException {
                                beforeLatch.countDown();
                                singleInputGate.close();
                            }
                        };
                canceler.start();
                beforeLatch.countDown();
                try {
                    while (mailboxExecutor.tryYield()) {}
                    assertEquals(1L, validatingHandler.triggeredCheckpointCounter);
                } catch (CancelTaskException e) {
                }
                canceler.join();
            }
        }
    }

    private static CheckpointBarrier barrier(long barrierId) {
        return new CheckpointBarrier(
                barrierId,
                barrierId,
                CheckpointOptions.unaligned(CheckpointStorageLocationReference.getDefault()));
    }

    private void assertAddedInputSize(
            RecordingChannelStateWriter stateWriter, int channelIndex, int size) {
        assertEquals(
                size,
                stateWriter.getAddedInput().get(new InputChannelInfo(0, channelIndex)).size());
    }

    private void enqueueEndOfState(CheckpointedInputGate checkpointedInputGate, int channelIndex)
            throws IOException {
        enqueue(checkpointedInputGate, channelIndex, EndOfChannelStateEvent.INSTANCE);
    }

    private void enqueueEndOfPartition(
            CheckpointedInputGate checkpointedInputGate, int channelIndex) throws IOException {
        enqueue(checkpointedInputGate, channelIndex, EndOfPartitionEvent.INSTANCE);
    }

    private void enqueue(
            CheckpointedInputGate checkpointedInputGate, int channelIndex, AbstractEvent event)
            throws IOException {
        boolean hasPriority = false;
        if (event instanceof CheckpointBarrier) {
            hasPriority =
                    ((CheckpointBarrier) event).getCheckpointOptions().isUnalignedCheckpoint();
        }
        enqueue(checkpointedInputGate, channelIndex, EventSerializer.toBuffer(event, hasPriority));
    }

    private void enqueue(
            CheckpointedInputGate checkpointedInputGate, int channelIndex, Buffer buffer)
            throws IOException {
        Integer sequenceNumber =
                channelIndexToSequenceNumber.compute(
                        channelIndex,
                        (key, oldSequence) -> oldSequence == null ? 0 : oldSequence + 1);
        ((RemoteInputChannel) checkpointedInputGate.getChannel(channelIndex))
                .onBuffer(buffer, sequenceNumber, 0);
    }

    private CheckpointedInputGate setupInputGate(
            int numberOfChannels,
            NetworkBufferPool networkBufferPool,
            ConnectionManager connectionManager)
            throws Exception {

        SingleInputGate singleInputGate =
                new SingleInputGateBuilder()
                        .setBufferPoolFactory(
                                networkBufferPool.createBufferPool(
                                        numberOfChannels, Integer.MAX_VALUE))
                        .setSegmentProvider(networkBufferPool)
                        .setChannelFactory(
                                (builder, gate) ->
                                        builder.setConnectionManager(connectionManager)
                                                .buildRemoteChannel(gate))
                        .setNumberOfChannels(numberOfChannels)
                        .build();
        singleInputGate.setup();
        MailboxExecutorImpl mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);

        CheckpointBarrierTracker barrierHandler =
                new CheckpointBarrierTracker(
                        numberOfChannels,
                        new AbstractInvokable(new DummyEnvironment()) {
                            @Override
                            public void invoke() {}
                        });

        CheckpointedInputGate checkpointedInputGate =
                new CheckpointedInputGate(
                        singleInputGate,
                        barrierHandler,
                        mailboxExecutor,
                        UpstreamRecoveryTracker.forInputGate(singleInputGate));
        for (int i = 0; i < numberOfChannels; i++) {
            ((RemoteInputChannel) checkpointedInputGate.getChannel(i)).requestSubpartition(0);
        }
        return checkpointedInputGate;
    }

    private CheckpointedInputGate setupInputGateWithAlternatingController(
            int numberOfChannels,
            NetworkBufferPool networkBufferPool,
            AbstractInvokable abstractInvokable,
            RecordingChannelStateWriter stateWriter)
            throws Exception {
        ConnectionManager connectionManager = new TestingConnectionManager();
        SingleInputGate singleInputGate =
                new SingleInputGateBuilder()
                        .setBufferPoolFactory(
                                networkBufferPool.createBufferPool(
                                        numberOfChannels, Integer.MAX_VALUE))
                        .setSegmentProvider(networkBufferPool)
                        .setChannelFactory(
                                (builder, gate) ->
                                        builder.setConnectionManager(connectionManager)
                                                .buildRemoteChannel(gate))
                        .setNumberOfChannels(numberOfChannels)
                        .setChannelStateWriter(stateWriter)
                        .build();
        singleInputGate.setup();
        MailboxExecutorImpl mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);

        SingleCheckpointBarrierHandler barrierHandler =
                AlternatingControllerTest.barrierHandler(
                        singleInputGate, abstractInvokable, stateWriter);
        CheckpointedInputGate checkpointedInputGate =
                new CheckpointedInputGate(
                        singleInputGate,
                        barrierHandler,
                        mailboxExecutor,
                        UpstreamRecoveryTracker.forInputGate(singleInputGate));
        for (int i = 0; i < numberOfChannels; i++) {
            ((RemoteInputChannel) checkpointedInputGate.getChannel(i)).requestSubpartition(0);
        }
        return checkpointedInputGate;
    }

    private static class ResumeCountingConnectionManager extends TestingConnectionManager {
        private int numResumed;

        @Override
        public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) {
            return new TestingPartitionRequestClient() {
                @Override
                public void resumeConsumption(RemoteInputChannel inputChannel) {
                    numResumed++;
                    super.resumeConsumption(inputChannel);
                }
            };
        }

        private int getNumResumed() {
            return numResumed;
        }
    }
}
