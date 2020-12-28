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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.TestingPartitionRequestClient;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** {@link CheckpointedInputGate} test. */
public class CheckpointedInputGateTest {
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
                emitEndOfState(gate, channelIndex);
                assertFalse("should align (block all channels)", gate.pollNext().isPresent());
            }

            emitEndOfState(gate, numberOfChannels - 1);
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

    private void emitEndOfState(CheckpointedInputGate checkpointedInputGate, int channelIndex)
            throws IOException {
        ((RemoteInputChannel) checkpointedInputGate.getChannel(channelIndex))
                .onBuffer(EventSerializer.toBuffer(EndOfChannelStateEvent.INSTANCE, false), 0, 0);
    }

    private CheckpointedInputGate setupInputGate(
            int numberOfChannels,
            NetworkBufferPool networkBufferPool,
            ResumeCountingConnectionManager connectionManager)
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
        CheckpointBarrierTracker barrierHandler =
                new CheckpointBarrierTracker(
                        numberOfChannels,
                        new AbstractInvokable(new DummyEnvironment()) {
                            @Override
                            public void invoke() {}
                        });
        MailboxExecutorImpl mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);

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
