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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.TestBarrierHandlerFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** A builder for creating instances of {@link CheckpointedInputGate} for tests. */
public class TestCheckpointedInputGateBuilder {
    private final int numChannels;
    private final TestBarrierHandlerFactory barrierHandlerFactory;

    private ChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
    private SupplierWithException<SingleInputGate, IOException> gateBuilder = this::buildTestGate;
    private MailboxExecutor mailboxExecutor;

    public TestCheckpointedInputGateBuilder(
            int numChannels, TestBarrierHandlerFactory barrierHandler) {
        this.numChannels = numChannels;
        this.barrierHandlerFactory = barrierHandler;

        this.mailboxExecutor = new SyncMailboxExecutor();
    }

    /**
     * Uses {@link RemoteInputChannel RemoteInputChannels} and enables {@link
     * #withMailboxExecutor()} by default.
     */
    public TestCheckpointedInputGateBuilder withRemoteChannels() {
        this.gateBuilder = this::buildRemoteGate;
        return this;
    }

    /**
     * Uses all channels as {@link RemoteInputChannel RemoteInputChannels} except the channel from
     * testChannelIds which should be {@link TestInputChannel}.
     */
    public TestCheckpointedInputGateBuilder withMixedChannels(Integer... testChannelIds) {
        this.gateBuilder = () -> buildMixedGate(testChannelIds);
        return this;
    }

    /** Uses {@link TestInputChannel TestInputChannels}. */
    public TestCheckpointedInputGateBuilder withTestChannels() {
        this.gateBuilder = this::buildTestGate;
        return this;
    }

    public TestCheckpointedInputGateBuilder withSyncExecutor() {
        this.mailboxExecutor = new SyncMailboxExecutor();
        return this;
    }

    public TestCheckpointedInputGateBuilder withMailboxExecutor() {
        // do not fire events automatically. If you need events, you should expose mailboxProcessor
        // and
        // execute it step by step
        this.mailboxExecutor = new MailboxProcessor().getMainMailboxExecutor();
        return this;
    }

    public TestCheckpointedInputGateBuilder withChannelStateWriter(
            ChannelStateWriter channelStateWriter) {
        this.channelStateWriter = channelStateWriter;
        return this;
    }

    public CheckpointedInputGate build() throws IOException {
        SingleInputGate gate = gateBuilder.get();

        return new CheckpointedInputGate(
                gate, barrierHandlerFactory.create(gate, channelStateWriter), mailboxExecutor);
    }

    private SingleInputGate buildTestGate() {
        SingleInputGate gate =
                new SingleInputGateBuilder().setNumberOfChannels(numChannels).build();
        TestInputChannel[] channels = new TestInputChannel[numChannels];
        for (int i = 0; i < numChannels; i++) {
            channels[i] = new TestInputChannel(gate, i, false, true);
        }
        gate.setInputChannels(channels);
        return gate;
    }

    private SingleInputGate buildMixedGate(Integer... testChannelIds) throws IOException {
        Set<Integer> testChannelIdSet = new HashSet<>(Arrays.asList(testChannelIds));
        SingleInputGate gate = buildRemoteGate();
        InputChannel[] channels = new InputChannel[numChannels];
        for (int i = 0; i < numChannels; i++) {
            if (testChannelIdSet.contains(i)) {
                channels[i] = new TestInputChannel(gate, i, false, true);
            } else {
                channels[i] = gate.getChannel(i);
            }
        }
        gate.setInputChannels(channels);

        return gate;
    }

    private SingleInputGate buildRemoteGate() throws IOException {
        int maxUsedBuffers = 10;
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(numChannels * maxUsedBuffers, 4096);
        SingleInputGate gate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildRemoteChannel)
                        .setNumberOfChannels(numChannels)
                        .setSegmentProvider(networkBufferPool)
                        .setBufferPoolFactory(
                                networkBufferPool.createBufferPool(numChannels, maxUsedBuffers))
                        .setChannelStateWriter(channelStateWriter)
                        .build();
        gate.setup();
        gate.requestPartitions();
        return gate;
    }
}
