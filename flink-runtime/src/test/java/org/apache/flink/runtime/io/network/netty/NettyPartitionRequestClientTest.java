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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ResumeConsumption;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.mockConnectionManagerWithPartitionRequestClient;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyPartitionRequestClient}. */
@ExtendWith(ParameterizedTestExtension.class)
class NettyPartitionRequestClientTest {
    @Parameter private boolean connectionReuseEnabled;

    @Parameters(name = "connection reuse enabled = {0}")
    private static Object[] parameters() {
        return new Object[][] {new Object[] {true}, new Object[] {false}};
    }

    @TestTemplate
    void testPartitionRequestClientReuse() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final NettyPartitionRequestClient client =
                createPartitionRequestClient(channel, handler, true);

        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            // Client should not be disposed in idle
            client.close(inputChannel);
            assertThat(client.canBeDisposed()).isFalse();

            // Client should be disposed in error
            handler.notifyAllChannelsOfErrorAndClose(new RuntimeException());
            assertThat(client.canBeDisposed()).isTrue();
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @TestTemplate
    void testRetriggerPartitionRequest() throws Exception {
        final long deadline = System.currentTimeMillis() + 30_000L; // 30 secs

        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client =
                createPartitionRequestClient(channel, handler, connectionReuseEnabled);

        final int numExclusiveBuffers = 2;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder()
                        .setConnectionManager(
                                mockConnectionManagerWithPartitionRequestClient(client))
                        .setPartitionRequestListenerTimeout(1)
                        .setMaxBackoff(2)
                        .buildRemoteChannel(inputGate);

        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            // first subpartition request
            inputChannel.requestSubpartitions();

            assertThat(channel.isWritable()).isTrue();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);
            assertThat(((PartitionRequest) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());
            assertThat(((PartitionRequest) readFromOutbound).credit).isEqualTo(numExclusiveBuffers);

            // retrigger subpartition request, e.g. due to failures
            inputGate.retriggerPartitionRequest(
                    inputChannel.getPartitionId().getPartitionId(), inputChannel.getChannelInfo());
            runAllScheduledPendingTasks(channel, deadline);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);
            assertThat(((PartitionRequest) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());
            assertThat(((PartitionRequest) readFromOutbound).credit).isEqualTo(numExclusiveBuffers);

            // retrigger subpartition request once again, e.g. due to failures
            inputGate.retriggerPartitionRequest(
                    inputChannel.getPartitionId().getPartitionId(), inputChannel.getChannelInfo());
            runAllScheduledPendingTasks(channel, deadline);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);
            assertThat(((PartitionRequest) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());
            assertThat(((PartitionRequest) readFromOutbound).credit).isEqualTo(numExclusiveBuffers);

            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @TestTemplate
    void testDoublePartitionRequest() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client =
                createPartitionRequestClient(channel, handler, connectionReuseEnabled);

        final int numExclusiveBuffers = 2;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            // The input channel should only send one partition request
            assertThat(channel.isWritable()).isTrue();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);
            assertThat(((PartitionRequest) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());
            assertThat(((PartitionRequest) readFromOutbound).credit).isEqualTo(numExclusiveBuffers);

            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @TestTemplate
    void testResumeConsumption() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client =
                createPartitionRequestClient(channel, handler, connectionReuseEnabled);

        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            inputChannel.resumeConsumption();
            channel.runPendingTasks();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(ResumeConsumption.class);
            assertThat(((ResumeConsumption) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());

            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @TestTemplate
    void testAcknowledgeAllRecordsProcessed() throws Exception {
        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        PartitionRequestClient client =
                createPartitionRequestClient(channel, handler, connectionReuseEnabled);

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartitions();

            inputChannel.acknowledgeAllRecordsProcessed();
            channel.runPendingTasks();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound).isInstanceOf(PartitionRequest.class);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound)
                    .isInstanceOf(NettyMessage.AckAllUserRecordsProcessed.class);
            assertThat(((NettyMessage.AckAllUserRecordsProcessed) readFromOutbound).receiverId)
                    .isEqualTo(inputChannel.getInputChannelId());

            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    private static NettyPartitionRequestClient createPartitionRequestClient(
            Channel tcpChannel, NetworkClientHandler clientHandler, boolean connectionReuseEnabled)
            throws Exception {
        ConnectionID connectionID =
                new ConnectionID(ResourceID.generate(), new InetSocketAddress("localhost", 0), 0);
        NettyConfig config =
                new NettyConfig(InetAddress.getLocalHost(), 0, 1024, 1, new Configuration());
        NettyClient nettyClient = new NettyClient(config);
        PartitionRequestClientFactory partitionRequestClientFactory =
                new PartitionRequestClientFactory(nettyClient, connectionReuseEnabled);

        return new NettyPartitionRequestClient(
                tcpChannel, clientHandler, connectionID, partitionRequestClientFactory);
    }

    /**
     * Run all pending scheduled tasks (waits until all tasks have been run or the deadline has
     * passed.
     *
     * @param channel the channel to execute tasks for
     * @param deadline maximum timestamp in ms to stop waiting further
     * @throws InterruptedException
     */
    private static void runAllScheduledPendingTasks(EmbeddedChannel channel, long deadline)
            throws InterruptedException {
        // NOTE: we don't have to be super fancy here; busy-polling with 1ms delays is enough
        while (channel.runScheduledPendingTasks() != -1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(1);
        }
    }
}
