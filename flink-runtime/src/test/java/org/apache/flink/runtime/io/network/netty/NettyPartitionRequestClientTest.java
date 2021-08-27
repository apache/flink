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
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.mockConnectionManagerWithPartitionRequestClient;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link NettyPartitionRequestClient}. */
public class NettyPartitionRequestClientTest {

    @Test
    public void testRetriggerPartitionRequest() throws Exception {
        final long deadline = System.currentTimeMillis() + 30_000L; // 30 secs

        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client = createPartitionRequestClient(channel, handler);

        final int numExclusiveBuffers = 2;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel =
                InputChannelBuilder.newBuilder()
                        .setConnectionManager(
                                mockConnectionManagerWithPartitionRequestClient(client))
                        .setInitialBackoff(1)
                        .setMaxBackoff(2)
                        .buildRemoteChannel(inputGate);

        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();

            // first subpartition request
            inputChannel.requestSubpartition(0);

            assertTrue(channel.isWritable());
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

            // retrigger subpartition request, e.g. due to failures
            inputGate.retriggerPartitionRequest(inputChannel.getPartitionId().getPartitionId());
            runAllScheduledPendingTasks(channel, deadline);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

            // retrigger subpartition request once again, e.g. due to failures
            inputGate.retriggerPartitionRequest(inputChannel.getPartitionId().getPartitionId());
            runAllScheduledPendingTasks(channel, deadline);

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

            assertNull(channel.readOutbound());
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @Test
    public void testDoublePartitionRequest() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client = createPartitionRequestClient(channel, handler);

        final int numExclusiveBuffers = 2;
        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            inputGate.setInputChannels(inputChannel);
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartition(0);

            // The input channel should only send one partition request
            assertTrue(channel.isWritable());
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((PartitionRequest) readFromOutbound).receiverId);
            assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

            assertNull(channel.readOutbound());
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @Test
    public void testResumeConsumption() throws Exception {
        final CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final PartitionRequestClient client = createPartitionRequestClient(channel, handler);

        final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        final SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartition(0);

            inputChannel.resumeConsumption();
            channel.runPendingTasks();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(ResumeConsumption.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((ResumeConsumption) readFromOutbound).receiverId);

            assertNull(channel.readOutbound());
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    @Test
    public void testAcknowledgeAllRecordsProcessed() throws Exception {
        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        PartitionRequestClient client = createPartitionRequestClient(channel, handler);

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
        SingleInputGate inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client);

        try {
            BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
            inputGate.setBufferPool(bufferPool);
            inputGate.setupChannels();
            inputChannel.requestSubpartition(0);

            inputChannel.acknowledgeAllRecordsProcessed();
            channel.runPendingTasks();
            Object readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(PartitionRequest.class));

            readFromOutbound = channel.readOutbound();
            assertThat(readFromOutbound, instanceOf(NettyMessage.AckAllUserRecordsProcessed.class));
            assertEquals(
                    inputChannel.getInputChannelId(),
                    ((NettyMessage.AckAllUserRecordsProcessed) readFromOutbound).receiverId);

            assertNull(channel.readOutbound());
        } finally {
            // Release all the buffer resources
            inputGate.close();

            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    private NettyPartitionRequestClient createPartitionRequestClient(
            Channel tcpChannel, NetworkClientHandler clientHandler) throws Exception {
        int port = NetUtils.getAvailablePort();
        ConnectionID connectionID = new ConnectionID(new InetSocketAddress("localhost", port), 0);
        NettyConfig config =
                new NettyConfig(InetAddress.getLocalHost(), port, 1024, 1, new Configuration());
        NettyClient nettyClient = new NettyClient(config);
        PartitionRequestClientFactory partitionRequestClientFactory =
                new PartitionRequestClientFactory(nettyClient);

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
    void runAllScheduledPendingTasks(EmbeddedChannel channel, long deadline)
            throws InterruptedException {
        // NOTE: we don't have to be super fancy here; busy-polling with 1ms delays is enough
        while (channel.runScheduledPendingTasks() != -1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(1);
        }
    }
}
