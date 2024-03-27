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

import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.testutils.TestingUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.connect;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerTransportErrorHandlingTest {

    /** Verifies remote closes trigger the release of all resources. */
    @Test
    void testRemoteClose() throws Exception {
        final TestPooledBufferProvider outboundBuffers = new TestPooledBufferProvider(16);

        final CountDownLatch sync = new CountDownLatch(1);

        final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);

        when(partitionManager.createSubpartitionViewOrRegisterListener(
                        any(ResultPartitionID.class),
                        any(ResultSubpartitionIndexSet.class),
                        any(BufferAvailabilityListener.class),
                        any(PartitionRequestListener.class)))
                .thenAnswer(
                        (Answer<Optional<ResultSubpartitionView>>)
                                invocationOnMock ->
                                        Optional.of(
                                                new CancelPartitionRequestTest
                                                        .InfiniteSubpartitionView(
                                                        outboundBuffers, sync)));

        NettyProtocol protocol =
                new NettyProtocol(partitionManager, mock(TaskEventDispatcher.class)) {

                    @Override
                    public ChannelHandler[] getClientChannelHandlers() {
                        return new ChannelHandler[] {
                            new NettyMessage.NettyMessageEncoder(),
                            // Close on read
                            new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    ctx.channel().close();
                                }
                            }
                        };
                    }
                };

        NettyTestUtil.NettyServerAndClient serverAndClient = null;

        try {
            serverAndClient = initServerAndClient(protocol);
            Channel ch = connect(serverAndClient);

            // Write something to trigger close by server
            ch.writeAndFlush(
                    new NettyMessage.PartitionRequest(
                            new ResultPartitionID(),
                            new ResultSubpartitionIndexSet(0),
                            new InputChannelID(),
                            Integer.MAX_VALUE));

            // Wait for the notification
            assertThat(sync.await(TestingUtils.TESTING_DURATION.toMillis(), TimeUnit.MILLISECONDS))
                    .withFailMessage(
                            "Timed out after waiting for "
                                    + TestingUtils.TESTING_DURATION.toMillis()
                                    + " ms to be notified about released partition.")
                    .isTrue();
        } finally {
            shutdown(serverAndClient);
        }
    }
}
