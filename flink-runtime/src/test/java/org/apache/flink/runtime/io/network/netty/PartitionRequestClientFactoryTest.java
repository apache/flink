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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/** {@link PartitionRequestClientFactory} test. */
public class PartitionRequestClientFactoryTest extends TestLogger {

    private static final int SERVER_PORT = NetUtils.getAvailablePort();

    @Test
    public void testInterruptsNotCached() throws Exception {
        NettyTestUtil.NettyServerAndClient nettyServerAndClient = createNettyServerAndClient();
        try {
            AwaitingNettyClient nettyClient =
                    new AwaitingNettyClient(nettyServerAndClient.client());
            PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(nettyClient, 0);

            nettyClient.awaitForInterrupts = true;
            connectAndInterrupt(factory, nettyServerAndClient.getConnectionID(0));

            nettyClient.awaitForInterrupts = false;
            factory.createPartitionRequestClient(nettyServerAndClient.getConnectionID(0));
        } finally {
            nettyServerAndClient.client().shutdown();
            nettyServerAndClient.server().shutdown();
        }
    }

    private void connectAndInterrupt(
            PartitionRequestClientFactory factory, ConnectionID connectionId) throws Exception {
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> interrupted = new CompletableFuture<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                started.complete(null);
                                factory.createPartitionRequestClient(connectionId);
                            } catch (InterruptedException e) {
                                interrupted.complete(null);
                            } catch (Exception e) {
                                interrupted.completeExceptionally(e);
                            }
                        });
        thread.start();
        started.get();
        thread.interrupt();
        interrupted.get();
    }

    @Test
    public void testExceptionsAreNotCached() throws Exception {
        NettyTestUtil.NettyServerAndClient nettyServerAndClient = createNettyServerAndClient();

        try {
            final PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(
                            new UnstableNettyClient(nettyServerAndClient.client(), 1), 0);

            final ConnectionID connectionID = nettyServerAndClient.getConnectionID(0);
            try {
                factory.createPartitionRequestClient(connectionID);
                fail("Expected the first request to fail.");
            } catch (RemoteTransportException expected) {
                // expected
            }

            factory.createPartitionRequestClient(connectionID);
        } finally {
            nettyServerAndClient.client().shutdown();
            nettyServerAndClient.server().shutdown();
        }
    }

    @Test
    public void testNettyClientConnectRetry() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 2);

        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(unstableNettyClient, 2);

        factory.createPartitionRequestClient(serverAndClient.getConnectionID(0));

        serverAndClient.client().shutdown();
        serverAndClient.server().shutdown();
    }

    // see https://issues.apache.org/jira/browse/FLINK-18821
    @Test(expected = IOException.class)
    public void testFailureReportedToSubsequentRequests() throws Exception {
        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(new FailingNettyClient(), 2);
        try {
            factory.createPartitionRequestClient(
                    new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(), 8080), 0));
        } catch (Exception e) {
            // expected
        }
        factory.createPartitionRequestClient(
                new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(), 8080), 0));
    }

    @Test(expected = IOException.class)
    public void testNettyClientConnectRetryFailure() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 3);

        try {
            PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(unstableNettyClient, 2);

            factory.createPartitionRequestClient(serverAndClient.getConnectionID(0));

        } finally {
            serverAndClient.client().shutdown();
            serverAndClient.server().shutdown();
        }
    }

    @Test
    public void testNettyClientConnectRetryMultipleThread() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 2);

        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(unstableNettyClient, 2);

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(10);
        List<Future<NettyPartitionRequestClient>> futures = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Future<NettyPartitionRequestClient> future =
                    threadPoolExecutor.submit(
                            () -> {
                                NettyPartitionRequestClient client = null;
                                try {
                                    client =
                                            factory.createPartitionRequestClient(
                                                    serverAndClient.getConnectionID(0));
                                } catch (Exception e) {
                                    fail(e.getMessage());
                                }
                                return client;
                            });

            futures.add(future);
        }

        futures.forEach(
                runnableFuture -> {
                    NettyPartitionRequestClient client;
                    try {
                        client = runnableFuture.get();
                        assertNotNull(client);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        fail();
                    }
                });

        threadPoolExecutor.shutdown();
        serverAndClient.client().shutdown();
        serverAndClient.server().shutdown();
    }

    private NettyTestUtil.NettyServerAndClient createNettyServerAndClient() throws Exception {
        return NettyTestUtil.initServerAndClient(
                new NettyProtocol(null, null) {

                    @Override
                    public ChannelHandler[] getServerChannelHandlers() {
                        return new ChannelHandler[10];
                    }

                    @Override
                    public ChannelHandler[] getClientChannelHandlers() {
                        return new ChannelHandler[] {mock(NetworkClientHandler.class)};
                    }
                });
    }

    private static class UnstableNettyClient extends NettyClient {

        private final NettyClient nettyClient;

        private int retry;

        UnstableNettyClient(NettyClient nettyClient, int retry) {
            super(null);
            this.nettyClient = nettyClient;
            this.retry = retry;
        }

        @Override
        ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
            if (retry > 0) {
                retry--;
                throw new ChannelException("Simulate connect failure");
            }

            return nettyClient.connect(serverSocketAddress);
        }
    }

    private static class FailingNettyClient extends NettyClient {

        FailingNettyClient() {
            super(null);
        }

        @Override
        ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
            throw new ChannelException("Simulate connect failure");
        }
    }

    private static class AwaitingNettyClient extends NettyClient {
        private volatile boolean awaitForInterrupts;
        private final NettyClient client;

        AwaitingNettyClient(NettyClient client) {
            super(null);
            this.client = client;
        }

        @Override
        ChannelFuture connect(InetSocketAddress serverSocketAddress) {
            if (awaitForInterrupts) {
                return new NeverCompletingChannelFuture();
            }
            try {
                return client.connect(serverSocketAddress);
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }
    }
}
