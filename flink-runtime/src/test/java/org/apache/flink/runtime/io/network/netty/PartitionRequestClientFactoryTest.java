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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.initServerAndClient;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.shutdown;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/** {@link PartitionRequestClientFactory} test. */
@ExtendWith(ParameterizedTestExtension.class)
public class PartitionRequestClientFactoryTest extends TestLogger {
    private static final ResourceID RESOURCE_ID = ResourceID.generate();

    @Parameter public boolean connectionReuseEnabled;

    @Parameters(name = "connectionReuseEnabled={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @TestTemplate
    void testInterruptsNotCached() throws Exception {
        NettyTestUtil.NettyServerAndClient nettyServerAndClient = createNettyServerAndClient();
        try {
            AwaitingNettyClient nettyClient =
                    new AwaitingNettyClient(nettyServerAndClient.client());
            PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(nettyClient, connectionReuseEnabled);

            nettyClient.awaitForInterrupts = true;
            connectAndInterrupt(factory, nettyServerAndClient.getConnectionID(RESOURCE_ID, 0));

            nettyClient.awaitForInterrupts = false;
            factory.createPartitionRequestClient(
                    nettyServerAndClient.getConnectionID(RESOURCE_ID, 0));
        } finally {
            shutdown(nettyServerAndClient);
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

    @TestTemplate
    void testExceptionsAreNotCached() throws Exception {
        NettyTestUtil.NettyServerAndClient nettyServerAndClient = createNettyServerAndClient();

        try {
            final PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(
                            new UnstableNettyClient(nettyServerAndClient.client(), 1),
                            connectionReuseEnabled);

            final ConnectionID connectionID = nettyServerAndClient.getConnectionID(RESOURCE_ID, 0);
            assertThatThrownBy(() -> factory.createPartitionRequestClient(connectionID))
                    .withFailMessage("Expected the first request to fail.")
                    .isInstanceOf(RemoteTransportException.class);

            factory.createPartitionRequestClient(connectionID);
        } finally {
            shutdown(nettyServerAndClient);
        }
    }

    @TestTemplate
    void testReuseNettyPartitionRequestClient() throws Exception {
        NettyTestUtil.NettyServerAndClient nettyServerAndClient = createNettyServerAndClient();
        try {
            checkReuseNettyPartitionRequestClient(nettyServerAndClient, 1);
            checkReuseNettyPartitionRequestClient(nettyServerAndClient, 2);
            checkReuseNettyPartitionRequestClient(nettyServerAndClient, 5);
            checkReuseNettyPartitionRequestClient(nettyServerAndClient, 10);
        } finally {
            shutdown(nettyServerAndClient);
        }
    }

    private void checkReuseNettyPartitionRequestClient(
            NettyTestUtil.NettyServerAndClient nettyServerAndClient, int maxNumberOfConnections)
            throws Exception {
        final Set<NettyPartitionRequestClient> set = new HashSet<>();

        final PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(
                        nettyServerAndClient.client(),
                        0,
                        maxNumberOfConnections,
                        connectionReuseEnabled);
        for (int i = 0; i < Math.max(100, maxNumberOfConnections); i++) {
            final ConnectionID connectionID =
                    nettyServerAndClient.getConnectionID(
                            RESOURCE_ID, (int) (Math.random() * Integer.MAX_VALUE));
            set.add(factory.createPartitionRequestClient(connectionID));
        }
        assertThat(set.size()).isLessThanOrEqualTo(maxNumberOfConnections);
    }

    /**
     * Verify that the netty client reuse when the netty server closes the channel and there is no
     * input channel.
     */
    @TestTemplate
    void testConnectionReuseWhenRemoteCloseAndNoInputChannel() throws Exception {
        CompletableFuture<Void> inactiveFuture = new CompletableFuture<>();
        CompletableFuture<Channel> serverChannelFuture = new CompletableFuture<>();
        NettyProtocol protocol =
                new NettyProtocol(null, null) {
                    @Override
                    public ChannelHandler[] getServerChannelHandlers() {
                        return new ChannelHandler[] {
                            // Close on read
                            new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRegistered(ChannelHandlerContext ctx)
                                        throws Exception {
                                    super.channelRegistered(ctx);
                                    serverChannelFuture.complete(ctx.channel());
                                }
                            }
                        };
                    }

                    @Override
                    public ChannelHandler[] getClientChannelHandlers() {
                        return new ChannelHandler[] {
                            new ChannelInactiveFutureHandler(inactiveFuture)
                        };
                    }
                };
        NettyTestUtil.NettyServerAndClient serverAndClient = initServerAndClient(protocol);

        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(
                        serverAndClient.client(), 2, 1, connectionReuseEnabled);

        ConnectionID connectionID = serverAndClient.getConnectionID(RESOURCE_ID, 0);
        NettyPartitionRequestClient oldClient = factory.createPartitionRequestClient(connectionID);

        // close server channel
        Channel channel = serverChannelFuture.get();
        channel.close();
        inactiveFuture.get();
        NettyPartitionRequestClient newClient = factory.createPartitionRequestClient(connectionID);
        assertThat(newClient).as("Factory should create a new client.").isNotSameAs(oldClient);
        shutdown(serverAndClient);
    }

    @TestTemplate
    void testNettyClientConnectRetry() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 2);

        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(
                        unstableNettyClient, 2, 1, connectionReuseEnabled);

        factory.createPartitionRequestClient(serverAndClient.getConnectionID(RESOURCE_ID, 0));

        shutdown(serverAndClient);
    }

    // see https://issues.apache.org/jira/browse/FLINK-18821
    @TestTemplate
    void testFailureReportedToSubsequentRequests() {
        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(
                        new FailingNettyClient(), 2, 1, connectionReuseEnabled);

        assertThatThrownBy(
                () ->
                        factory.createPartitionRequestClient(
                                new ConnectionID(
                                        ResourceID.generate(),
                                        new InetSocketAddress(InetAddress.getLocalHost(), 8080),
                                        0)));

        assertThatThrownBy(
                        () ->
                                factory.createPartitionRequestClient(
                                        new ConnectionID(
                                                ResourceID.generate(),
                                                new InetSocketAddress(
                                                        InetAddress.getLocalHost(), 8080),
                                                0)))
                .isInstanceOf(IOException.class);
    }

    @TestTemplate
    void testNettyClientConnectRetryFailure() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 3);

        try {
            PartitionRequestClientFactory factory =
                    new PartitionRequestClientFactory(
                            unstableNettyClient, 2, 1, connectionReuseEnabled);

            assertThatThrownBy(
                            () ->
                                    factory.createPartitionRequestClient(
                                            serverAndClient.getConnectionID(RESOURCE_ID, 0)))
                    .isInstanceOf(IOException.class);
        } finally {
            shutdown(serverAndClient);
        }
    }

    @TestTemplate
    void testNettyClientConnectRetryMultipleThread() throws Exception {
        NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
        UnstableNettyClient unstableNettyClient =
                new UnstableNettyClient(serverAndClient.client(), 2);

        PartitionRequestClientFactory factory =
                new PartitionRequestClientFactory(
                        unstableNettyClient, 2, 1, connectionReuseEnabled);

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
                                                    serverAndClient.getConnectionID(
                                                            RESOURCE_ID, 0));
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
                        assertThat(client).isNotNull();
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        fail();
                    }
                });

        threadPoolExecutor.shutdown();
        shutdown(serverAndClient);
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

    private static class ChannelInactiveFutureHandler
            extends CreditBasedPartitionRequestClientHandler {

        private final CompletableFuture<Void> inactiveFuture;

        private ChannelInactiveFutureHandler(CompletableFuture<Void> inactiveFuture) {
            this.inactiveFuture = inactiveFuture;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            inactiveFuture.complete(null);
        }

        public CompletableFuture<Void> getInactiveFuture() {
            return inactiveFuture;
        }
    }
}
