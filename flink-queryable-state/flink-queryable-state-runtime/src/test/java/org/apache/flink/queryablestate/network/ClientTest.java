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

package org.apache.flink.queryablestate.network;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.MessageType;
import org.apache.flink.queryablestate.network.stats.AtomicKvStateRequestStats;
import org.apache.flink.queryablestate.server.KvStateServerImpl;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Client}. */
class ClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTest.class);

    // Thread pool for client bootstrap (shared between tests)
    private NioEventLoopGroup nioGroup;

    @BeforeEach
    void setUp() {
        nioGroup = new NioEventLoopGroup();
    }

    @AfterEach
    void tearDown() {
        if (nioGroup != null) {
            // note: no "quiet period" to not trigger Netty#4357
            nioGroup.shutdownGracefully();
        }
    }

    /** Tests simple queries, of which half succeed and half fail. */
    @Test
    void testSimpleRequests() throws Exception {
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        Client<KvStateInternalRequest, KvStateResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new Client<>("Test Client", 1, serializer, stats);

            // Random result
            final byte[] expected = new byte[1024];
            ThreadLocalRandom.current().nextBytes(expected);

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getKvStateServerAddress(serverChannel);

            long numQueries = 1024L;

            List<CompletableFuture<KvStateResponse>> futures = new ArrayList<>();
            for (long i = 0L; i < numQueries; i++) {
                KvStateInternalRequest request =
                        new KvStateInternalRequest(new KvStateID(), new byte[0]);
                futures.add(client.sendRequest(serverAddress, request));
            }

            // Respond to messages
            Exception testException = new RuntimeException("Expected test Exception");

            for (long i = 0L; i < numQueries; i++) {
                ByteBuf buf = received.take();
                assertThat(buf).withFailMessage("Receive timed out").isNotNull();

                Channel ch = channel.get();
                assertThat(ch).withFailMessage("Channel not active").isNotNull();

                assertThat(MessageType.REQUEST).isEqualTo(MessageSerializer.deserializeHeader(buf));
                long requestId = MessageSerializer.getRequestId(buf);

                buf.release();

                if (i % 2L == 0L) {
                    ByteBuf response =
                            MessageSerializer.serializeResponse(
                                    serverChannel.alloc(),
                                    requestId,
                                    new KvStateResponse(expected));

                    ch.writeAndFlush(response);
                } else {
                    ByteBuf response =
                            MessageSerializer.serializeRequestFailure(
                                    serverChannel.alloc(), requestId, testException);

                    ch.writeAndFlush(response);
                }
            }

            for (long i = 0L; i < numQueries; i++) {

                if (i % 2L == 0L) {
                    KvStateResponse serializedResult = futures.get((int) i).get();
                    assertThat(expected).containsExactly(serializedResult.getContent());
                } else {
                    CompletableFuture<KvStateResponse> future = futures.get((int) i);
                    FlinkAssertions.assertThatFuture(future)
                            .eventuallyFailsWith(ExecutionException.class)
                            .satisfies(FlinkAssertions.anyCauseMatches(RuntimeException.class));
                }
            }

            assertThat(numQueries).isEqualTo(stats.getNumRequests());
            long expectedRequests = numQueries / 2L;

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != expectedRequests
                    || stats.getNumFailed() != expectedRequests) {
                Thread.sleep(100L);
            }

            assertThat(expectedRequests).isEqualTo(stats.getNumSuccessful());
            assertThat(expectedRequests).isEqualTo(stats.getNumFailed());
        } finally {
            if (client != null) {
                Exception exc = null;
                try {

                    // todo here we were seeing this problem:
                    // https://github.com/netty/netty/issues/4357 if we do a get().
                    // this is why we now simply wait a bit so that everything is
                    // shut down and then we check

                    client.shutdown().get();
                } catch (Exception e) {
                    exc = e;
                    LOG.error("An exception occurred while shutting down netty.", e);
                }

                assertThat(client.isEventGroupShutdown())
                        .withFailMessage(ExceptionUtils.stringifyException(exc))
                        .isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /** Tests that a request to an unavailable host is failed with ConnectException. */
    @Test
    void testRequestUnavailableHost() {
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        Client<KvStateInternalRequest, KvStateResponse> client = null;

        try {
            client = new Client<>("Test Client", 1, serializer, stats);

            // Since no real servers are created based on the server address, the given fixed port
            // is enough.
            InetSocketAddress serverAddress =
                    new InetSocketAddress("flink-qs-client-test-unavailable-host", 12345);

            KvStateInternalRequest request =
                    new KvStateInternalRequest(new KvStateID(), new byte[0]);
            CompletableFuture<KvStateResponse> future = client.sendRequest(serverAddress, request);

            assertThat(future).isNotNull();
            assertThatThrownBy(future::get).hasRootCauseInstanceOf(ConnectException.class);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /** Multiple threads concurrently fire queries. */
    @Test
    void testConcurrentQueries() throws Exception {
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ExecutorService executor = null;
        Client<KvStateInternalRequest, KvStateResponse> client = null;
        Channel serverChannel = null;

        final byte[] serializedResult = new byte[1024];
        ThreadLocalRandom.current().nextBytes(serializedResult);

        try {
            int numQueryTasks = 4;
            final int numQueriesPerTask = 1024;

            executor = Executors.newFixedThreadPool(numQueryTasks);

            client = new Client<>("Test Client", 1, serializer, stats);

            serverChannel =
                    createServerChannel(new RespondingChannelHandler(serializer, serializedResult));

            final InetSocketAddress serverAddress = getKvStateServerAddress(serverChannel);

            final Client<KvStateInternalRequest, KvStateResponse> finalClient = client;
            Callable<List<CompletableFuture<KvStateResponse>>> queryTask =
                    () -> {
                        List<CompletableFuture<KvStateResponse>> results =
                                new ArrayList<>(numQueriesPerTask);

                        for (int i = 0; i < numQueriesPerTask; i++) {
                            KvStateInternalRequest request =
                                    new KvStateInternalRequest(new KvStateID(), new byte[0]);
                            results.add(finalClient.sendRequest(serverAddress, request));
                        }

                        return results;
                    };

            // Submit query tasks
            List<Future<List<CompletableFuture<KvStateResponse>>>> futures = new ArrayList<>();
            for (int i = 0; i < numQueryTasks; i++) {
                futures.add(executor.submit(queryTask));
            }

            // Verify results
            for (Future<List<CompletableFuture<KvStateResponse>>> future : futures) {
                List<CompletableFuture<KvStateResponse>> results = future.get();
                for (CompletableFuture<KvStateResponse> result : results) {
                    KvStateResponse actual = result.get();
                    assertThat(serializedResult).containsExactly(actual.getContent());
                }
            }

            int totalQueries = numQueryTasks * numQueriesPerTask;

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != totalQueries) {
                Thread.sleep(100L);
            }

            assertThat(totalQueries).isEqualTo(stats.getNumRequests());
            assertThat(totalQueries).isEqualTo(stats.getNumSuccessful());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /**
     * Tests that a server failure closes the connection and removes it from the established
     * connections.
     */
    @Test
    void testFailureClosesChannel() throws Exception {
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        Client<KvStateInternalRequest, KvStateResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new Client<>("Test Client", 1, serializer, stats);

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getKvStateServerAddress(serverChannel);

            // Requests
            List<CompletableFuture<KvStateResponse>> futures = new ArrayList<>();
            KvStateInternalRequest request =
                    new KvStateInternalRequest(new KvStateID(), new byte[0]);

            futures.add(client.sendRequest(serverAddress, request));
            futures.add(client.sendRequest(serverAddress, request));

            ByteBuf buf = received.take();
            assertThat(buf).withFailMessage("Receive timed out").isNotNull();
            buf.release();

            buf = received.take();
            assertThat(buf).withFailMessage("Receive timed out").isNotNull();
            buf.release();

            assertThat(stats.getNumConnections()).isEqualTo(1L);

            Channel ch = channel.get();
            assertThat(ch).withFailMessage("Channel not active").isNotNull();

            // Respond with failure
            ch.writeAndFlush(
                    MessageSerializer.serializeServerFailure(
                            serverChannel.alloc(),
                            new RuntimeException("Expected test server failure")));

            CompletableFuture<KvStateResponse> removedFuture = futures.remove(0);
            FlinkAssertions.assertThatFuture(removedFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(FlinkAssertions.anyCauseMatches(RuntimeException.class));

            removedFuture = futures.remove(0);
            FlinkAssertions.assertThatFuture(removedFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(FlinkAssertions.anyCauseMatches(RuntimeException.class));

            assertThat(stats.getNumConnections()).isZero();

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != 0L || stats.getNumFailed() != 2L) {
                Thread.sleep(100L);
            }

            assertThat(stats.getNumRequests()).isEqualTo(2L);
            assertThat(stats.getNumSuccessful()).isZero();
            assertThat(stats.getNumFailed()).isEqualTo(2L);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /**
     * Tests that a server channel close, closes the connection and removes it from the established
     * connections.
     */
    @Test
    void testServerClosesChannel() throws Exception {
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        Client<KvStateInternalRequest, KvStateResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new Client<>("Test Client", 1, serializer, stats);

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getKvStateServerAddress(serverChannel);

            // Requests
            KvStateInternalRequest request =
                    new KvStateInternalRequest(new KvStateID(), new byte[0]);
            CompletableFuture<KvStateResponse> future = client.sendRequest(serverAddress, request);

            received.take();

            assertThat(stats.getNumConnections()).isEqualTo(1);

            channel.get().close().await();

            FlinkAssertions.assertThatFuture(future)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(FlinkAssertions.anyCauseMatches(ClosedChannelException.class));

            assertThat(stats.getNumConnections()).isZero();

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != 0L || stats.getNumFailed() != 1L) {
                Thread.sleep(100L);
            }

            assertThat(stats.getNumRequests()).isEqualTo(1L);
            assertThat(stats.getNumSuccessful()).isZero();
            assertThat(stats.getNumFailed()).isEqualTo(1L);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /**
     * Tests multiple clients querying multiple servers until 100k queries have been processed. At
     * this point, the client is shut down and its verified that all ongoing requests are failed.
     */
    @Test
    void testClientServerIntegration() throws Throwable {
        // Config
        final int numServers = 2;
        final int numServerEventLoopThreads = 2;
        final int numServerQueryThreads = 2;

        final int numClientEventLoopThreads = 4;
        final int numClientsTasks = 8;

        final int batchSize = 16;

        final int numKeyGroups = 1;

        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        KvStateRegistry dummyRegistry = new KvStateRegistry();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(dummyRegistry);

        AbstractKeyedStateBackend<Integer> backend =
                abstractBackend.createKeyedStateBackend(
                        dummyEnv,
                        new JobID(),
                        "test_op",
                        IntSerializer.INSTANCE,
                        numKeyGroups,
                        new KeyGroupRange(0, 0),
                        dummyRegistry.createTaskRegistry(new JobID(), new JobVertexID()),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());

        AtomicKvStateRequestStats clientStats = new AtomicKvStateRequestStats();

        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        Client<KvStateInternalRequest, KvStateResponse> client = null;
        ExecutorService clientTaskExecutor = null;
        final KvStateServerImpl[] server = new KvStateServerImpl[numServers];

        try {
            client =
                    new Client<>("Test Client", numClientEventLoopThreads, serializer, clientStats);
            clientTaskExecutor = Executors.newFixedThreadPool(numClientsTasks);

            // Create state
            ValueStateDescriptor<Integer> desc =
                    new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
            desc.setQueryable("any");

            // Create servers
            KvStateRegistry[] registry = new KvStateRegistry[numServers];
            AtomicKvStateRequestStats[] serverStats = new AtomicKvStateRequestStats[numServers];
            final KvStateID[] ids = new KvStateID[numServers];

            for (int i = 0; i < numServers; i++) {
                registry[i] = new KvStateRegistry();
                serverStats[i] = new AtomicKvStateRequestStats();
                server[i] =
                        new KvStateServerImpl(
                                InetAddress.getLocalHost().getHostName(),
                                Collections.singletonList(0).iterator(),
                                numServerEventLoopThreads,
                                numServerQueryThreads,
                                registry[i],
                                serverStats[i]);

                server[i].start();

                backend.setCurrentKey(1010 + i);

                // Value per server
                ValueState<Integer> state =
                        backend.getPartitionedState(
                                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

                state.update(201 + i);

                // we know it must be a KvState but this is not exposed to the user via State
                InternalKvState<Integer, ?, Integer> kvState =
                        (InternalKvState<Integer, ?, Integer>) state;

                // Register KvState (one state instance for all server)
                ids[i] =
                        registry[i].registerKvState(
                                new JobID(),
                                new JobVertexID(),
                                new KeyGroupRange(0, 0),
                                "any",
                                kvState,
                                getClass().getClassLoader());
            }

            final Client<KvStateInternalRequest, KvStateResponse> finalClient = client;
            Callable<Void> queryTask =
                    () -> {
                        while (true) {
                            if (Thread.interrupted()) {
                                throw new InterruptedException();
                            }

                            // Random server permutation
                            List<Integer> random = new ArrayList<>();
                            for (int j = 0; j < batchSize; j++) {
                                random.add(j);
                            }
                            Collections.shuffle(random);

                            // Dispatch queries
                            List<CompletableFuture<KvStateResponse>> futures =
                                    new ArrayList<>(batchSize);

                            for (int j = 0; j < batchSize; j++) {
                                int targetServer = random.get(j) % numServers;

                                byte[] serializedKeyAndNamespace =
                                        KvStateSerializer.serializeKeyAndNamespace(
                                                1010 + targetServer,
                                                IntSerializer.INSTANCE,
                                                VoidNamespace.INSTANCE,
                                                VoidNamespaceSerializer.INSTANCE);

                                KvStateInternalRequest request =
                                        new KvStateInternalRequest(
                                                ids[targetServer], serializedKeyAndNamespace);
                                futures.add(
                                        finalClient.sendRequest(
                                                server[targetServer].getServerAddress(), request));
                            }

                            // Verify results
                            for (int j = 0; j < batchSize; j++) {
                                int targetServer = random.get(j) % numServers;

                                Future<KvStateResponse> future = futures.get(j);
                                byte[] buf = future.get().getContent();
                                int value =
                                        KvStateSerializer.deserializeValue(
                                                buf, IntSerializer.INSTANCE);
                                assertThat(value).isEqualTo(201L + targetServer);
                            }
                        }
                    };

            // Submit tasks
            List<Future<Void>> taskFutures = new ArrayList<>();
            for (int i = 0; i < numClientsTasks; i++) {
                taskFutures.add(clientTaskExecutor.submit(queryTask));
            }

            long numRequests;
            while ((numRequests = clientStats.getNumRequests()) < 100_000L) {
                Thread.sleep(100L);
                LOG.info("Number of requests {}/100_000", numRequests);
            }

            try {
                client.shutdown().get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertThat(client.isEventGroupShutdown()).isTrue();

            for (Future<Void> future : taskFutures) {
                try {
                    future.get();
                } catch (Throwable throwable) {
                    FlinkAssertions.assertThatChainOfCauses(throwable)
                            .anySatisfy(
                                    cause ->
                                            assertThat(cause)
                                                    .isInstanceOfAny(
                                                            ClosedChannelException.class,
                                                            IllegalStateException.class));
                }
            }

            assertThat(clientStats.getNumConnections())
                    .withFailMessage("Connection leak (client)")
                    .isZero();
            for (int i = 0; i < numServers; i++) {
                boolean success = false;
                int numRetries = 0;
                while (!success) {
                    try {
                        assertThat(serverStats[i].getNumConnections())
                                .withFailMessage("Connection leak (server)")
                                .isZero();
                        success = true;
                    } catch (Throwable t) {
                        if (numRetries < 10) {
                            LOG.info("Retrying connection leak check (server)");
                            Thread.sleep((numRetries + 1) * 50L);
                            numRetries++;
                        } else {
                            throw t;
                        }
                    }
                }
            }
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            for (int i = 0; i < numServers; i++) {
                if (server[i] != null) {
                    server[i].shutdown();
                }
            }

            if (clientTaskExecutor != null) {
                clientTaskExecutor.shutdown();
            }
        }
    }

    // ------------------------------------------------------------------------

    private Channel createServerChannel(final ChannelHandler... handlers)
            throws UnknownHostException, InterruptedException {
        ServerBootstrap bootstrap =
                new ServerBootstrap()
                        // Bind address and port
                        .localAddress(InetAddress.getLocalHost(), 0)
                        // NIO server channels
                        .group(nioGroup)
                        .channel(NioServerSocketChannel.class)
                        // See initializer for pipeline details
                        .childHandler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel ch) throws Exception {
                                        ch.pipeline()
                                                .addLast(
                                                        new LengthFieldBasedFrameDecoder(
                                                                Integer.MAX_VALUE, 0, 4, 0, 4))
                                                .addLast(handlers);
                                    }
                                });

        return bootstrap.bind().sync().channel();
    }

    private InetSocketAddress getKvStateServerAddress(Channel serverChannel) {
        return (InetSocketAddress) serverChannel.localAddress();
    }

    private static class ChannelDataCollectingHandler extends ChannelInboundHandlerAdapter {
        private final AtomicReference<Channel> channel;
        private final LinkedBlockingQueue<ByteBuf> received;

        private ChannelDataCollectingHandler(
                AtomicReference<Channel> channel, LinkedBlockingQueue<ByteBuf> received) {
            this.channel = channel;
            this.received = received;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            channel.set(ctx.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            received.add((ByteBuf) msg);
        }
    }

    @ChannelHandler.Sharable
    private static final class RespondingChannelHandler extends ChannelInboundHandlerAdapter {
        private final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer;
        private final byte[] serializedResult;

        private RespondingChannelHandler(
                MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer,
                byte[] serializedResult) {
            this.serializer = serializer;
            this.serializedResult = serializedResult;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST);
            long requestId = MessageSerializer.getRequestId(buf);
            KvStateInternalRequest request = serializer.deserializeRequest(buf);

            buf.release();

            KvStateResponse response = new KvStateResponse(serializedResult);
            ByteBuf serResponse =
                    MessageSerializer.serializeResponse(ctx.alloc(), requestId, response);

            ctx.channel().writeAndFlush(serResponse);
        }
    }
}
