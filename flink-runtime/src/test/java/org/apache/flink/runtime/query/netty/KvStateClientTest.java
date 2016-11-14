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

package org.apache.flink.runtime.query.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.netty.message.KvStateRequest;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.NetUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KvStateClientTest {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateClientTest.class);

	// Thread pool for client bootstrap (shared between tests)
	private static final NioEventLoopGroup NIO_GROUP = new NioEventLoopGroup();

	private final static FiniteDuration TEST_TIMEOUT = new FiniteDuration(100, TimeUnit.SECONDS);

	@AfterClass
	public static void tearDown() throws Exception {
		if (NIO_GROUP != null) {
			NIO_GROUP.shutdownGracefully();
		}
	}

	/**
	 * Tests simple queries, of which half succeed and half fail.
	 */
	@Test
	public void testSimpleRequests() throws Exception {
		Deadline deadline = TEST_TIMEOUT.fromNow();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateClient client = null;
		Channel serverChannel = null;

		try {
			client = new KvStateClient(1, stats);

			// Random result
			final byte[] expected = new byte[1024];
			ThreadLocalRandom.current().nextBytes(expected);

			final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
			final AtomicReference<Channel> channel = new AtomicReference<>();

			serverChannel = createServerChannel(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelActive(ChannelHandlerContext ctx) throws Exception {
					channel.set(ctx.channel());
				}

				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					received.add((ByteBuf) msg);
				}
			});

			KvStateServerAddress serverAddress = getKvStateServerAddress(serverChannel);

			List<Future<byte[]>> futures = new ArrayList<>();

			int numQueries = 1024;

			for (int i = 0; i < numQueries; i++) {
				futures.add(client.getKvState(serverAddress, new KvStateID(), new byte[0]));
			}

			// Respond to messages
			Exception testException = new RuntimeException("Expected test Exception");

			for (int i = 0; i < numQueries; i++) {
				ByteBuf buf = received.poll(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
				assertNotNull("Receive timed out", buf);

				Channel ch = channel.get();
				assertNotNull("Channel not active", ch);

				assertEquals(KvStateRequestType.REQUEST, KvStateRequestSerializer.deserializeHeader(buf));
				KvStateRequest request = KvStateRequestSerializer.deserializeKvStateRequest(buf);

				buf.release();

				if (i % 2 == 0) {
					ByteBuf response = KvStateRequestSerializer.serializeKvStateRequestResult(
							serverChannel.alloc(),
							request.getRequestId(),
							expected);

					ch.writeAndFlush(response);
				} else {
					ByteBuf response = KvStateRequestSerializer.serializeKvStateRequestFailure(
							serverChannel.alloc(),
							request.getRequestId(),
							testException);

					ch.writeAndFlush(response);
				}
			}

			for (int i = 0; i < numQueries; i++) {
				if (i % 2 == 0) {
					byte[] serializedResult = Await.result(futures.get(i), deadline.timeLeft());
					assertArrayEquals(expected, serializedResult);
				} else {
					try {
						Await.result(futures.get(i), deadline.timeLeft());
						fail("Did not throw expected Exception");
					} catch (RuntimeException ignored) {
						// Expected
					}
				}
			}

			assertEquals(numQueries, stats.getNumRequests());
			int expectedRequests = numQueries / 2;

			// Counts can take some time to propagate
			while (deadline.hasTimeLeft() && (stats.getNumSuccessful() != expectedRequests ||
					stats.getNumFailed() != expectedRequests)) {
				Thread.sleep(100);
			}

			assertEquals(expectedRequests, stats.getNumSuccessful());
			assertEquals(expectedRequests, stats.getNumFailed());
		} finally {
			if (client != null) {
				client.shutDown();
			}

			if (serverChannel != null) {
				serverChannel.close();
			}

			assertEquals("Channel leak", 0, stats.getNumConnections());
		}
	}

	/**
	 * Tests that a request to an unavailable host is failed with ConnectException.
	 */
	@Test
	public void testRequestUnavailableHost() throws Exception {
		Deadline deadline = TEST_TIMEOUT.fromNow();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();
		KvStateClient client = null;

		try {
			client = new KvStateClient(1, stats);

			int availablePort = NetUtils.getAvailablePort();

			KvStateServerAddress serverAddress = new KvStateServerAddress(
					InetAddress.getLocalHost(),
					availablePort);

			Future<byte[]> future = client.getKvState(serverAddress, new KvStateID(), new byte[0]);

			try {
				Await.result(future, deadline.timeLeft());
				fail("Did not throw expected ConnectException");
			} catch (ConnectException ignored) {
				// Expected
			}
		} finally {
			if (client != null) {
				client.shutDown();
			}

			assertEquals("Channel leak", 0, stats.getNumConnections());
		}
	}

	/**
	 * Multiple threads concurrently fire queries.
	 */
	@Test
	public void testConcurrentQueries() throws Exception {
		Deadline deadline = TEST_TIMEOUT.fromNow();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		ExecutorService executor = null;
		KvStateClient client = null;
		Channel serverChannel = null;

		final byte[] serializedResult = new byte[1024];
		ThreadLocalRandom.current().nextBytes(serializedResult);

		try {
			int numQueryTasks = 4;
			final int numQueriesPerTask = 1024;

			executor = Executors.newFixedThreadPool(numQueryTasks);

			client = new KvStateClient(1, stats);

			serverChannel = createServerChannel(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					ByteBuf buf = (ByteBuf) msg;
					assertEquals(KvStateRequestType.REQUEST, KvStateRequestSerializer.deserializeHeader(buf));
					KvStateRequest request = KvStateRequestSerializer.deserializeKvStateRequest(buf);

					buf.release();

					ByteBuf response = KvStateRequestSerializer.serializeKvStateRequestResult(
							ctx.alloc(),
							request.getRequestId(),
							serializedResult);

					ctx.channel().writeAndFlush(response);
				}
			});

			final KvStateServerAddress serverAddress = getKvStateServerAddress(serverChannel);

			final KvStateClient finalClient = client;
			Callable<List<Future<byte[]>>> queryTask = new Callable<List<Future<byte[]>>>() {
				@Override
				public List<Future<byte[]>> call() throws Exception {
					List<Future<byte[]>> results = new ArrayList<>(numQueriesPerTask);

					for (int i = 0; i < numQueriesPerTask; i++) {
						results.add(finalClient.getKvState(
								serverAddress,
								new KvStateID(),
								new byte[0]));
					}

					return results;
				}
			};

			// Submit query tasks
			List<java.util.concurrent.Future<List<Future<byte[]>>>> futures = new ArrayList<>();
			for (int i = 0; i < numQueryTasks; i++) {
				futures.add(executor.submit(queryTask));
			}

			// Verify results
			for (java.util.concurrent.Future<List<Future<byte[]>>> future : futures) {
				List<Future<byte[]>> results = future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
				for (Future<byte[]> result : results) {
					byte[] actual = Await.result(result, deadline.timeLeft());
					assertArrayEquals(serializedResult, actual);
				}
			}

			int totalQueries = numQueryTasks * numQueriesPerTask;

			// Counts can take some time to propagate
			while (deadline.hasTimeLeft() && stats.getNumSuccessful() != totalQueries) {
				Thread.sleep(100);
			}

			assertEquals(totalQueries, stats.getNumRequests());
			assertEquals(totalQueries, stats.getNumSuccessful());
		} finally {
			if (executor != null) {
				executor.shutdown();
			}

			if (serverChannel != null) {
				serverChannel.close();
			}

			if (client != null) {
				client.shutDown();
			}

			assertEquals("Channel leak", 0, stats.getNumConnections());
		}
	}

	/**
	 * Tests that a server failure closes the connection and removes it from
	 * the established connections.
	 */
	@Test
	public void testFailureClosesChannel() throws Exception {
		Deadline deadline = TEST_TIMEOUT.fromNow();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateClient client = null;
		Channel serverChannel = null;

		try {
			client = new KvStateClient(1, stats);

			final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
			final AtomicReference<Channel> channel = new AtomicReference<>();

			serverChannel = createServerChannel(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelActive(ChannelHandlerContext ctx) throws Exception {
					channel.set(ctx.channel());
				}

				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					received.add((ByteBuf) msg);
				}
			});

			KvStateServerAddress serverAddress = getKvStateServerAddress(serverChannel);

			// Requests
			List<Future<byte[]>> futures = new ArrayList<>();
			futures.add(client.getKvState(serverAddress, new KvStateID(), new byte[0]));
			futures.add(client.getKvState(serverAddress, new KvStateID(), new byte[0]));

			ByteBuf buf = received.poll(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			assertNotNull("Receive timed out", buf);
			buf.release();

			buf = received.poll(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			assertNotNull("Receive timed out", buf);
			buf.release();

			assertEquals(1, stats.getNumConnections());

			Channel ch = channel.get();
			assertNotNull("Channel not active", ch);

			// Respond with failure
			ch.writeAndFlush(KvStateRequestSerializer.serializeServerFailure(
					serverChannel.alloc(),
					new RuntimeException("Expected test server failure")));

			try {
				Await.result(futures.remove(0), deadline.timeLeft());
				fail("Did not throw expected server failure");
			} catch (RuntimeException ignored) {
				// Expected
			}

			try {
				Await.result(futures.remove(0), deadline.timeLeft());
				fail("Did not throw expected server failure");
			} catch (RuntimeException ignored) {
				// Expected
			}

			assertEquals(0, stats.getNumConnections());

			// Counts can take some time to propagate
			while (deadline.hasTimeLeft() && (stats.getNumSuccessful() != 0 ||
					stats.getNumFailed() != 2)) {
				Thread.sleep(100);
			}

			assertEquals(2, stats.getNumRequests());
			assertEquals(0, stats.getNumSuccessful());
			assertEquals(2, stats.getNumFailed());
		} finally {
			if (client != null) {
				client.shutDown();
			}

			if (serverChannel != null) {
				serverChannel.close();
			}

			assertEquals("Channel leak", 0, stats.getNumConnections());
		}
	}

	/**
	 * Tests that a server channel close, closes the connection and removes it
	 * from the established connections.
	 */
	@Test
	public void testServerClosesChannel() throws Exception {
		Deadline deadline = TEST_TIMEOUT.fromNow();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateClient client = null;
		Channel serverChannel = null;

		try {
			client = new KvStateClient(1, stats);

			final AtomicBoolean received = new AtomicBoolean();
			final AtomicReference<Channel> channel = new AtomicReference<>();

			serverChannel = createServerChannel(new ChannelInboundHandlerAdapter() {
				@Override
				public void channelActive(ChannelHandlerContext ctx) throws Exception {
					channel.set(ctx.channel());
				}

				@Override
				public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
					received.set(true);
				}
			});

			KvStateServerAddress serverAddress = getKvStateServerAddress(serverChannel);

			// Requests
			Future<byte[]> future = client.getKvState(serverAddress, new KvStateID(), new byte[0]);

			while (!received.get() && deadline.hasTimeLeft()) {
				Thread.sleep(50);
			}
			assertTrue("Receive timed out", received.get());

			assertEquals(1, stats.getNumConnections());

			channel.get().close().await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

			try {
				Await.result(future, deadline.timeLeft());
				fail("Did not throw expected server failure");
			} catch (ClosedChannelException ignored) {
				// Expected
			}

			assertEquals(0, stats.getNumConnections());

			// Counts can take some time to propagate
			while (deadline.hasTimeLeft() && (stats.getNumSuccessful() != 0 ||
					stats.getNumFailed() != 1)) {
				Thread.sleep(100);
			}

			assertEquals(1, stats.getNumRequests());
			assertEquals(0, stats.getNumSuccessful());
			assertEquals(1, stats.getNumFailed());
		} finally {
			if (client != null) {
				client.shutDown();
			}

			if (serverChannel != null) {
				serverChannel.close();
			}

			assertEquals("Channel leak", 0, stats.getNumConnections());
		}
	}

	/**
	 * Tests multiple clients querying multiple servers until 100k queries have
	 * been processed. At this point, the client is shut down and its verified
	 * that all ongoing requests are failed.
	 */
	@Test
	public void testClientServerIntegration() throws Exception {
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

		AbstractKeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				dummyRegistry.createTaskRegistry(new JobID(), new JobVertexID()));


		final FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

		AtomicKvStateRequestStats clientStats = new AtomicKvStateRequestStats();

		KvStateClient client = null;
		ExecutorService clientTaskExecutor = null;
		final KvStateServer[] server = new KvStateServer[numServers];

		try {
			client = new KvStateClient(numClientEventLoopThreads, clientStats);
			clientTaskExecutor = Executors.newFixedThreadPool(numClientsTasks);

			// Create state
			ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE, null);
			desc.setQueryable("any");

			// Create servers
			KvStateRegistry[] registry = new KvStateRegistry[numServers];
			AtomicKvStateRequestStats[] serverStats = new AtomicKvStateRequestStats[numServers];
			final KvStateID[] ids = new KvStateID[numServers];

			for (int i = 0; i < numServers; i++) {
				registry[i] = new KvStateRegistry();
				serverStats[i] = new AtomicKvStateRequestStats();
				server[i] = new KvStateServer(
						InetAddress.getLocalHost(),
						0,
						numServerEventLoopThreads,
						numServerQueryThreads,
						registry[i],
						serverStats[i]);

				server[i].start();

				backend.setCurrentKey(1010 + i);

				// Value per server
				ValueState<Integer> state = backend.getPartitionedState(VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE,
						desc);

				state.update(201 + i);

				// we know it must be a KvStat but this is not exposed to the user via State
				KvState<?> kvState = (KvState<?>) state;

				// Register KvState (one state instance for all server)
				ids[i] = registry[i].registerKvState(new JobID(), new JobVertexID(), new KeyGroupRange(0, 0), "any", kvState);
			}

			final KvStateClient finalClient = client;
			Callable<Void> queryTask = new Callable<Void>() {
				@Override
				public Void call() throws Exception {
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
						List<Future<byte[]>> futures = new ArrayList<>(batchSize);

						for (int j = 0; j < batchSize; j++) {
							int targetServer = random.get(j) % numServers;

							byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
									1010 + targetServer,
									IntSerializer.INSTANCE,
									VoidNamespace.INSTANCE,
									VoidNamespaceSerializer.INSTANCE);

							futures.add(finalClient.getKvState(
									server[targetServer].getAddress(),
									ids[targetServer],
									serializedKeyAndNamespace));
						}

						// Verify results
						for (int j = 0; j < batchSize; j++) {
							int targetServer = random.get(j) % numServers;

							Future<byte[]> future = futures.get(j);
							byte[] buf = Await.result(future, timeout);
							int value = KvStateRequestSerializer.deserializeValue(buf, IntSerializer.INSTANCE);
							assertEquals(201 + targetServer, value);
						}
					}
				}
			};

			// Submit tasks
			List<java.util.concurrent.Future<Void>> taskFutures = new ArrayList<>();
			for (int i = 0; i < numClientsTasks; i++) {
				taskFutures.add(clientTaskExecutor.submit(queryTask));
			}

			long numRequests;
			while ((numRequests = clientStats.getNumRequests()) < 100_000) {
				Thread.sleep(100);
				LOG.info("Number of requests {}/100_000", numRequests);
			}

			// Shut down
			client.shutDown();

			for (java.util.concurrent.Future<Void> future : taskFutures) {
				try {
					future.get();
					fail("Did not throw expected Exception after shut down");
				} catch (ExecutionException t) {
					if (t.getCause() instanceof ClosedChannelException ||
							t.getCause() instanceof IllegalStateException) {
						// Expected
					} else {
						t.printStackTrace();
						fail("Failed with unexpected Exception type: " + t.getClass().getName());
					}
				}
			}

			assertEquals("Connection leak (client)", 0, clientStats.getNumConnections());
			for (int i = 0; i < numServers; i++) {
				boolean success = false;
				int numRetries = 0;
				while (!success) {
					try {
						assertEquals("Connection leak (server)", 0, serverStats[i].getNumConnections());
						success = true;
					} catch (Throwable t) {
						if (numRetries < 10) {
							LOG.info("Retrying connection leak check (server)");
							Thread.sleep((numRetries + 1) * 50);
							numRetries++;
						} else {
							throw t;
						}
					}
				}
			}
		} finally {
			if (client != null) {
				client.shutDown();
			}

			for (int i = 0; i < numServers; i++) {
				if (server[i] != null) {
					server[i].shutDown();
				}
			}

			if (clientTaskExecutor != null) {
				clientTaskExecutor.shutdown();
			}
		}
	}

	// ------------------------------------------------------------------------

	private Channel createServerChannel(final ChannelHandler... handlers) throws UnknownHostException, InterruptedException {
		ServerBootstrap bootstrap = new ServerBootstrap()
				// Bind address and port
				.localAddress(InetAddress.getLocalHost(), 0)
				// NIO server channels
				.group(NIO_GROUP)
				.channel(NioServerSocketChannel.class)
				// See initializer for pipeline details
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline()
								.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
								.addLast(handlers);
					}
				});

		return bootstrap.bind().sync().channel();
	}

	private KvStateServerAddress getKvStateServerAddress(Channel serverChannel) {
		InetSocketAddress localAddress = (InetSocketAddress) serverChannel.localAddress();

		return new KvStateServerAddress(localAddress.getAddress(), localAddress.getPort());
	}
}
