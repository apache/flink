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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;

import org.junit.Test;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * {@link PartitionRequestClientFactory} test.
 */
public class PartitionRequestClientFactoryTest {

	private static final int SERVER_PORT = NetUtils.getAvailablePort();

	@Test
	public void testNettyClientConnectRetry() throws Exception {
		NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
		UnstableNettyClient unstableNettyClient = new UnstableNettyClient(serverAndClient.client(), 2);

		PartitionRequestClientFactory factory = new PartitionRequestClientFactory(unstableNettyClient, 2);
		ConnectionID serverAddress = new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(),
			serverAndClient.server().getConfig().getServerPort()), 0);

		factory.createPartitionRequestClient(serverAddress);

		serverAndClient.client().shutdown();
		serverAndClient.server().shutdown();
	}

	@Test(expected = CompletionException.class)
	public void testNettyClientConnectRetryFailure() throws Exception {
		NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
		UnstableNettyClient unstableNettyClient = new UnstableNettyClient(serverAndClient.client(), 3);

		try {
			PartitionRequestClientFactory factory = new PartitionRequestClientFactory(unstableNettyClient, 2);
			ConnectionID serverAddress = new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(),
				serverAndClient.server().getConfig().getServerPort()), 0);

			factory.createPartitionRequestClient(serverAddress);

		} catch (Exception e) {
			throw e;
		} finally {
			serverAndClient.client().shutdown();
			serverAndClient.server().shutdown();
		}
	}

	@Test
	public void testNettyClientConnectRetryMultipleThread() throws Exception {
		NettyTestUtil.NettyServerAndClient serverAndClient = createNettyServerAndClient();
		UnstableNettyClient unstableNettyClient = new UnstableNettyClient(serverAndClient.client(), 2);

		PartitionRequestClientFactory factory = new PartitionRequestClientFactory(unstableNettyClient, 2);
		ConnectionID serverAddress = new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(),
			serverAndClient.server().getConfig().getServerPort()), 0);

		ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(10);
		List<Future<NettyPartitionRequestClient>> futures = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			Future<NettyPartitionRequestClient> future = threadPoolExecutor.submit(new Callable<NettyPartitionRequestClient>() {
				@Override
				public NettyPartitionRequestClient call() {
					NettyPartitionRequestClient client = null;
					try {
						client = factory.createPartitionRequestClient(serverAddress);
					} catch (Exception e) {
						fail(e.getMessage());
					}
					return client;
				}
			});

			futures.add(future);
		}

		futures.forEach(runnableFuture -> {
			NettyPartitionRequestClient client = null;
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
		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(
			new NettyProtocol(null, null) {

				@Override
				public ChannelHandler[] getServerChannelHandlers () {
					return new ChannelHandler[10];
				}

				@Override
				public ChannelHandler[] getClientChannelHandlers () {
					return new ChannelHandler[]{mock(NetworkClientHandler.class)};
				}
			});

		return serverAndClient;
	}

	private static class UnstableNettyClient extends NettyClient {

		private NettyClient nettyClient;

		private int retry;

		public UnstableNettyClient(NettyClient nettyClient, int retry) {
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

	private static class CountDownLatchOnConnectHandler extends ChannelOutboundHandlerAdapter {

		private final CountDownLatch syncOnConnect;

		public CountDownLatchOnConnectHandler(CountDownLatch syncOnConnect) {
			this.syncOnConnect = syncOnConnect;
		}

		@Override
		public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
			syncOnConnect.countDown();
		}
	}

	private static class UncaughtTestExceptionHandler implements UncaughtExceptionHandler {

		private final List<Throwable> errors = new ArrayList<Throwable>(1);

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			errors.add(e);
		}

		private List<Throwable> getErrors() {
			return errors;
		}
	}

	// ------------------------------------------------------------------------

	private static Tuple2<NettyServer, NettyClient> createNettyServerAndClient(NettyProtocol protocol) throws IOException {
		final NettyConfig config = new NettyConfig(InetAddress.getLocalHost(), SERVER_PORT, 32 * 1024, 1, new Configuration());

		final NettyServer server = new NettyServer(config);
		final NettyClient client = new NettyClient(config);

		boolean success = false;

		try {
			NettyBufferPool bufferPool = new NettyBufferPool(1);

			server.init(protocol, bufferPool);
			client.init(protocol, bufferPool);

			success = true;
		}
		finally {
			if (!success) {
				server.shutdown();
				client.shutdown();
			}
		}

		return new Tuple2<NettyServer, NettyClient>(server, client);
	}

	private static ConnectionID createServerConnectionID(int connectionIndex) throws UnknownHostException {
		return new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(), SERVER_PORT), connectionIndex);
	}
}
