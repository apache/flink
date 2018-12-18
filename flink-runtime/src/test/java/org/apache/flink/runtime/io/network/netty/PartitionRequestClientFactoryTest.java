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
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class PartitionRequestClientFactoryTest {

	private final static int SERVER_PORT = NetUtils.getAvailablePort();

	@Test
	public void testResourceReleaseAfterInterruptedConnect() throws Exception {

		// Latch to synchronize on the connect call.
		final CountDownLatch syncOnConnect = new CountDownLatch(1);

		final Tuple2<NettyServer, NettyClient> netty = createNettyServerAndClient(
				new NettyProtocol(null, null, true) {

					@Override
					public ChannelHandler[] getServerChannelHandlers() {
						return new ChannelHandler[0];
					}

					@Override
					public ChannelHandler[] getClientChannelHandlers() {
						return new ChannelHandler[] {
								new CountDownLatchOnConnectHandler(syncOnConnect)};
					}
				});

		final NettyServer server = netty.f0;
		final NettyClient client = netty.f1;

		final UncaughtTestExceptionHandler exceptionHandler = new UncaughtTestExceptionHandler();

		try {
			final PartitionRequestClientFactory factory = new PartitionRequestClientFactory(client);

			final Thread connect = new Thread(new Runnable() {
				@Override
				public void run() {
					ConnectionID serverAddress = null;

					try {
						serverAddress = createServerConnectionID(0);

						// This triggers a connect
						factory.createPartitionRequestClient(serverAddress);
					}
					catch (Throwable t) {

						if (serverAddress != null) {
							factory.closeOpenChannelConnections(serverAddress);
							Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
						} else {
							t.printStackTrace();
							fail("Could not create RemoteAddress for server.");
						}
					}
				}
			});

			connect.setUncaughtExceptionHandler(exceptionHandler);

			connect.start();

			// Wait on the connect
			syncOnConnect.await();

			connect.interrupt();
			connect.join();

			// Make sure that after a failed connect all resources are cleared.
			assertEquals(0, factory.getNumberOfActiveClients());

			// Make sure that the interrupt exception is not swallowed
			assertTrue(exceptionHandler.getErrors().size() > 0);
		}
		finally {
			if (server != null) {
				server.shutdown();
			}

			if (client != null) {
				client.shutdown();
			}
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
