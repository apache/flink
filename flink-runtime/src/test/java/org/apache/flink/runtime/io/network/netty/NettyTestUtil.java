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
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test utility for Netty server and client setup.
 */
public class NettyTestUtil {

	static final int DEFAULT_SEGMENT_SIZE = 1024;

	// ---------------------------------------------------------------------------------------------
	// NettyServer and NettyClient
	// ---------------------------------------------------------------------------------------------

	static NettyServer initServer(NettyConfig config, NettyProtocol protocol, NettyBufferPool bufferPool) throws Exception {
		final NettyServer server = new NettyServer(config);

		try {
			server.init(protocol, bufferPool);
		}
		catch (Exception e) {
			server.shutdown();
			throw e;
		}

		return server;
	}

	static NettyServer initServer(
			NettyConfig config,
			NettyBufferPool bufferPool,
			Function<SSLHandlerFactory, NettyServer.ServerChannelInitializer> channelInitializer) throws Exception {
		final NettyServer server = new NettyServer(config);

		try {
			server.init(bufferPool, channelInitializer);
		}
		catch (Exception e) {
			server.shutdown();
			throw e;
		}

		return server;
	}

	static NettyClient initClient(NettyConfig config, NettyProtocol protocol, NettyBufferPool bufferPool) throws Exception {
		final NettyClient client = new NettyClient(config);

		try {
			client.init(protocol, bufferPool);
		}
		catch (Exception e) {
			client.shutdown();
			throw e;
		}

		return client;
	}

	static NettyServerAndClient initServerAndClient(NettyProtocol protocol) throws Exception {
		return initServerAndClient(protocol, createConfig());
	}

	static NettyServerAndClient initServerAndClient(NettyProtocol protocol, NettyConfig config)
			throws Exception {

		NettyBufferPool bufferPool = new NettyBufferPool(1);

		final NettyClient client = initClient(config, protocol, bufferPool);
		final NettyServer server = initServer(config, protocol, bufferPool);

		return new NettyServerAndClient(server, client);
	}

	static Channel connect(NettyServerAndClient serverAndClient) throws Exception {
		return connect(serverAndClient.client(), serverAndClient.server());
	}

	static Channel connect(NettyClient client, NettyServer server) throws Exception {
		final NettyConfig config = server.getConfig();

		return client
				.connect(new InetSocketAddress(config.getServerAddress(), config.getServerPort()))
				.sync()
				.channel();
	}

	static void awaitClose(Channel ch) throws InterruptedException {
		// Wait for the channel to be closed
		while (ch.isActive()) {
			ch.closeFuture().await(1, TimeUnit.SECONDS);
		}
	}

	static void shutdown(NettyServerAndClient serverAndClient) {
		if (serverAndClient != null) {
			if (serverAndClient.server() != null) {
				serverAndClient.server().shutdown();
			}

			if (serverAndClient.client() != null) {
				serverAndClient.client().shutdown();
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// NettyConfig
	// ---------------------------------------------------------------------------------------------

	static NettyConfig createConfig() throws Exception {
		return createConfig(DEFAULT_SEGMENT_SIZE, new Configuration());
	}

	static NettyConfig createConfig(int segmentSize) throws Exception {
		return createConfig(segmentSize, new Configuration());
	}

	static NettyConfig createConfig(Configuration config) throws Exception {
		return createConfig(DEFAULT_SEGMENT_SIZE, config);
	}

	static NettyConfig createConfig(int segmentSize, Configuration config) throws Exception {
		checkArgument(segmentSize > 0);
		checkNotNull(config);

		return new NettyConfig(
				InetAddress.getLocalHost(),
				NetUtils.getAvailablePort(),
				segmentSize,
				1,
				config);
	}

	// ---------------------------------------------------------------------------------------------
	// Encoding & Decoding
	// ---------------------------------------------------------------------------------------------

	static <T extends NettyMessage> T encodeAndDecode(T msg, EmbeddedChannel channel) {
		channel.writeOutbound(msg);
		ByteBuf encoded = channel.readOutbound();

		assertTrue(channel.writeInbound(encoded));

		return channel.readInbound();
	}

	// ---------------------------------------------------------------------------------------------
	// Message Verification
	// ---------------------------------------------------------------------------------------------

	static void verifyErrorResponse(ErrorResponse expected, ErrorResponse actual) {
		assertEquals(expected.receiverId, actual.receiverId);
		assertEquals(expected.cause.getClass(), actual.cause.getClass());
		assertEquals(expected.cause.getMessage(), actual.cause.getMessage());

		if (expected.receiverId == null) {
			assertTrue(actual.isFatalError());
		}
	}

	static void verifyBufferResponseHeader(BufferResponse expected, BufferResponse actual) {
		assertEquals(expected.backlog, actual.backlog);
		assertEquals(expected.sequenceNumber, actual.sequenceNumber);
		assertEquals(expected.bufferSize, actual.bufferSize);
		assertEquals(expected.receiverId, actual.receiverId);
	}

	// ------------------------------------------------------------------------

	static final class NettyServerAndClient {

		private final NettyServer server;
		private final NettyClient client;

		NettyServerAndClient(NettyServer server, NettyClient client) {
			this.server = checkNotNull(server);
			this.client = checkNotNull(client);
		}

		NettyServer server() {
			return server;
		}

		NettyClient client() {
			return client;
		}
	}
}
