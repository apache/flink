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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.io.network.netty.NettyTestUtil.NettyServerAndClient;
import org.apache.flink.runtime.net.SSLUtilsTest;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLSessionContext;

import java.net.InetAddress;
import java.util.List;

import static org.apache.flink.configuration.SecurityOptions.SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT;
import static org.apache.flink.configuration.SecurityOptions.SSL_INTERNAL_HANDSHAKE_TIMEOUT;
import static org.apache.flink.configuration.SecurityOptions.SSL_INTERNAL_SESSION_CACHE_SIZE;
import static org.apache.flink.configuration.SecurityOptions.SSL_INTERNAL_SESSION_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the SSL connection between Netty Server and Client used for the
 * data plane.
 */
@RunWith(Parameterized.class)
public class NettyClientServerSslTest extends TestLogger {

	@Parameterized.Parameter
	public String sslProvider;

	@Parameterized.Parameters(name = "SSL provider = {0}")
	public static List<String> parameters() {
		return SSLUtilsTest.AVAILABLE_SSL_PROVIDERS;
	}

	/**
	 * Verify valid ssl configuration and connection.
	 */
	@Test
	public void testValidSslConnection() throws Exception {
		testValidSslConnection(createSslConfig());
	}

	/**
	 * Verify valid (advanced) ssl configuration and connection.
	 */
	@Test
	public void testValidSslConnectionAdvanced() throws Exception {
		Configuration sslConfig = createSslConfig();
		sslConfig.setInteger(SSL_INTERNAL_SESSION_CACHE_SIZE, 1);
		sslConfig.setInteger(SSL_INTERNAL_SESSION_TIMEOUT, 1_000);
		sslConfig.setInteger(SSL_INTERNAL_HANDSHAKE_TIMEOUT, 1_000);
		sslConfig.setInteger(SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT, 1_000);

		testValidSslConnection(sslConfig);
	}

	private void testValidSslConnection(Configuration sslConfig) throws Exception {
		OneShotLatch serverChannelInitComplete = new OneShotLatch();
		final SslHandler[] serverSslHandler = new SslHandler[1];

		NettyProtocol protocol = new NoOpProtocol();

		NettyConfig nettyConfig = createNettyConfig(sslConfig);

		final NettyBufferPool bufferPool = new NettyBufferPool(1);
		final NettyServer server = NettyTestUtil.initServer(
			nettyConfig,
			bufferPool,
			sslHandlerFactory ->
				new TestingServerChannelInitializer(
					protocol,
					sslHandlerFactory,
					serverChannelInitComplete,
					serverSslHandler));
		final NettyClient client = NettyTestUtil.initClient(nettyConfig, protocol, bufferPool);
		final NettyServerAndClient serverAndClient = new NettyServerAndClient(server, client);

		Channel ch = NettyTestUtil.connect(serverAndClient);

		SslHandler clientSslHandler = (SslHandler) ch.pipeline().get("ssl");
		assertEqualsOrDefault(sslConfig, SSL_INTERNAL_HANDSHAKE_TIMEOUT, clientSslHandler.getHandshakeTimeoutMillis());
		assertEqualsOrDefault(sslConfig, SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT, clientSslHandler.getCloseNotifyFlushTimeoutMillis());

		// should be able to send text data
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		ch.writeAndFlush("test").sync();

		// session context is only be available after a session was setup -> this should be true after data was sent
		serverChannelInitComplete.await();
		assertNotNull(serverSslHandler[0]);

		// verify server parameters
		assertEqualsOrDefault(sslConfig, SSL_INTERNAL_HANDSHAKE_TIMEOUT, serverSslHandler[0].getHandshakeTimeoutMillis());
		assertEqualsOrDefault(sslConfig, SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT, serverSslHandler[0].getCloseNotifyFlushTimeoutMillis());
		SSLSessionContext sessionContext = serverSslHandler[0].engine().getSession().getSessionContext();
		assertNotNull("bug in unit test setup: session context not available", sessionContext);
		// note: can't verify session cache setting at the client - delegate to server instead (with our own channel initializer)
		assertEqualsOrDefault(sslConfig, SSL_INTERNAL_SESSION_CACHE_SIZE, sessionContext.getSessionCacheSize());
		int sessionTimeout = sslConfig.getInteger(SSL_INTERNAL_SESSION_TIMEOUT);
		if (sessionTimeout != -1) {
			// session timeout config is in milliseconds but the context returns it in seconds
			assertEquals(sessionTimeout / 1000, sessionContext.getSessionTimeout());
		} else {
			assertTrue("default value (-1) should not be propagated", sessionContext.getSessionTimeout() >= 0);
		}

		NettyTestUtil.shutdown(serverAndClient);
	}

	private static void assertEqualsOrDefault(Configuration sslConfig, ConfigOption<Integer> option, long actual) {
		long expected = sslConfig.getInteger(option);
		if (expected != option.defaultValue()) {
			assertEquals(expected, actual);
		} else {
			assertTrue("default value (" + option.defaultValue() + ") should not be propagated",
				actual >= 0);
		}
	}

	/**
	 * Verify failure on invalid ssl configuration.
	 */
	@Test
	public void testInvalidSslConfiguration() throws Exception {
		NettyProtocol protocol = new NoOpProtocol();

		Configuration config = createSslConfig();
		// Modify the keystore password to an incorrect one
		config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "invalidpassword");

		NettyConfig nettyConfig = createNettyConfig(config);

		NettyTestUtil.NettyServerAndClient serverAndClient = null;
		try {
			serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);
			Assert.fail("Created server and client from invalid configuration");
		} catch (Exception e) {
			// Exception should be thrown as expected
		}

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify SSL handshake error when untrusted server certificate is used.
	 */
	@Test
	public void testSslHandshakeError() throws Exception {
		NettyProtocol protocol = new NoOpProtocol();

		Configuration config = createSslConfig();

		// Use a server certificate which is not present in the truststore
		config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE, "src/test/resources/untrusted.keystore");

		NettyConfig nettyConfig = createNettyConfig(config);

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Channel ch = NettyTestUtil.connect(serverAndClient);
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());

		// Attempting to write data over ssl should fail
		assertFalse(ch.writeAndFlush("test").await().isSuccess());

		NettyTestUtil.shutdown(serverAndClient);
	}

	@Test
	public void testClientUntrustedCertificate() throws Exception {
		final Configuration serverConfig = createSslConfig();
		final Configuration clientConfig = createSslConfig();

		// give the client a different keystore / certificate
		clientConfig.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE, "src/test/resources/untrusted.keystore");

		final NettyConfig nettyServerConfig = createNettyConfig(serverConfig);
		final NettyConfig nettyClientConfig = createNettyConfig(clientConfig);

		final NettyBufferPool bufferPool = new NettyBufferPool(1);
		final NettyProtocol protocol = new NoOpProtocol();

		final NettyServer server = NettyTestUtil.initServer(nettyServerConfig, protocol, bufferPool);
		final NettyClient client = NettyTestUtil.initClient(nettyClientConfig, protocol, bufferPool);
		final NettyServerAndClient serverAndClient = new NettyServerAndClient(server, client);

		final Channel ch = NettyTestUtil.connect(serverAndClient);
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());

		// Attempting to write data over ssl should fail
		assertFalse(ch.writeAndFlush("test").await().isSuccess());

		NettyTestUtil.shutdown(serverAndClient);
	}

	private Configuration createSslConfig() {
		return SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores(sslProvider);
	}

	private static NettyConfig createNettyConfig(Configuration config) {
		return new NettyConfig(
				InetAddress.getLoopbackAddress(),
				NetUtils.getAvailablePort(),
				NettyTestUtil.DEFAULT_SEGMENT_SIZE,
				1,
				config);
	}

	private static final class NoOpProtocol extends NettyProtocol {

		NoOpProtocol() {
			super(null, null, true);
		}

		@Override
		public ChannelHandler[] getServerChannelHandlers() {
			return new ChannelHandler[0];
		}

		@Override
		public ChannelHandler[] getClientChannelHandlers() {
			return new ChannelHandler[0];
		}
	}

	/**
	 * Wrapper around {@link NettyServer.ServerChannelInitializer} making the server's SSL handler
	 * available for the tests.
	 */
	private static class TestingServerChannelInitializer extends NettyServer.ServerChannelInitializer {
		private final OneShotLatch latch;
		private final SslHandler[] serverHandler;

		TestingServerChannelInitializer(
			NettyProtocol protocol,
			SSLHandlerFactory sslHandlerFactory,
			OneShotLatch latch,
			SslHandler[] serverHandler) {
			super(protocol, sslHandlerFactory);
			this.latch = latch;
			this.serverHandler = serverHandler;
		}

		@Override
		public void initChannel(SocketChannel channel) throws Exception {
			super.initChannel(channel);

			SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
			assertNotNull(sslHandler);
			serverHandler[0] = sslHandler;

			latch.trigger();
		}
	}
}
