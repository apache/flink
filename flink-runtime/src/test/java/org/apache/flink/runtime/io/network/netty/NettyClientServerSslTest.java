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

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.SSLProtocolHandler;
import org.apache.flink.util.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NettyClientServerSslTest {

	private static final byte[] TEST_PAYLOAD = "HELLO".getBytes();

	/**
	 * Verify valid ssl configuration and connection
	 *
	 */
	@Test
	public void testValidSslConnection() throws Exception {
		NettyConfig nettyConfig = new PartitionRequestNettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			createSslConfig());

		TestProtocol protocol = createProtocol(nettyConfig);

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Promise<ByteBuf> responsePromise = serverAndClient.client().newPromise();
		Channel ch = NettyTestUtil.connect(serverAndClient, protocol.newClientChannelHandler(responsePromise));

		// verify that the SSL handshake was successful
		assertTrue(SSLProtocolHandler.getSSLHandler(ch).handshakeFuture().sync().isSuccess());

		// verify that data may be sent over the channel
		ByteBuf expected = ch.alloc().buffer().writeBytes(TEST_PAYLOAD);
		ReferenceCountUtil.retain(expected);
		ch.writeAndFlush(expected).sync();
		expected.resetReaderIndex();
		ByteBuf actual = responsePromise.sync().getNow();
		assertTrue(expected.equals(actual));

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify failure on invalid ssl configuration
	 *
	 */
	@Test
	public void testInvalidSslConfiguration() throws Exception {

		Configuration config = createSslConfig();
		// Modify the keystore password to an incorrect one
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "invalidpassword");

		NettyConfig nettyConfig = new PartitionRequestNettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

		try {
			new SSLProtocolHandler(nettyConfig, false);
			Assert.fail("Created server and client from invalid configuration");
		} catch (Exception e) {
			// Exception should be thrown as expected
		}
	}

	/**
	 * Verify SSL handshake error when untrusted server certificate is used
	 *
	 */
	@Test
	public void testSslHandshakeError() throws Exception {

		Configuration config = createSslConfig();

		// Use a server certificate which is not present in the truststore
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/untrusted.keystore");

		NettyConfig nettyConfig = new PartitionRequestNettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

		TestProtocol protocol = createProtocol(nettyConfig);

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);
		Promise<ByteBuf> responsePromise = serverAndClient.client().newPromise();
		Channel ch = NettyTestUtil.connect(serverAndClient, protocol.newClientChannelHandler(responsePromise));

		Future<Channel> handshakeFuture = SSLProtocolHandler.getSSLHandler(ch).handshakeFuture().await();
		assertFalse(handshakeFuture.isSuccess());
		assertTrue(handshakeFuture.cause() instanceof javax.net.ssl.SSLHandshakeException);

		NettyTestUtil.shutdown(serverAndClient);
	}

	private Configuration createSslConfig() throws Exception {

		Configuration flinkConfig = new Configuration();
		flinkConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
		return flinkConfig;
	}

	private TestProtocol createProtocol(final NettyConfig nettyConfig) throws Exception {
		return new TestProtocol(nettyConfig);
	}

	static class TestProtocol implements NettyProtocol {
		private final NettyConfig nettyConfig;
		public TestProtocol(NettyConfig nettyConfig) {
			this.nettyConfig = nettyConfig;
		}

		@Override
		public ChannelHandler getServerChannelHandler() {
			return new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel channel) throws Exception {
					channel.pipeline()
						.addLast(new SSLProtocolHandler(nettyConfig, false))
						.addLast(new ServerHandler());
				}
			};
		}

		class ServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
				// echo the message
				ReferenceCountUtil.retain(msg);
				ctx.writeAndFlush(msg).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
			}
		}

		@Override
		public ChannelHandler getClientChannelHandler() {
			return null;
		}

		public ChannelHandler newClientChannelHandler(final Promise<ByteBuf> promise) {
			return new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel channel) throws Exception {
					channel.pipeline()
						.addLast(new SSLProtocolHandler(nettyConfig, true))
						.addLast(new ClientHandler(promise));
				}
			};
		}

		class ClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
			private final Promise<ByteBuf> promise;
			private ClientHandler(Promise<ByteBuf> promise) {
				this.promise = promise;
			}

			@Override
			protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
				ReferenceCountUtil.retain(msg);
				promise.setSuccess(msg);
				ctx.close();
			}
		}
	}
}
