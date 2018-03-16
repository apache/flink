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
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringEncoder;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NettyClientServerSslTest {

	/**
	 * Verify valid ssl configuration and connection.
	 */
	@Test
	public void testValidSslConnection() throws Exception {
		NettyProtocol protocol = new NettyProtocol(null, null, true) {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			createSslConfig());

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Channel ch = NettyTestUtil.connect(serverAndClient);

		// should be able to send text data
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		assertTrue(ch.writeAndFlush("test").await().isSuccess());

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify failure on invalid ssl configuration.
	 */
	@Test
	public void testInvalidSslConfiguration() throws Exception {
		NettyProtocol protocol = new NettyProtocol(null, null, true) {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		Configuration config = createSslConfig();
		// Modify the keystore password to an incorrect one
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "invalidpassword");

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

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
		NettyProtocol protocol = new NettyProtocol(null, null, true) {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		Configuration config = createSslConfig();

		// Use a server certificate which is not present in the truststore
		config.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/untrusted.keystore");

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Channel ch = NettyTestUtil.connect(serverAndClient);
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());

		// Attempting to write data over ssl should fail
		assertFalse(ch.writeAndFlush("test").await().isSuccess());

		NettyTestUtil.shutdown(serverAndClient);
	}

	private Configuration createSslConfig() throws Exception {

		Configuration flinkConfig = new Configuration();
		flinkConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		flinkConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		flinkConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		flinkConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		flinkConfig.setString(SecurityOptions.SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		flinkConfig.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");
		return flinkConfig;
	}
}
