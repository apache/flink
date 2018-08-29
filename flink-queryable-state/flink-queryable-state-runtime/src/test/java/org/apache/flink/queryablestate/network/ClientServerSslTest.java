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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.net.SSLEngineFactory;
import org.apache.flink.core.net.SSLUtils;
import org.apache.flink.core.net.SSLUtilsTest;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.TestMessage;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tests the SSL connection between {@link Client} and {@link AbstractServerBase}.
 */
public class ClientServerSslTest {

	private static final Logger LOG = LoggerFactory.getLogger(ClientServerSslTest.class);

	private static final List<Integer> PORT_RANGE = Collections.singletonList(7777);

	private static final MessageSerializer<TestMessage, TestMessage> SERIALIZER = new MessageSerializer<>(new TestMessage.TestMessageDeserializer(), new TestMessage.TestMessageDeserializer());

	// Thread pool for client bootstrap (shared between tests)
	private NioEventLoopGroup nioGroup;

	@Before
	public void setUp() throws Exception {
		nioGroup = new NioEventLoopGroup();
	}

	@After
	public void tearDown() throws Exception {
		if (nioGroup != null) {
			// note: no "quiet period" to not trigger Netty#4357
			nioGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
		}
	}

	/**
	 */
	@Test
	public void testValidSslConnection() throws Throwable {
		final Configuration config = SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores();
		SSLEngineFactory serverSslFactory = SSLUtils.createInternalServerSSLEngineFactory(config);
		SSLEngineFactory clientSslFactory = SSLUtils.createInternalClientSSLEngineFactory(config);
		try (
			TestServer server1 = new TestServer(serverSslFactory);
			TestClient client = new TestClient(clientSslFactory)) {
			server1.start();
			TestMessage response1 = client.sendRequest(server1.getServerAddress(), new TestMessage("ping")).join();
			Assert.assertEquals(server1.getServerName() + "-ping", response1.getMessage());
		}
	}

	/**
	 * Verify SSL handshake error when untrusted server or client certificate is used.
	 */
	@Test
	public void testSslHandshakeError() throws Throwable {
		final Configuration config = SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores();
		// Use a server certificate which is not present in the truststore
		SSLUtilsTest.setUntrustedKeyStoreConfig(config);
		SSLEngineFactory serverSslFactory = SSLUtils.createInternalServerSSLEngineFactory(config);
		SSLEngineFactory clientSslFactory = SSLUtils.createInternalClientSSLEngineFactory(config);

		try (
			TestServer server1 = new TestServer(serverSslFactory);
			TestClient client = new TestClient(clientSslFactory)) {
			server1.start();
			try {
				client.sendRequest(server1.getServerAddress(), new TestMessage("ping")).join();
				Assert.fail("expected to fail with SSL error");
			} catch (Throwable th) {
				if (!(ExceptionUtils.stripCompletionException(th) instanceof SSLException)) {
					throw th;
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	private static class TestClient extends Client<TestMessage, TestMessage> implements AutoCloseable {
		TestClient(SSLEngineFactory sslFactory) {
			super("Test Client", 1, SERIALIZER, new DisabledKvStateRequestStats(), sslFactory);
		}

		@Override
		public void close() throws Exception {
			shutdown().join();
		}
	}

	/**
	 * A server that receives a {@link TestMessage test message} and returns another test
	 * message containing the same string as the request with the name of the server prepended.
	 */
	private static class TestServer extends AbstractServerBase<TestMessage, TestMessage> implements AutoCloseable {
		TestServer(SSLEngineFactory sslFactory) throws UnknownHostException {
			super("Test Server", InetAddress.getLocalHost(), PORT_RANGE.iterator(), 1, 1, sslFactory);
		}

		@Override
		public AbstractServerHandler<TestMessage, TestMessage> initializeHandler() {
			return new AbstractServerHandler<TestMessage, TestMessage>(
				this, SERIALIZER, new DisabledKvStateRequestStats()) {
				@Override
				public CompletableFuture<TestMessage> handleRequest(long requestId, TestMessage request) {
					TestMessage response = new TestMessage(getServerName() + '-' + request.getMessage());
					return CompletableFuture.completedFuture(response);
				}

				@Override
				public CompletableFuture<Void> shutdown() {
					return CompletableFuture.completedFuture(null);
				}
			};
		}

		@Override
		public void close() throws Exception {
			shutdownServer().join();
		}
	}
}
