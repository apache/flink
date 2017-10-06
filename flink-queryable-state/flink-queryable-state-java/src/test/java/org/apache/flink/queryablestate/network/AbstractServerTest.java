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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.runtime.query.netty.DisabledKvStateRequestStats;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tests general behavior of the {@link AbstractServerBase}.
 */
public class AbstractServerTest {

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	/**
	 * Tests that in case of port collision, a FlinkRuntimeException is thrown
	 * with a specific message.
	 */
	@Test
	public void testServerInitializationFailure() throws Throwable {

		// the expected exception along with the adequate message
		expectedEx.expect(FlinkRuntimeException.class);
		expectedEx.expectMessage("Unable to start server Test Server 2. All ports in provided range are occupied.");

		TestServer server1 = null;
		TestServer server2 = null;
		try {

			server1 = startServer("Test Server 1", 7777);
			Assert.assertEquals(7777L, server1.getServerAddress().getPort());

			server2 = startServer("Test Server 2", 7777);
		} finally {

			if (server1 != null) {
				server1.shutdown();
			}

			if (server2 != null) {
				server2.shutdown();
			}
		}
	}

	/**
	 * Tests that in case of port collision and big enough port range,
	 * the server will try to bind to the next port in the range.
	 */
	@Test
	public void testPortRangeSuccess() throws Throwable {
		TestServer server1 = null;
		TestServer server2 = null;
		Client<TestMessage, TestMessage> client = null;

		try {
			server1 = startServer("Test Server 1", 7777, 7778, 7779);
			Assert.assertEquals(7777L, server1.getServerAddress().getPort());

			server2 = startServer("Test Server 2", 7777, 7778, 7779);
			Assert.assertEquals(7778L, server2.getServerAddress().getPort());

			client = new Client<>(
					"Test Client",
					1,
					new MessageSerializer<>(new TestMessage.TestMessageDeserializer(), new TestMessage.TestMessageDeserializer()),
					new DisabledKvStateRequestStats());

			TestMessage response1 = client.sendRequest(server1.getServerAddress(), new TestMessage("ping")).join();
			Assert.assertEquals(server1.getServerName() + "-ping", response1.getMessage());

			TestMessage response2 = client.sendRequest(server2.getServerAddress(), new TestMessage("pong")).join();
			Assert.assertEquals(server2.getServerName() + "-pong", response2.getMessage());
		} finally {

			if (server1 != null) {
				server1.shutdown();
			}

			if (server2 != null) {
				server2.shutdown();
			}

			if (client != null) {
				client.shutdown();
			}
		}
	}

	/**
	 * Initializes a {@link TestServer} with the given port range.
	 * @param serverName the name of the server.
	 * @param ports a range of ports.
	 * @return A test server with the given name.
	 */
	private TestServer startServer(String serverName, int... ports) throws Throwable {
		List<Integer> portList = new ArrayList<>(ports.length);
		for (int p : ports) {
			portList.add(p);
		}

		final TestServer server = new TestServer(serverName, portList.iterator());
		server.start();
		return server;
	}

	/**
	 * A server that receives a {@link TestMessage test message} and returns another test
	 * message containing the same string as the request with the name of the server prepended.
	 */
	private class TestServer extends AbstractServerBase<TestMessage, TestMessage> {

		protected TestServer(String name, Iterator<Integer> bindPort) throws UnknownHostException {
			super(name, InetAddress.getLocalHost(), bindPort, 1, 1);
		}

		@Override
		public AbstractServerHandler<TestMessage, TestMessage> initializeHandler() {
			return new AbstractServerHandler<TestMessage, TestMessage>(
					this,
					new MessageSerializer<>(new TestMessage.TestMessageDeserializer(), new TestMessage.TestMessageDeserializer()),
					new DisabledKvStateRequestStats()) {

				@Override
				public CompletableFuture<TestMessage> handleRequest(long requestId, TestMessage request) {
					TestMessage response = new TestMessage(getServerName() + '-' + request.getMessage());
					return CompletableFuture.completedFuture(response);
				}

				@Override
				public void shutdown() {
					// do nothing
				}
			};
		}
	}

	/**
	 * Message with a string as payload.
	 */
	private static class TestMessage extends MessageBody {

		private final String message;

		TestMessage(String message) {
			this.message = Preconditions.checkNotNull(message);
		}

		public String getMessage() {
			return message;
		}

		@Override
		public byte[] serialize() {
			byte[] content = message.getBytes(ConfigConstants.DEFAULT_CHARSET);

			// message size + 4 for the length itself
			return ByteBuffer.allocate(content.length + Integer.BYTES)
					.putInt(content.length)
					.put(content)
					.array();
		}

		/**
		 * The deserializer for our {@link TestMessage test messages}.
		 */
		public static class TestMessageDeserializer implements MessageDeserializer<TestMessage> {

			@Override
			public TestMessage deserializeMessage(ByteBuf buf) {
				int length = buf.readInt();
				String message = "";
				if (length > 0) {
					byte[] name = new byte[length];
					buf.readBytes(name);
					message = new String(name, ConfigConstants.DEFAULT_CHARSET);
				}
				return new TestMessage(message);
			}
		}
	}
}
