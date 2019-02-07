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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the over sized response message handling of the {@link AkkaRpcActor}.
 */
public class AkkaRpcActorOversizedResponseMessageTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final int FRAMESIZE = 32000;

	private static final String OVERSIZED_PAYLOAD = new String(new byte[FRAMESIZE]);

	private static final String PAYLOAD = "Hello";

	private static RpcService rpcService1;

	private static RpcService rpcService2;

	@BeforeClass
	public static void setupClass() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setString(AkkaOptions.FRAMESIZE, FRAMESIZE + " b");

		rpcService1 = AkkaRpcServiceUtils.createRpcService("localhost", 0, configuration);
		rpcService2 = AkkaRpcServiceUtils.createRpcService("localhost", 0, configuration);
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		RpcUtils.terminateRpcServices(TIMEOUT, rpcService1, rpcService2);
	}

	@Test
	public void testOverSizedResponseMsgAsync() throws Exception {
		try {
			runRemoteMessageResponseTest(OVERSIZED_PAYLOAD, this::requestMessageAsync);

			fail("Expected the RPC to fail.");
		} catch (ExecutionException e) {
			assertThat(ExceptionUtils.findThrowable(e, AkkaRpcException.class).isPresent(), is(true));
		}
	}

	@Test
	public void testNormalSizedResponseMsgAsync() throws Exception {
		final String message = runRemoteMessageResponseTest(PAYLOAD, this::requestMessageAsync);
		assertThat(message, is(equalTo(PAYLOAD)));
	}

	@Test
	public void testNormalSizedResponseMsgSync() throws Exception {
		final String message = runRemoteMessageResponseTest(PAYLOAD, MessageRpcGateway::messageSync);
		assertThat(message, is(equalTo(PAYLOAD)));
	}

	@Test
	public void testOverSizedResponseMsgSync() throws Exception {
		try {
			runRemoteMessageResponseTest(OVERSIZED_PAYLOAD, MessageRpcGateway::messageSync);

			fail("Expected the RPC to fail.");
		} catch (RpcException e) {
			assertThat(ExceptionUtils.findThrowable(e, AkkaRpcException.class).isPresent(), is(true));
		}
	}

	/**
	 * Tests that we can send arbitrarily large objects when communicating locally with
	 * the rpc endpoint.
	 */
	@Test
	public void testLocalOverSizedResponseMsgSync() throws Exception {
		final String message = runLocalMessageResponseTest(OVERSIZED_PAYLOAD, MessageRpcGateway::messageSync);
		assertThat(message, is(equalTo(OVERSIZED_PAYLOAD)));
	}

	/**
	 * Tests that we can send arbitrarily large objects when communicating locally with
	 * the rpc endpoint.
	 */
	@Test
	public void testLocalOverSizedResponseMsgAsync() throws Exception {
		final String message = runLocalMessageResponseTest(OVERSIZED_PAYLOAD, this::requestMessageAsync);
		assertThat(message, is(equalTo(OVERSIZED_PAYLOAD)));
	}

	private String requestMessageAsync(MessageRpcGateway messageRpcGateway) throws Exception {
		CompletableFuture<String> messageFuture = messageRpcGateway.messageAsync();
		return messageFuture.get();
	}

	private <T> T runRemoteMessageResponseTest(String payload, FunctionWithException<MessageRpcGateway, T, Exception> rpcCall) throws Exception {
		final MessageRpcEndpoint rpcEndpoint = new MessageRpcEndpoint(rpcService1, payload);

		try {
			rpcEndpoint.start();

			MessageRpcGateway rpcGateway = rpcService2.connect(rpcEndpoint.getAddress(), MessageRpcGateway.class).get();

			return rpcCall.apply(rpcGateway);
		} finally {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, TIMEOUT);
		}
	}

	private <T> T runLocalMessageResponseTest(String payload, FunctionWithException<MessageRpcGateway, T, Exception> rpcCall) throws Exception {
		final MessageRpcEndpoint rpcEndpoint = new MessageRpcEndpoint(rpcService1, payload);

		try {
			rpcEndpoint.start();

			MessageRpcGateway rpcGateway = rpcService1.connect(rpcEndpoint.getAddress(), MessageRpcGateway.class).get();

			return rpcCall.apply(rpcGateway);
		} finally {
			RpcUtils.terminateRpcEndpoint(rpcEndpoint, TIMEOUT);
		}
	}

	// -------------------------------------------------------------------------

	interface MessageRpcGateway extends RpcGateway {
		CompletableFuture<String> messageAsync();

		String messageSync() throws RpcException;
	}

	static class MessageRpcEndpoint extends RpcEndpoint implements MessageRpcGateway {

		@Nonnull
		private final String message;

		MessageRpcEndpoint(RpcService rpcService, @Nonnull String message) {
			super(rpcService);
			this.message = message;
		}

		@Override
		public CompletableFuture<String> messageAsync() {
			return CompletableFuture.completedFuture(message);
		}

		@Override
		public String messageSync() throws RpcException {
			return message;
		}
	}
}
