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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link RpcGatewayRetriever}.
 */
public class RpcGatewayRetrieverTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);
	private static TestingRpcService rpcService;

	@BeforeClass
	public static void setup() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardown() throws InterruptedException, ExecutionException, TimeoutException {
		if (rpcService != null) {
			rpcService.stopService();
			rpcService.getTerminationFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			rpcService = null;
		}
	}

	/**
	 * Tests that the RpcGatewayRetriever can retrieve the specified gateway type from a leader retrieval service.
	 */
	@Test
	public void testRpcGatewayRetrieval() throws Exception {
		final String expectedValue = "foobar";
		final String expectedValue2 = "barfoo";
		final UUID leaderSessionId = UUID.randomUUID();

		RpcGatewayRetriever<DummyGateway> gatewayRetriever = new RpcGatewayRetriever<>(rpcService, DummyGateway.class, 0, Time.milliseconds(0L));
		TestingLeaderRetrievalService testingLeaderRetrievalService = new TestingLeaderRetrievalService();
		DummyRpcEndpoint dummyRpcEndpoint = new DummyRpcEndpoint(rpcService, "dummyRpcEndpoint1", expectedValue);
		DummyRpcEndpoint dummyRpcEndpoint2 = new DummyRpcEndpoint(rpcService, "dummyRpcEndpoint2", expectedValue2);
		rpcService.registerGateway(dummyRpcEndpoint.getAddress(), dummyRpcEndpoint.getSelfGateway(DummyGateway.class));
		rpcService.registerGateway(dummyRpcEndpoint2.getAddress(), dummyRpcEndpoint2.getSelfGateway(DummyGateway.class));

		try {
			dummyRpcEndpoint.start();
			dummyRpcEndpoint2.start();

			testingLeaderRetrievalService.start(gatewayRetriever);

			final CompletableFuture<DummyGateway> gatewayFuture = gatewayRetriever.getFuture();

			assertFalse(gatewayFuture.isDone());

			testingLeaderRetrievalService.notifyListener(dummyRpcEndpoint.getAddress(), leaderSessionId);

			final DummyGateway dummyGateway = gatewayFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertEquals(dummyRpcEndpoint.getAddress(), dummyGateway.getAddress());
			assertEquals(expectedValue, dummyGateway.foobar(TIMEOUT).get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS));

			// elect a new leader
			testingLeaderRetrievalService.notifyListener(dummyRpcEndpoint2.getAddress(), leaderSessionId);

			final CompletableFuture<DummyGateway> gatewayFuture2 = gatewayRetriever.getFuture();
			final DummyGateway dummyGateway2 = gatewayFuture2.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertEquals(dummyRpcEndpoint2.getAddress(), dummyGateway2.getAddress());
			assertEquals(expectedValue2, dummyGateway2.foobar(TIMEOUT).get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS));
		} finally {
			dummyRpcEndpoint.shutDown();
			dummyRpcEndpoint2.shutDown();
			dummyRpcEndpoint.getTerminationFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
			dummyRpcEndpoint2.getTerminationFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Testing RpcGateway.
	 */
	public interface DummyGateway extends RpcGateway {
		CompletableFuture<String> foobar(@RpcTimeout Time timeout);
	}

	static class DummyRpcEndpoint extends RpcEndpoint implements DummyGateway {

		private final String value;

		protected DummyRpcEndpoint(RpcService rpcService, String endpointId, String value) {
			super(rpcService, endpointId);
			this.value = value;
		}

		@Override
		public CompletableFuture<String> foobar(Time timeout) {
			return CompletableFuture.completedFuture(value);
		}
	}
}
