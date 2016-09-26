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

package org.apache.flink.runtime.registration;

import org.apache.flink.runtime.registration.RetryingRegistrationTest.TestRegistrationSuccess;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for RegisteredRpcConnection, validating the successful, failure and close behavior.
 */
public class RegisteredRpcConnectionTest extends TestLogger {

	@Test
	public void testSuccessfulRpcConnection() throws Exception {
		final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
		final UUID leaderId = UUID.randomUUID();
		final String connectionID = "Test RPC Connection ID";

		// an endpoint that immediately returns success
		TestRegistrationGateway testGateway = new TestRegistrationGateway(new RetryingRegistrationTest.TestRegistrationSuccess(connectionID));
		TestingRpcService rpcService = new TestingRpcService();

		try {
			rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

			TestRpcConnection connection = new TestRpcConnection(testRpcConnectionEndpointAddress, leaderId, rpcService.getExecutor(), rpcService);
			connection.start();

			//wait for connection established
			Thread.sleep(RetryingRegistrationTest.TestRetryingRegistration.MAX_TIMEOUT);

			// validate correct invocation and result
			assertTrue(connection.isConnected());
			assertEquals(testRpcConnectionEndpointAddress, connection.getTargetAddress());
			assertEquals(leaderId, connection.getTargetLeaderId());
			assertEquals(testGateway, connection.getTargetGateway());
			assertEquals(connectionID, connection.getConnectionId());
		}
		finally {
			testGateway.stop();
			rpcService.stopService();
		}
	}

	@Test
	public void testRpcConnectionFailures() throws Exception {
		final String connectionFailureMessage = "Test RPC Connection failure";
		final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
		final UUID leaderId = UUID.randomUUID();

		TestingRpcService rpcService = new TestingRpcService();

		try {
			// gateway that upon calls Throw an exception
			TestRegistrationGateway testGateway = mock(TestRegistrationGateway.class);
			when(testGateway.registrationCall(any(UUID.class), anyLong())).thenThrow(
				new RuntimeException(connectionFailureMessage));

			rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

			TestRpcConnection connection = new TestRpcConnection(testRpcConnectionEndpointAddress, leaderId, rpcService.getExecutor(), rpcService);
			connection.start();

			//wait for connection failure
			Thread.sleep(RetryingRegistrationTest.TestRetryingRegistration.MAX_TIMEOUT);

			// validate correct invocation and result
			assertFalse(connection.isConnected());
			assertEquals(testRpcConnectionEndpointAddress, connection.getTargetAddress());
			assertEquals(leaderId, connection.getTargetLeaderId());
			assertNull(connection.getTargetGateway());
			assertEquals(connectionFailureMessage, connection.getFailareMessage());
		}
		finally {
			rpcService.stopService();
		}
	}

	@Test
	public void testRpcConnectionClose() throws Exception {
		final String testRpcConnectionEndpointAddress = "<TestRpcConnectionEndpointAddress>";
		final UUID leaderId = UUID.randomUUID();
		final String connectionID = "Test RPC Connection ID";

		TestRegistrationGateway testGateway = new TestRegistrationGateway(new RetryingRegistrationTest.TestRegistrationSuccess(connectionID));
		TestingRpcService rpcService = new TestingRpcService();

		try{
			rpcService.registerGateway(testRpcConnectionEndpointAddress, testGateway);

			TestRpcConnection connection = new TestRpcConnection(testRpcConnectionEndpointAddress, leaderId, rpcService.getExecutor(), rpcService);
			connection.start();
			//close the connection
			connection.close();

			// validate connection is closed
			assertEquals(testRpcConnectionEndpointAddress, connection.getTargetAddress());
			assertEquals(leaderId, connection.getTargetLeaderId());
			assertTrue(connection.isClosed());
		}
		finally {
			testGateway.stop();
			rpcService.stopService();
		}
	}

	// ------------------------------------------------------------------------
	//  test RegisteredRpcConnection
	// ------------------------------------------------------------------------

	private static class TestRpcConnection extends RegisteredRpcConnection<TestRegistrationGateway, TestRegistrationSuccess> {

		private final RpcService rpcService;

		private String connectionId;

		private String failureMessage;

		public TestRpcConnection(String targetAddress,
								 UUID targetLeaderId,
								 Executor executor,
								 RpcService rpcService)
		{
			super(LoggerFactory.getLogger(RegisteredRpcConnectionTest.class), targetAddress, targetLeaderId, executor);
			this.rpcService = rpcService;
		}

		@Override
		protected RetryingRegistration<TestRegistrationGateway, RetryingRegistrationTest.TestRegistrationSuccess> generateRegistration() {
			return new RetryingRegistrationTest.TestRetryingRegistration(rpcService, getTargetAddress(), getTargetLeaderId());
		}

		@Override
		protected void onRegistrationSuccess(RetryingRegistrationTest.TestRegistrationSuccess success) {
			connectionId = success.getCorrelationId();
		}

		@Override
		protected void onRegistrationFailure(Throwable failure) {
			failureMessage = failure.getMessage();
		}

		public String getConnectionId() {
			return connectionId;
		}

		public String getFailareMessage() {
			return failureMessage;
		}
	}
}
