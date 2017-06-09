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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the generic retrying registration class, validating the failure, retry, and back-off behavior.
 */
public class RetryingRegistrationTest extends TestLogger {

	@Test
	public void testSimpleSuccessfulRegistration() throws Exception {
		final String testId = "laissez les bon temps roulez";
		final String testEndpointAddress = "<test-address>";
		final UUID leaderId = UUID.randomUUID();

		// an endpoint that immediately returns success
		TestRegistrationGateway testGateway = new TestRegistrationGateway(new TestRegistrationSuccess(testId));
		TestingRpcService rpc = new TestingRpcService();

		try {
			rpc.registerGateway(testEndpointAddress, testGateway);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, testEndpointAddress, leaderId);
			registration.startRegistration();

			Future<Tuple2<TestRegistrationGateway, TestRegistrationSuccess>> future = registration.getFuture();
			assertNotNull(future);

			// multiple accesses return the same future
			assertEquals(future, registration.getFuture());

			Tuple2<TestRegistrationGateway, TestRegistrationSuccess> success =
					future.get(10L, TimeUnit.SECONDS);

			// validate correct invocation and result
			assertEquals(testId, success.f1.getCorrelationId());
			assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
		}
		finally {
			testGateway.stop();
			rpc.stopService();
		}
	}

	@Test
	public void testPropagateFailures() throws Exception {
		final String testExceptionMessage = "testExceptionMessage";

		// RPC service that fails with exception upon the connection
		RpcService rpc = mock(RpcService.class);
		when(rpc.connect(anyString(), any(Class.class))).thenThrow(new RuntimeException(testExceptionMessage));

		TestRetryingRegistration registration = new TestRetryingRegistration(rpc, "testaddress", UUID.randomUUID());
		registration.startRegistration();

		Future<?> future = registration.getFuture();
		assertTrue(future.isDone());

		try {
			future.get();

			fail("We expected an ExecutionException.");
		} catch (ExecutionException e) {
			assertEquals(testExceptionMessage, e.getCause().getMessage());
		}
	}

	@Test
	public void testRetryConnectOnFailure() throws Exception {
		final String testId = "laissez les bon temps roulez";
		final UUID leaderId = UUID.randomUUID();

		ExecutorService executor = Executors.newCachedThreadPool();
		TestRegistrationGateway testGateway = new TestRegistrationGateway(new TestRegistrationSuccess(testId));

		try {
			// RPC service that fails upon the first connection, but succeeds on the second
			RpcService rpc = mock(RpcService.class);
			when(rpc.connect(anyString(), any(Class.class))).thenReturn(
					FlinkCompletableFuture.completedExceptionally(new Exception("test connect failure")),  // first connection attempt fails
					FlinkCompletableFuture.completed(testGateway)                         // second connection attempt succeeds
			);
			when(rpc.getExecutor()).thenReturn(executor);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, "foobar address", leaderId);
			registration.startRegistration();

			Tuple2<TestRegistrationGateway, TestRegistrationSuccess> success =
				registration.getFuture().get(10L, TimeUnit.SECONDS);

			// validate correct invocation and result
			assertEquals(testId, success.f1.getCorrelationId());
			assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
		}
		finally {
			testGateway.stop();
			executor.shutdown();
		}
	}

	@Test
	public void testRetriesOnTimeouts() throws Exception {
		final String testId = "rien ne va plus";
		final String testEndpointAddress = "<test-address>";
		final UUID leaderId = UUID.randomUUID();

		// an endpoint that immediately returns futures with timeouts before returning a successful future
		TestRegistrationGateway testGateway = new TestRegistrationGateway(
				null, // timeout
				null, // timeout
				new TestRegistrationSuccess(testId) // success
		);

		TestingRpcService rpc = new TestingRpcService();

		try {
			rpc.registerGateway(testEndpointAddress, testGateway);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, testEndpointAddress, leaderId);

			long started = System.nanoTime();
			registration.startRegistration();

			Future<Tuple2<TestRegistrationGateway, TestRegistrationSuccess>> future = registration.getFuture();
			Tuple2<TestRegistrationGateway, TestRegistrationSuccess> success =
					future.get(10L, TimeUnit.SECONDS);

			long finished = System.nanoTime();
			long elapsedMillis = (finished - started) / 1000000;

			// validate correct invocation and result
			assertEquals(testId, success.f1.getCorrelationId());
			assertEquals(leaderId, testGateway.getInvocations().take().leaderId());

			// validate that some retry-delay / back-off behavior happened
			assertTrue("retries did not properly back off", elapsedMillis >= 3 * TestRetryingRegistration.INITIAL_TIMEOUT);
		}
		finally {
			rpc.stopService();
			testGateway.stop();
		}
	}

	@Test
	public void testDecline() throws Exception {
		final String testId = "qui a coupe le fromage";
		final String testEndpointAddress = "<test-address>";
		final UUID leaderId = UUID.randomUUID();

		TestingRpcService rpc = new TestingRpcService();

		TestRegistrationGateway testGateway = new TestRegistrationGateway(
				null, // timeout
				new RegistrationResponse.Decline("no reason "),
				null, // timeout
				new TestRegistrationSuccess(testId) // success
		);

		try {
			rpc.registerGateway(testEndpointAddress, testGateway);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, testEndpointAddress, leaderId);

			long started = System.nanoTime();
			registration.startRegistration();

			Future<Tuple2<TestRegistrationGateway, TestRegistrationSuccess>> future = registration.getFuture();
			Tuple2<TestRegistrationGateway, TestRegistrationSuccess> success =
					future.get(10L, TimeUnit.SECONDS);

			long finished = System.nanoTime();
			long elapsedMillis = (finished - started) / 1000000;

			// validate correct invocation and result
			assertEquals(testId, success.f1.getCorrelationId());
			assertEquals(leaderId, testGateway.getInvocations().take().leaderId());

			// validate that some retry-delay / back-off behavior happened
			assertTrue("retries did not properly back off", elapsedMillis >=
					2 * TestRetryingRegistration.INITIAL_TIMEOUT + TestRetryingRegistration.DELAY_ON_DECLINE);
		}
		finally {
			testGateway.stop();
			rpc.stopService();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testRetryOnError() throws Exception {
		final String testId = "Petit a petit, l'oiseau fait son nid";
		final String testEndpointAddress = "<test-address>";
		final UUID leaderId = UUID.randomUUID();

		TestingRpcService rpc = new TestingRpcService();

		try {
			// gateway that upon calls first responds with a failure, then with a success
			TestRegistrationGateway testGateway = mock(TestRegistrationGateway.class);

			when(testGateway.registrationCall(any(UUID.class), anyLong())).thenReturn(
					FlinkCompletableFuture.<RegistrationResponse>completedExceptionally(new Exception("test exception")),
					FlinkCompletableFuture.<RegistrationResponse>completed(new TestRegistrationSuccess(testId)));

			rpc.registerGateway(testEndpointAddress, testGateway);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, testEndpointAddress, leaderId);

			long started = System.nanoTime();
			registration.startRegistration();

			Future<Tuple2<TestRegistrationGateway, TestRegistrationSuccess>> future = registration.getFuture();
			Tuple2<TestRegistrationGateway, TestRegistrationSuccess> success =
					future.get(10, TimeUnit.SECONDS);

			long finished = System.nanoTime();
			long elapsedMillis = (finished - started) / 1000000;

			assertEquals(testId, success.f1.getCorrelationId());

			// validate that some retry-delay / back-off behavior happened
			assertTrue("retries did not properly back off",
					elapsedMillis >= TestRetryingRegistration.DELAY_ON_ERROR);
		}
		finally {
			rpc.stopService();
		}
	}

	@Test
	public void testCancellation() throws Exception {
		final String testEndpointAddress = "my-test-address";
		final UUID leaderId = UUID.randomUUID();

		TestingRpcService rpc = new TestingRpcService();

		try {
			FlinkCompletableFuture<RegistrationResponse> result = new FlinkCompletableFuture<>();

			TestRegistrationGateway testGateway = mock(TestRegistrationGateway.class);
			when(testGateway.registrationCall(any(UUID.class), anyLong())).thenReturn(result);

			rpc.registerGateway(testEndpointAddress, testGateway);

			TestRetryingRegistration registration = new TestRetryingRegistration(rpc, testEndpointAddress, leaderId);
			registration.startRegistration();

			// cancel and fail the current registration attempt
			registration.cancel();
			result.completeExceptionally(new TimeoutException());

			// there should not be a second registration attempt
			verify(testGateway, atMost(1)).registrationCall(any(UUID.class), anyLong());
		}
		finally {
			rpc.stopService();
		}
	}

	// ------------------------------------------------------------------------
	//  test registration
	// ------------------------------------------------------------------------

	protected static class TestRegistrationSuccess extends RegistrationResponse.Success {
		private static final long serialVersionUID = 5542698790917150604L;

		private final String correlationId;

		public TestRegistrationSuccess(String correlationId) {
			this.correlationId = correlationId;
		}

		public String getCorrelationId() {
			return correlationId;
		}
	}

	protected static class TestRetryingRegistration extends RetryingRegistration<TestRegistrationGateway, TestRegistrationSuccess> {

		// we use shorter timeouts here to speed up the tests
		static final long INITIAL_TIMEOUT = 20;
		static final long MAX_TIMEOUT = 200;
		static final long DELAY_ON_ERROR = 200;
		static final long DELAY_ON_DECLINE = 200;

		public TestRetryingRegistration(RpcService rpc, String targetAddress, UUID leaderId) {
			super(LoggerFactory.getLogger(RetryingRegistrationTest.class),
					rpc, "TestEndpoint",
					TestRegistrationGateway.class,
					targetAddress, leaderId,
					INITIAL_TIMEOUT, MAX_TIMEOUT, DELAY_ON_ERROR, DELAY_ON_DECLINE);
		}

		@Override
		protected Future<RegistrationResponse> invokeRegistration(
				TestRegistrationGateway gateway, UUID leaderId, long timeoutMillis) {
			return gateway.registrationCall(leaderId, timeoutMillis);
		}
	}
}
