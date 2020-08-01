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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertexTest;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link BackPressureRequestCoordinator}.
 */
public class BackPressureRequestCoordinatorTest extends TestLogger {

	private static final long requestTimeout = 10000;
	private static final double backPressureRatio = 0.5;
	private static final String requestTimeoutMessage = "Request timeout.";

	private static ScheduledExecutorService executorService;
	private BackPressureRequestCoordinator coordinator;

	@Rule
	public Timeout caseTimeout = new Timeout(10, TimeUnit.SECONDS);

	@BeforeClass
	public static void setUp() throws Exception {
		executorService = new ScheduledThreadPoolExecutor(1);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (executorService != null) {
			executorService.shutdown();
		}
	}

	@Before
	public void initCoordinator() throws Exception {
		coordinator = new BackPressureRequestCoordinator(executorService, requestTimeout);
	}

	@After
	public void shutdownCoordinator() throws Exception {
		if (coordinator != null) {
			// verify no more pending request
			assertEquals(0, coordinator.getNumberOfPendingRequests());
			coordinator.shutDown();
		}
	}

	/**
	 * Tests request of task back pressure stats and verifies the response.
	 */
	@Test
	public void testSuccessfulBackPressureRequest() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> requestFuture = coordinator.triggerBackPressureRequest(vertices);
		BackPressureStats backPressureStats = requestFuture.get();

		// verify the request result
		assertEquals(0, backPressureStats.getRequestId());
		Map<ExecutionAttemptID, Double> backPressureRatios = backPressureStats.getBackPressureRatios();
		for (ExecutionVertex executionVertex : vertices) {
			ExecutionAttemptID executionId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			assertEquals(backPressureRatio, backPressureRatios.get(executionId), 0.0);
		}
	}

	/**
	 * Tests back pressure request of non-running tasks fails the future.
	 */
	@Test
	public void testRequestNotRunningTasks() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.DEPLOYING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> requestFuture = coordinator.triggerBackPressureRequest(vertices);
		assertTrue(requestFuture.isDone());
		try {
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/**
	 * Tests failed request to execution fails the future.
	 */
	@Test
	public void testBackPressureRequestWithException() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.EXCEPTIONALLY);

		CompletableFuture<BackPressureStats> requestFuture = coordinator.triggerBackPressureRequest(vertices);
		try {
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}
	}

	/**
	 * Tests that request timeout if not finished in time.
	 */
	@Test
	public void testBackPressureRequestTimeout() throws Exception {
		long requestTimeout = 100;
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.TIMEOUT, requestTimeout);
		BackPressureRequestCoordinator coordinator = new BackPressureRequestCoordinator(executorService, requestTimeout);

		try {
			CompletableFuture<BackPressureStats> requestFuture = coordinator.triggerBackPressureRequest(vertices);
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(e, requestTimeoutMessage).isPresent());
		} finally {
			coordinator.shutDown();
		}
	}

	/**
	 * Tests shutdown fails all pending requests and future request triggers.
	 */
	@Test
	public void testShutDown() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		List<CompletableFuture<BackPressureStats>> requestFutures = new ArrayList<>();

		// trigger request
		requestFutures.add(coordinator.triggerBackPressureRequest(vertices));
		requestFutures.add(coordinator.triggerBackPressureRequest(vertices));

		for (CompletableFuture<BackPressureStats> future : requestFutures) {
			assertFalse(future.isDone());
		}

		// shut down
		coordinator.shutDown();

		// verify all completed
		for (CompletableFuture<BackPressureStats> future : requestFutures) {
			assertTrue(future.isCompletedExceptionally());
		}

		// verify new trigger returns failed future
		CompletableFuture<BackPressureStats> future = coordinator.triggerBackPressureRequest(vertices);
		assertTrue(future.isCompletedExceptionally());
	}

	private ExecutionVertex[] createExecutionVertices(
			ExecutionState state,
			CompletionType completionType) throws Exception {
		return createExecutionVertices(state, completionType, requestTimeout);
	}

	private ExecutionVertex[] createExecutionVertices(
			ExecutionState state,
			CompletionType completionType,
			long requestTimeout) throws Exception {
		return new ExecutionVertex[] {
			createExecutionVertex(0, ExecutionState.RUNNING, CompletionType.SUCCESSFULLY, requestTimeout),
			createExecutionVertex(1, state, completionType, requestTimeout),
			createExecutionVertex(2, ExecutionState.RUNNING, CompletionType.SUCCESSFULLY, requestTimeout)
		};
	}

	private ExecutionVertex createExecutionVertex(
			int subTaskIndex,
			ExecutionState state,
			CompletionType completionType,
			long requestTimeout) throws Exception {
		return new TestingExecutionVertex(
			ExecutionJobVertexTest.createExecutionJobVertex(4, 4),
			subTaskIndex,
			Time.seconds(10),
			1L,
			System.currentTimeMillis(),
			state,
			completionType,
			requestTimeout);
	}

	/**
	 * Completion types of the request future.
	 */
	private enum CompletionType {
		SUCCESSFULLY,
		EXCEPTIONALLY,
		TIMEOUT,
		NEVER_COMPLETE
	}

	/**
	 * A testing {@link ExecutionVertex} implementation used to wrap {@link TestingExecution}.
	 */
	private static class TestingExecutionVertex extends ExecutionVertex {

		private final Execution execution;

		TestingExecutionVertex(
				ExecutionJobVertex jobVertex,
				int subTaskIndex,
				Time timeout,
				long initialGlobalModVersion,
				long createTimestamp,
				ExecutionState state,
				CompletionType completionType,
				long requestTimeout) {

			super(
				jobVertex,
				subTaskIndex,
				new IntermediateResult[0],
				timeout,
				initialGlobalModVersion,
				createTimestamp,
				JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue());
			execution = new TestingExecution(
				Runnable::run,
				this,
				0,
				initialGlobalModVersion,
				createTimestamp,
				timeout,
				state,
				completionType,
				requestTimeout);
		}

		@Override
		public Execution getCurrentExecutionAttempt() {
			return execution;
		}
	}

	/**
	 * A testing implementation of {@link Execution} which acts differently according to
	 * the given {@link ExecutionState} and {@link CompletionType}.
	 */
	private static class TestingExecution extends Execution {

		private final ExecutionState state;
		private final CompletionType completionType;
		private final long requestTimeout;

		TestingExecution(
				Executor executor,
				ExecutionVertex vertex,
				int attemptNumber,
				long globalModVersion,
				long startTimestamp,
				Time rpcTimeout,
				ExecutionState state,
				CompletionType completionType,
				long requestTimeout) {
			super(executor, vertex, attemptNumber, globalModVersion, startTimestamp, rpcTimeout);
			this.state = checkNotNull(state);
			this.completionType = checkNotNull(completionType);
			this.requestTimeout = requestTimeout;
		}

		@Override
		public CompletableFuture<TaskBackPressureResponse> requestBackPressure(int requestId, Time timeout) {
			CompletableFuture<TaskBackPressureResponse> responseFuture = new CompletableFuture<>();
			switch (completionType) {
				case SUCCESSFULLY:
					responseFuture.complete(new TaskBackPressureResponse(0, getAttemptId(), backPressureRatio));
					break;
				case EXCEPTIONALLY:
					responseFuture.completeExceptionally(new RuntimeException("Request failed."));
					break;
				case TIMEOUT:
					executorService.schedule(
						() -> responseFuture.completeExceptionally(new TimeoutException(requestTimeoutMessage)),
						requestTimeout,
						TimeUnit.MILLISECONDS);
					break;
				case NEVER_COMPLETE:
					break; // do nothing
				default:
					throw new RuntimeException("Unknown completion type.");
			}
			return responseFuture;
		}

		@Override
		public ExecutionState getState() {
			return state;
		}
	}
}
