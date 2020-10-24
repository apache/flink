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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link BackPressureRequestCoordinator}.
 */
public class BackPressureRequestCoordinatorTest extends TestLogger {

	private static final long requestTimeout = 100;
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
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.TIMEOUT);

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

		final ExecutionJobVertex ejv = ExecutionGraphTestUtils.getExecutionJobVertex(
			new JobVertexID(),
			3,
			null,
			new DirectScheduledExecutorService(),
			ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionVertex[] vertices = ejv.getTaskVertices();

		assignSlot(vertices[0], CompletionType.SUCCESSFULLY);
		vertices[0].getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

		assignSlot(vertices[1], completionType);
		vertices[1].getCurrentExecutionAttempt().transitionState(state);

		assignSlot(vertices[2], CompletionType.SUCCESSFULLY);
		vertices[2].getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

		return vertices;
	}

	private static void assignSlot(ExecutionVertex executionVertex, CompletionType completionType) {
		final LogicalSlot slot = new TestingLogicalSlotBuilder()
			.setTaskManagerGateway(
				createTaskManagerGateway(executionVertex.getCurrentExecutionAttempt().getAttemptId(), completionType))
			.createTestingLogicalSlot();
		ExecutionGraphTestUtils.setVertexResource(executionVertex, slot);
	}

	private static TaskManagerGateway createTaskManagerGateway(
			ExecutionAttemptID executionAttemptId,
			CompletionType completionType) {
		final CompletableFuture<TaskBackPressureResponse> responseFuture = new CompletableFuture<>();
		switch (completionType) {
			case SUCCESSFULLY:
				responseFuture.complete(new TaskBackPressureResponse(0, executionAttemptId, backPressureRatio));
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
				// do nothing
				break;
			default:
				throw new RuntimeException("Unknown completion type.");
		}

		return new MockBackPressureRequestTaskManagerGateway(responseFuture);
	}

	private static class MockBackPressureRequestTaskManagerGateway extends SimpleAckingTaskManagerGateway {
		private final CompletableFuture<TaskBackPressureResponse> responseFuture;

		private MockBackPressureRequestTaskManagerGateway(CompletableFuture<TaskBackPressureResponse> responseFuture) {
			this.responseFuture = responseFuture;
		}

		@Override
		public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
				ExecutionAttemptID executionAttemptId,
				int requestId,
				Time timeout) {
			return responseFuture;
		}
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
}
