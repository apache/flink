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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link BackPressureRequestCoordinator}.
 */
public class BackPressureRequestCoordinatorTest extends TestLogger {

	private static final long requestTimeout = 10000;
	private static final double backPressureRatio = 0.5;

	private static ScheduledExecutorService executorService;
	private BackPressureRequestCoordinator coord;

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
	public void init() throws Exception {
		coord = new BackPressureRequestCoordinator(executorService, requestTimeout);
	}

	/** Tests simple request of task back pressure stats. */
	@Test(timeout = 10000L)
	public void testTriggerBackPressureRequest() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);
		BackPressureStats backPressureStats = requestFuture.get();

		// verify the request result
		assertEquals(0, backPressureStats.getRequestId());
		assertTrue(backPressureStats.getEndTime() >= backPressureStats.getStartTime());

		Map<ExecutionAttemptID, Double> tracesByTask = backPressureStats.getBackPressureRatios();
		for (ExecutionVertex executionVertex : vertices) {
			ExecutionAttemptID executionId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			assertEquals(backPressureRatio, tracesByTask.get(executionId), 0.0);
		}

		// verify no more pending request
		assertEquals(0, coord.getNumberOfPendingRequests());

		// verify no error on late collect
		coord.collectTaskBackPressureStat(0, vertices[0].getCurrentExecutionAttempt().getAttemptId(), 0.0);
	}

	/** Tests simple request and collection of task back pressure stats. */
	@Test(timeout = 10000L)
	public void testCollectTaskBackPressureStat() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);
		assertFalse(requestFuture.isDone());

		coord.collectTaskBackPressureStat(0, vertices[1].getCurrentExecutionAttempt().getAttemptId(), backPressureRatio);
		BackPressureStats backPressureStats = requestFuture.get();

		// verify the request result
		assertEquals(0, backPressureStats.getRequestId());
		assertTrue(backPressureStats.getEndTime() >= backPressureStats.getStartTime());

		Map<ExecutionAttemptID, Double> tracesByTask = backPressureStats.getBackPressureRatios();
		for (ExecutionVertex executionVertex : vertices) {
			ExecutionAttemptID executionId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			assertEquals(backPressureRatio, tracesByTask.get(executionId), 0.0);
		}

		// verify no more pending request
		assertEquals(0, coord.getNumberOfPendingRequests());

		// verify no error on late collect
		coord.collectTaskBackPressureStat(0, vertices[0].getCurrentExecutionAttempt().getAttemptId(), 0.0);
	}

	/** Tests back pressure request of non-running tasks fails the future. */
	@Test
	public void testRequestNotRunningTasks() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.DEPLOYING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);
		assertTrue(requestFuture.isDone());
		try {
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/** Tests failed request to execution fails the future. */
	@Test(timeout = 10000L)
	public void testRequestToExecutionFails() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.EXCEPTIONALLY);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);
		try {
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}
	}

	/** Tests that request timeout if not finished in time. */
	@Test(timeout = 10000L)
	public void testTriggerBackPressureRequestTimeout() throws Exception {
		final long requestTimeout = 1000;
		coord = new BackPressureRequestCoordinator(executorService, requestTimeout);
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.TIMEOUT, requestTimeout);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);

		// wait until finish
		while (!requestFuture.isDone()) {
			Thread.sleep(100);
		}

		try {
			requestFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause().getCause().getMessage().toLowerCase().contains("timeout"));
		}

		// collect after timeout should be ignored
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectTaskBackPressureStat(0, executionId, 0.0);
	}

	/** Tests that collecting of unknown request id is ignored. */
	@Test
	public void testCollectBackPressureForUnknownRequest() throws Exception {
		coord.collectTaskBackPressureStat(0, new ExecutionAttemptID(), 0.0);
	}

	/** Tests cancellation of a pending request. */
	@Test
	public void testCancelTaskBackPressureRequest() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		CompletableFuture<BackPressureStats> requestFuture = coord.triggerBackPressureRequest(vertices);
		assertFalse(requestFuture.isDone());

		coord.cancelBackPressureRequest(0, null);

		assertTrue(requestFuture.isCompletedExceptionally());

		// collect on canceled request should be ignored
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectTaskBackPressureStat(0, executionId, 0.0);
	}

	/** Tests that collecting for a unknown task fails. */
	@Test(expected = IllegalArgumentException.class)
	public void testCollectStackTraceForUnknownTask() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.SUCCESSFULLY);

		coord.triggerBackPressureRequest(vertices);

		coord.collectTaskBackPressureStat(0, new ExecutionAttemptID(), 0.0);
	}

	/** Tests shutdown fails all pending requests and future request triggers. */
	@Test
	public void testShutDown() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		List<CompletableFuture<BackPressureStats>> requestFutures = new ArrayList<>();

		// trigger request
		requestFutures.add(coord.triggerBackPressureRequest(vertices));
		requestFutures.add(coord.triggerBackPressureRequest(vertices));

		for (CompletableFuture<BackPressureStats> future : requestFutures) {
			assertFalse(future.isDone());
		}

		// shut down
		coord.shutDown();

		// verify all completed
		for (CompletableFuture<BackPressureStats> future : requestFutures) {
			assertTrue(future.isCompletedExceptionally());
		}

		// verify new trigger returns failed future
		CompletableFuture<BackPressureStats> future = coord.triggerBackPressureRequest(vertices);
		assertTrue(future.isCompletedExceptionally());
	}

	private ExecutionVertex[] createExecutionVertices(ExecutionState state, CompletionType completionType) {
		return createExecutionVertices(state, completionType, requestTimeout);
	}

	private ExecutionVertex[] createExecutionVertices(
			ExecutionState state, CompletionType completionType, long requestTimeout) {
		return new ExecutionVertex[] {
			createExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, CompletionType.SUCCESSFULLY, requestTimeout),
			createExecutionVertex(new ExecutionAttemptID(), state, completionType, requestTimeout)
		};
	}

	private ExecutionVertex createExecutionVertex(
			ExecutionAttemptID executionId,
			ExecutionState state,
			CompletionType completionType,
			long requestTimeout) {

		Execution execution = mock(Execution.class);
		CompletableFuture<TaskBackPressureResponse> responseFuture = new CompletableFuture<>();
		switch (completionType) {
			case SUCCESSFULLY:
				responseFuture.complete(new TaskBackPressureResponse(0, executionId, backPressureRatio));
				break;
			case EXCEPTIONALLY:
				responseFuture.completeExceptionally(new RuntimeException("Request failed."));
				break;
			case TIMEOUT:
				executorService.schedule(
					() -> responseFuture.completeExceptionally(new TimeoutException("Request timeout.")),
					requestTimeout,
					TimeUnit.MILLISECONDS);
				break;
			case NEVER_COMPLETE:
				break; // do nothing
			default:
				throw new RuntimeException("Unknown completion type.");
		}

		when(execution.getAttemptId()).thenReturn(executionId);
		when(execution.getState()).thenReturn(state);
		when(execution.requestBackPressure(
			ArgumentMatchers.anyInt(),
			ArgumentMatchers.any(Time.class))).thenReturn(responseFuture);

		ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		when(executionVertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(executionVertex.getCurrentExecutionAttempt()).thenReturn(execution);

		return executionVertex;
	}

	/**
	 * Completion type of the request future.
	 */
	private enum CompletionType {
		SUCCESSFULLY,
		EXCEPTIONALLY,
		TIMEOUT,
		NEVER_COMPLETE
	}
}
