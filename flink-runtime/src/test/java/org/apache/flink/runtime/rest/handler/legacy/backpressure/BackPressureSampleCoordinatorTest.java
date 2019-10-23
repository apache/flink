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
import org.apache.flink.runtime.messages.TaskBackPressureSampleResponse;
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
 * Tests for the {@link BackPressureSampleCoordinator}.
 */
public class BackPressureSampleCoordinatorTest extends TestLogger {

	private static final long sampleTimeout = 10000;
	private static final double backPressureRatio = 0.5;

	private static ScheduledExecutorService executorService;
	private BackPressureSampleCoordinator coord;

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
		coord = new BackPressureSampleCoordinator(executorService, sampleTimeout);
	}

	/** Tests simple sampling of task back pressure stats. */
	@Test(timeout = 10000L)
	public void testTriggerStackTraceSample() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);
		BackPressureStats backPressureStats = sampleFuture.get();

		// verify the sampling result
		assertEquals(0, backPressureStats.getSampleId());
		assertTrue(backPressureStats.getEndTime() >= backPressureStats.getStartTime());

		Map<ExecutionAttemptID, Double> tracesByTask = backPressureStats.getBackPressureRatioByTask();
		for (ExecutionVertex executionVertex : vertices) {
			ExecutionAttemptID executionId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			assertEquals(backPressureRatio, tracesByTask.get(executionId), 0.0);
		}

		// verify no more pending sample
		assertEquals(0, coord.getNumberOfPendingSamples());

		// verify no error on late collect
		coord.collectTaskBackPressureStat(0, vertices[0].getCurrentExecutionAttempt().getAttemptId(), 0.0);
	}

	/** Tests simple sampling and collection of task back pressure stats. */
	@Test(timeout = 10000L)
	public void testCollectTaskBackPressureStat() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);
		assertFalse(sampleFuture.isDone());

		coord.collectTaskBackPressureStat(0, vertices[1].getCurrentExecutionAttempt().getAttemptId(), backPressureRatio);
		BackPressureStats backPressureStats = sampleFuture.get();

		// verify the sampling result
		assertEquals(0, backPressureStats.getSampleId());
		assertTrue(backPressureStats.getEndTime() >= backPressureStats.getStartTime());

		Map<ExecutionAttemptID, Double> tracesByTask = backPressureStats.getBackPressureRatioByTask();
		for (ExecutionVertex executionVertex : vertices) {
			ExecutionAttemptID executionId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			assertEquals(backPressureRatio, tracesByTask.get(executionId), 0.0);
		}

		// verify no more pending sample
		assertEquals(0, coord.getNumberOfPendingSamples());

		// verify no error on late collect
		coord.collectTaskBackPressureStat(0, vertices[0].getCurrentExecutionAttempt().getAttemptId(), 0.0);
	}

	/** Tests sampling of non-running tasks fails the future. */
	@Test
	public void testSampleNotRunningTasks() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.DEPLOYING, CompletionType.SUCCESSFULLY);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);
		assertTrue(sampleFuture.isDone());
		try {
			sampleFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/** Tests failed execution sampling fails the future. */
	@Test(timeout = 10000L)
	public void testExecutionSamplingFails() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.EXCEPTIONALLY);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);
		try {
			sampleFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}
	}

	/** Tests that samples timeout if not finished in time. */
	@Test(timeout = 10000L)
	public void testTriggerStackTraceSampleTimeout() throws Exception {
		final long sampleTimeout = 1000;
		coord = new BackPressureSampleCoordinator(executorService, sampleTimeout);
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.TIMEOUT, sampleTimeout);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);

		// wait until finish
		while (!sampleFuture.isDone()) {
			Thread.sleep(100);
		}

		try {
			sampleFuture.get();
			fail("Exception expected.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause().getCause().getMessage().toLowerCase().contains("timeout"));
		}

		// collect after timeout should be ignored
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectTaskBackPressureStat(0, executionId, 0.0);
	}

	/** Tests that collecting of unknown sample id is ignored. */
	@Test
	public void testCollectStackTraceForUnknownSample() throws Exception {
		coord.collectTaskBackPressureStat(0, new ExecutionAttemptID(), 0.0);
	}

	/** Tests cancellation of a pending sample. */
	@Test
	public void testCancelTaskBackPressureSample() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		CompletableFuture<BackPressureStats> sampleFuture = triggerTaskBackPressureSample(vertices);
		assertFalse(sampleFuture.isDone());

		coord.cancelTaskBackPressureSample(0, null);

		assertTrue(sampleFuture.isCompletedExceptionally());

		// collect on canceled sample should be ignored
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectTaskBackPressureStat(0, executionId, 0.0);
	}

	/** Tests that collecting for a unknown task fails. */
	@Test(expected = IllegalArgumentException.class)
	public void testCollectStackTraceForUnknownTask() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.SUCCESSFULLY);

		triggerTaskBackPressureSample(vertices);

		coord.collectTaskBackPressureStat(0, new ExecutionAttemptID(), 0.0);
	}

	/** Tests shutdown fails all pending samples and future sample triggers. */
	@Test
	public void testShutDown() throws Exception {
		ExecutionVertex[] vertices = createExecutionVertices(ExecutionState.RUNNING, CompletionType.NEVER_COMPLETE);

		List<CompletableFuture<BackPressureStats>> sampleFutures = new ArrayList<>();

		// trigger sampling
		sampleFutures.add(triggerTaskBackPressureSample(vertices));
		sampleFutures.add(triggerTaskBackPressureSample(vertices));

		for (CompletableFuture<BackPressureStats> future : sampleFutures) {
			assertFalse(future.isDone());
		}

		// shut down
		coord.shutDown();

		// verify all completed
		for (CompletableFuture<BackPressureStats> future : sampleFutures) {
			assertTrue(future.isCompletedExceptionally());
		}

		// verify new trigger returns failed future
		CompletableFuture<BackPressureStats> future = triggerTaskBackPressureSample(vertices);
		assertTrue(future.isCompletedExceptionally());
	}

	private ExecutionVertex[] createExecutionVertices(ExecutionState state, CompletionType completionType) {
		return createExecutionVertices(state, completionType, sampleTimeout);
	}

	private ExecutionVertex[] createExecutionVertices(
			ExecutionState state, CompletionType completionType, long sampleTimeout) {
		return new ExecutionVertex[] {
			createExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, CompletionType.SUCCESSFULLY, sampleTimeout),
			createExecutionVertex(new ExecutionAttemptID(), state, completionType, sampleTimeout)
		};
	}

	private CompletableFuture<BackPressureStats> triggerTaskBackPressureSample(ExecutionVertex[] vertices) {
		return coord.triggerTaskBackPressureSample(vertices, 100, Time.milliseconds(50L));
	}

	private ExecutionVertex createExecutionVertex(
			ExecutionAttemptID executionId,
			ExecutionState state,
			CompletionType completionType,
			long sampleTimeout) {

		Execution execution = mock(Execution.class);
		CompletableFuture<TaskBackPressureSampleResponse> responseFuture = new CompletableFuture<>();
		switch (completionType) {
			case SUCCESSFULLY:
				responseFuture.complete(new TaskBackPressureSampleResponse(0, executionId, backPressureRatio));
				break;
			case EXCEPTIONALLY:
				responseFuture.completeExceptionally(new RuntimeException("Sampling failed."));
				break;
			case TIMEOUT:
				executorService.schedule(
					() -> responseFuture.completeExceptionally(new TimeoutException("Sampling timeout.")),
					sampleTimeout,
					TimeUnit.MILLISECONDS);
				break;
			case NEVER_COMPLETE:
				break; // do nothing
			default:
				throw new RuntimeException("Unknown completion type.");
		}

		when(execution.getAttemptId()).thenReturn(executionId);
		when(execution.getState()).thenReturn(state);
		when(execution.sampleTaskBackPressure(
			ArgumentMatchers.anyInt(),
			ArgumentMatchers.anyInt(),
			ArgumentMatchers.any(Time.class),
			ArgumentMatchers.any(Time.class))).thenReturn(responseFuture);

		ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		when(executionVertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(executionVertex.getCurrentExecutionAttempt()).thenReturn(execution);

		return executionVertex;
	}

	/**
	 * Completion type of the sampling future.
	 */
	private enum CompletionType {
		SUCCESSFULLY,
		EXCEPTIONALLY,
		TIMEOUT,
		NEVER_COMPLETE
	}
}
