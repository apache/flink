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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorSystem;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for the {@link StackTraceSampleCoordinator}.
 */
public class StackTraceSampleCoordinatorTest {

	private static ActorSystem system;

	private StackTraceSampleCoordinator coord;

	@BeforeClass
	public static void setUp() throws Exception {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (system != null) {
			system.shutdown();
		}
	}

	@Before
	public void init() throws Exception {
		this.coord = new StackTraceSampleCoordinator(system.dispatcher(), 60000);
	}

	/** Tests simple trigger and collect of stack trace samples. */
	@Test
	public void testTriggerStackTraceSample() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true)
		};

		int numSamples = 1;
		Time delayBetweenSamples = Time.milliseconds(100L);
		int maxStackTraceDepth = 0;

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, numSamples, delayBetweenSamples, maxStackTraceDepth);

		// Verify messages have been sent
		for (ExecutionVertex vertex : vertices) {
			ExecutionAttemptID expectedExecutionId = vertex
					.getCurrentExecutionAttempt().getAttemptId();

			TriggerStackTraceSample expectedMsg = new TriggerStackTraceSample(
					0,
					expectedExecutionId,
					numSamples,
					delayBetweenSamples,
					maxStackTraceDepth);

			verify(vertex.getCurrentExecutionAttempt())
				.requestStackTraceSample(eq(0), eq(numSamples), eq(delayBetweenSamples), eq(maxStackTraceDepth), any(Time.class));
		}

		assertFalse(sampleFuture.isDone());

		StackTraceElement[] stackTraceSample = Thread.currentThread().getStackTrace();
		List<StackTraceElement[]> traces = new ArrayList<>();
		traces.add(stackTraceSample);
		traces.add(stackTraceSample);
		traces.add(stackTraceSample);

		// Collect stack traces
		for (int i = 0; i < vertices.length; i++) {
			ExecutionAttemptID executionId = vertices[i].getCurrentExecutionAttempt().getAttemptId();
			coord.collectStackTraces(0, executionId, traces);

			if (i == vertices.length - 1) {
				assertTrue(sampleFuture.isDone());
			} else {
				assertFalse(sampleFuture.isDone());
			}
		}

		// Verify completed stack trace sample
		StackTraceSample sample = sampleFuture.get();

		assertEquals(0, sample.getSampleId());
		assertTrue(sample.getEndTime() >= sample.getStartTime());

		Map<ExecutionAttemptID, List<StackTraceElement[]>> tracesByTask = sample.getStackTraces();

		for (ExecutionVertex vertex : vertices) {
			ExecutionAttemptID executionId = vertex.getCurrentExecutionAttempt().getAttemptId();
			List<StackTraceElement[]> sampleTraces = tracesByTask.get(executionId);

			assertNotNull("Task not found", sampleTraces);
			assertTrue(traces.equals(sampleTraces));
		}

		// Verify no more pending sample
		assertEquals(0, coord.getNumberOfPendingSamples());

		// Verify no error on late collect
		coord.collectStackTraces(0, vertices[0].getCurrentExecutionAttempt().getAttemptId(), traces);
	}

	/** Tests triggering for non-running tasks fails the future. */
	@Test
	public void testTriggerStackTraceSampleNotRunningTasks() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.DEPLOYING, true)
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
			vertices,
			1,
			Time.milliseconds(100L),
			0);

		assertTrue(sampleFuture.isDone());

		try {
			sampleFuture.get();
			fail("Expected exception.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IllegalStateException);
		}
	}

	/** Tests triggering for reset tasks fails the future. */
	@Test(timeout = 1000L)
	public void testTriggerStackTraceSampleResetRunningTasks() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				// Fails to send the message to the execution (happens when execution is reset)
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, false)
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
			vertices,
			1,
			Time.milliseconds(100L),
			0);

		try {
			sampleFuture.get();
			fail("Expected exception.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}
	}

	/** Tests that samples time out if they don't finish in time. */
	@Test(timeout = 1000L)
	public void testTriggerStackTraceSampleTimeout() throws Exception {
		int timeout = 100;

		coord = new StackTraceSampleCoordinator(system.dispatcher(), timeout);

		final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

		try {

			ExecutionVertex[] vertices = new ExecutionVertex[]{
				mockExecutionVertexWithTimeout(
					new ExecutionAttemptID(),
					ExecutionState.RUNNING,
					scheduledExecutorService,
					timeout)
			};

			Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

			// Wait for the timeout
			Thread.sleep(timeout * 2);

			boolean success = false;
			for (int i = 0; i < 10; i++) {
				if (sampleFuture.isDone()) {
					success = true;
					break;
				}

				Thread.sleep(timeout);
			}

			assertTrue("Sample did not time out", success);

			try {
				sampleFuture.get();
				fail("Expected exception.");
			} catch (ExecutionException e) {
				assertTrue(e.getCause().getCause().getMessage().contains("Timeout"));
			}


			// Collect after the timeout (should be ignored)
			ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
			coord.collectStackTraces(0, executionId, new ArrayList<StackTraceElement[]>());
		} finally {
			scheduledExecutorService.shutdownNow();
		}
	}

	/** Tests that collecting an unknown sample is ignored. */
	@Test
	public void testCollectStackTraceForUnknownSample() throws Exception {
		coord.collectStackTraces(0, new ExecutionAttemptID(), new ArrayList<StackTraceElement[]>());
	}

	/** Tests cancelling of a pending sample. */
	@Test
	public void testCancelStackTraceSample() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		assertFalse(sampleFuture.isDone());

		// Cancel
		coord.cancelStackTraceSample(0, null);

		// Verify completed
		assertTrue(sampleFuture.isDone());

		// Verify no more pending samples
		assertEquals(0, coord.getNumberOfPendingSamples());
	}

	/** Tests that collecting for a cancelled sample throws no Exception. */
	@Test
	public void testCollectStackTraceForCanceledSample() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		assertFalse(sampleFuture.isDone());

		coord.cancelStackTraceSample(0, null);

		assertTrue(sampleFuture.isDone());

		// Verify no error on late collect
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectStackTraces(0, executionId, new ArrayList<StackTraceElement[]>());
	}

	/** Tests that collecting for a cancelled sample throws no Exception. */
	@Test
	public void testCollectForDiscardedPendingSample() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		assertFalse(sampleFuture.isDone());

		coord.cancelStackTraceSample(0, null);

		assertTrue(sampleFuture.isDone());

		// Verify no error on late collect
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectStackTraces(0, executionId, new ArrayList<StackTraceElement[]>());
	}


	/** Tests that collecting for a unknown task fails. */
	@Test(expected = IllegalArgumentException.class)
	public void testCollectStackTraceForUnknownTask() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		coord.triggerStackTraceSample(vertices, 1, Time.milliseconds(100L), 0);

		coord.collectStackTraces(0, new ExecutionAttemptID(), new ArrayList<StackTraceElement[]>());
	}

	/** Tests that shut down fails all pending samples and future sample triggers. */
	@Test
	public void testShutDown() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		List<Future<StackTraceSample>> sampleFutures = new ArrayList<>();

		// Trigger
		sampleFutures.add(coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0));

		sampleFutures.add(coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0));

		for (Future<StackTraceSample> future : sampleFutures) {
			assertFalse(future.isDone());
		}

		// Shut down
		coord.shutDown();

		// Verify all completed
		for (Future<StackTraceSample> future : sampleFutures) {
			assertTrue(future.isDone());
		}

		// Verify new trigger returns failed future
		Future<StackTraceSample> future = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		assertTrue(future.isDone());

		try {
			future.get();
			fail("Expected exception.");
		} catch (ExecutionException e) {
			// we expected an exception here :-)
		}

	}

	// ------------------------------------------------------------------------

	private ExecutionVertex mockExecutionVertex(
			ExecutionAttemptID executionId,
			ExecutionState state,
			boolean sendSuccess) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(executionId);
		when(exec.getState()).thenReturn(state);
		when(exec.requestStackTraceSample(anyInt(), anyInt(), any(Time.class), anyInt(), any(Time.class)))
			.thenReturn(
				sendSuccess ?
					FlinkCompletableFuture.completed(mock(StackTraceSampleResponse.class)) :
					FlinkCompletableFuture.<StackTraceSampleResponse>completedExceptionally(new Exception("Send failed")));

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}

	private ExecutionVertex mockExecutionVertexWithTimeout(
		ExecutionAttemptID executionId,
		ExecutionState state,
		ScheduledExecutorService scheduledExecutorService,
		int timeout) {

		final CompletableFuture<StackTraceSampleResponse> future = new FlinkCompletableFuture<>();

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(executionId);
		when(exec.getState()).thenReturn(state);
		when(exec.requestStackTraceSample(anyInt(), anyInt(), any(Time.class), anyInt(), any(Time.class)))
			.thenReturn(future);

		scheduledExecutorService.schedule(new Runnable() {
			@Override
			public void run() {
				future.completeExceptionally(new TimeoutException("Timeout"));
			}
		}, timeout, TimeUnit.MILLISECONDS);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}
}
