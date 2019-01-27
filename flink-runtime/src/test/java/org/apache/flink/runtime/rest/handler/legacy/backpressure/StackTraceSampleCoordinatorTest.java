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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for the {@link StackTraceSampleCoordinator}.
 */
public class StackTraceSampleCoordinatorTest extends TestLogger {

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

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
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

			Mockito.verify(vertex.getCurrentExecutionAttempt())
				.requestStackTraceSample(Matchers.eq(0), Matchers.eq(numSamples), Matchers.eq(delayBetweenSamples), Matchers.eq(maxStackTraceDepth), Matchers.any(Time.class));
		}

		Assert.assertFalse(sampleFuture.isDone());

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
				Assert.assertTrue(sampleFuture.isDone());
			} else {
				Assert.assertFalse(sampleFuture.isDone());
			}
		}

		// Verify completed stack trace sample
		StackTraceSample sample = sampleFuture.get();

		Assert.assertEquals(0, sample.getSampleId());
		Assert.assertTrue(sample.getEndTime() >= sample.getStartTime());

		Map<ExecutionAttemptID, List<StackTraceElement[]>> tracesByTask = sample.getStackTraces();

		for (ExecutionVertex vertex : vertices) {
			ExecutionAttemptID executionId = vertex.getCurrentExecutionAttempt().getAttemptId();
			List<StackTraceElement[]> sampleTraces = tracesByTask.get(executionId);

			Assert.assertNotNull("Task not found", sampleTraces);
			Assert.assertTrue(traces.equals(sampleTraces));
		}

		// Verify no more pending sample
		Assert.assertEquals(0, coord.getNumberOfPendingSamples());

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

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
			vertices,
			1,
			Time.milliseconds(100L),
			0);

		Assert.assertTrue(sampleFuture.isDone());

		try {
			sampleFuture.get();
			Assert.fail("Expected exception.");
		} catch (ExecutionException e) {
			Assert.assertTrue(e.getCause() instanceof IllegalStateException);
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

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
			vertices,
			1,
			Time.milliseconds(100L),
			0);

		try {
			sampleFuture.get();
			Assert.fail("Expected exception.");
		} catch (ExecutionException e) {
			Assert.assertTrue(e.getCause() instanceof RuntimeException);
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

			CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
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

			Assert.assertTrue("Sample did not time out", success);

			try {
				sampleFuture.get();
				Assert.fail("Expected exception.");
			} catch (ExecutionException e) {
				Assert.assertTrue(e.getCause().getCause().getMessage().contains("Timeout"));
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

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		Assert.assertFalse(sampleFuture.isDone());

		// Cancel
		coord.cancelStackTraceSample(0, null);

		// Verify completed
		Assert.assertTrue(sampleFuture.isDone());

		// Verify no more pending samples
		Assert.assertEquals(0, coord.getNumberOfPendingSamples());
	}

	/** Tests that collecting for a cancelled sample throws no Exception. */
	@Test
	public void testCollectStackTraceForCanceledSample() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		Assert.assertFalse(sampleFuture.isDone());

		coord.cancelStackTraceSample(0, null);

		Assert.assertTrue(sampleFuture.isDone());

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

		CompletableFuture<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		Assert.assertFalse(sampleFuture.isDone());

		coord.cancelStackTraceSample(0, null);

		Assert.assertTrue(sampleFuture.isDone());

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

		List<CompletableFuture<StackTraceSample>> sampleFutures = new ArrayList<>();

		// Trigger
		sampleFutures.add(coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0));

		sampleFutures.add(coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0));

		for (CompletableFuture<StackTraceSample> future : sampleFutures) {
			Assert.assertFalse(future.isDone());
		}

		// Shut down
		coord.shutDown();

		// Verify all completed
		for (CompletableFuture<StackTraceSample> future : sampleFutures) {
			Assert.assertTrue(future.isDone());
		}

		// Verify new trigger returns failed future
		CompletableFuture<StackTraceSample> future = coord.triggerStackTraceSample(
				vertices, 1, Time.milliseconds(100L), 0);

		Assert.assertTrue(future.isDone());

		try {
			future.get();
			Assert.fail("Expected exception.");
		} catch (ExecutionException e) {
			// we expected an exception here :-)
		}

	}

	// ------------------------------------------------------------------------

	private ExecutionVertex mockExecutionVertex(
			ExecutionAttemptID executionId,
			ExecutionState state,
			boolean sendSuccess) {

		Execution exec = Mockito.mock(Execution.class);
		CompletableFuture<StackTraceSampleResponse> failedFuture = new CompletableFuture<>();
		failedFuture.completeExceptionally(new Exception("Send failed."));

		Mockito.when(exec.getAttemptId()).thenReturn(executionId);
		Mockito.when(exec.getState()).thenReturn(state);
		Mockito.when(exec.requestStackTraceSample(Matchers.anyInt(), Matchers.anyInt(), Matchers.any(Time.class), Matchers.anyInt(), Matchers.any(Time.class)))
			.thenReturn(
				sendSuccess ?
					CompletableFuture.completedFuture(Mockito.mock(StackTraceSampleResponse.class)) :
					failedFuture);

		ExecutionVertex vertex = Mockito.mock(ExecutionVertex.class);
		Mockito.when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		Mockito.when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}

	private ExecutionVertex mockExecutionVertexWithTimeout(
		ExecutionAttemptID executionId,
		ExecutionState state,
		ScheduledExecutorService scheduledExecutorService,
		int timeout) {

		final CompletableFuture<StackTraceSampleResponse> future = new CompletableFuture<>();

		Execution exec = Mockito.mock(Execution.class);
		Mockito.when(exec.getAttemptId()).thenReturn(executionId);
		Mockito.when(exec.getState()).thenReturn(state);
		Mockito.when(exec.requestStackTraceSample(Matchers.anyInt(), Matchers.anyInt(), Matchers.any(Time.class), Matchers.anyInt(), Matchers.any(Time.class)))
			.thenReturn(future);

		scheduledExecutorService.schedule(new Runnable() {
			@Override
			public void run() {
				future.completeExceptionally(new TimeoutException("Timeout"));
			}
		}, timeout, TimeUnit.MILLISECONDS);

		ExecutionVertex vertex = Mockito.mock(ExecutionVertex.class);
		Mockito.when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		Mockito.when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}
}
