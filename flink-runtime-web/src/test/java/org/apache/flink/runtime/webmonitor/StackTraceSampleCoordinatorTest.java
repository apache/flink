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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.StackTraceSampleMessages.TriggerStackTraceSample;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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
		this.coord = new StackTraceSampleCoordinator(system, 60000);
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
		FiniteDuration delayBetweenSamples = new FiniteDuration(100, TimeUnit.MILLISECONDS);
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

			verify(vertex).sendMessageToCurrentExecution(
					eq(expectedMsg), eq(expectedExecutionId), any(AkkaActorGateway.class));
		}

		assertFalse(sampleFuture.isCompleted());

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
				assertTrue(sampleFuture.isCompleted());
			} else {
				assertFalse(sampleFuture.isCompleted());
			}
		}

		// Verify completed stack trace sample
		StackTraceSample sample = sampleFuture.value().get().get();

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
				new FiniteDuration(100, TimeUnit.MILLISECONDS),
				0);

		assertTrue(sampleFuture.isCompleted());
		assertTrue(sampleFuture.failed().isCompleted());

		assertTrue(sampleFuture.failed().value().get().get() instanceof IllegalStateException);
	}

	/** Tests triggering for reset tasks fails the future. */
	@Test
	public void testTriggerStackTraceSampleResetRunningTasks() throws Exception {
		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
				// Fails to send the message to the execution (happens when execution is reset)
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, false)
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices,
				1,
				new FiniteDuration(100, TimeUnit.MILLISECONDS),
				0);

		assertTrue(sampleFuture.isCompleted());
		assertTrue(sampleFuture.failed().isCompleted());
		assertTrue(sampleFuture.failed().value().get().get().getCause() instanceof RuntimeException);
	}

	/** Tests that samples time out if they don't finish in time. */
	@Test
	public void testTriggerStackTraceSampleTimeout() throws Exception {
		int timeout = 100;

		coord = new StackTraceSampleCoordinator(system, timeout);

		ExecutionVertex[] vertices = new ExecutionVertex[] {
				mockExecutionVertex(new ExecutionAttemptID(), ExecutionState.RUNNING, true),
		};

		Future<StackTraceSample> sampleFuture = coord.triggerStackTraceSample(
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

		// Wait for the timeout
		Thread.sleep(timeout * 2);

		boolean success = false;
		for (int i = 0; i < 10; i++) {
			if (sampleFuture.isCompleted()) {
				success = true;
				break;
			}

			Thread.sleep(timeout);
		}

		assertTrue("Sample did not time out", success);

		Throwable cause = sampleFuture.failed().value().get().get();
		assertTrue(cause.getCause().getMessage().contains("Time out"));

		// Collect after the timeout (should be ignored)
		ExecutionAttemptID executionId = vertices[0].getCurrentExecutionAttempt().getAttemptId();
		coord.collectStackTraces(0, executionId, new ArrayList<StackTraceElement[]>());
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
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

		assertFalse(sampleFuture.isCompleted());

		// Cancel
		coord.cancelStackTraceSample(0, null);

		// Verify completed
		assertTrue(sampleFuture.isCompleted());

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
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

		assertFalse(sampleFuture.isCompleted());

		coord.cancelStackTraceSample(0, null);

		assertTrue(sampleFuture.isCompleted());

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
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

		assertFalse(sampleFuture.isCompleted());

		coord.cancelStackTraceSample(0, null);

		assertTrue(sampleFuture.isCompleted());

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

		coord.triggerStackTraceSample(vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

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
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0));

		sampleFutures.add(coord.triggerStackTraceSample(
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0));

		for (Future<StackTraceSample> future : sampleFutures) {
			assertFalse(future.isCompleted());
		}

		// Shut down
		coord.shutDown();

		// Verify all completed
		for (Future<StackTraceSample> future : sampleFutures) {
			assertTrue(future.isCompleted());
		}

		// Verify new trigger returns failed future
		Future<StackTraceSample> future = coord.triggerStackTraceSample(
				vertices, 1, new FiniteDuration(100, TimeUnit.MILLISECONDS), 0);

		assertTrue(future.isCompleted());
		assertTrue(future.failed().isCompleted());
	}

	// ------------------------------------------------------------------------

	private ExecutionVertex mockExecutionVertex(
			ExecutionAttemptID executionId,
			ExecutionState state,
			boolean sendSuccess) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(executionId);
		when(exec.getState()).thenReturn(state);

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(new JobVertexID());
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.sendMessageToCurrentExecution(
				any(Serializable.class), any(ExecutionAttemptID.class), any(AkkaActorGateway.class)))
				.thenReturn(sendSuccess);

		return vertex;
	}
}
