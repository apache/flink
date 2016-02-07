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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackPressureStatsTrackerTest {

	/** Tests simple statistics with fake stack traces. */
	@Test
	@SuppressWarnings("unchecked")
	public void testTriggerStackTraceSample() throws Exception {
		Promise<StackTraceSample> samplePromise = new Promise.DefaultPromise<>();

		StackTraceSampleCoordinator sampleCoordinator = mock(StackTraceSampleCoordinator.class);
		when(sampleCoordinator.triggerStackTraceSample(
				any(ExecutionVertex[].class),
				anyInt(),
				any(FiniteDuration.class),
				anyInt())).thenReturn(samplePromise.future());

		ExecutionGraph graph = mock(ExecutionGraph.class);

		// Same Thread execution context
		when(graph.getExecutionContext()).thenReturn(new ExecutionContext() {

			@Override
			public void execute(Runnable runnable) {
				runnable.run();
			}

			@Override
			public void reportFailure(Throwable t) {
				fail();
			}

			@Override
			public ExecutionContext prepare() {
				return this;
			}
		});

		ExecutionVertex[] taskVertices = new ExecutionVertex[4];

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobId()).thenReturn(new JobID());
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getGraph()).thenReturn(graph);
		when(jobVertex.getTaskVertices()).thenReturn(taskVertices);

		taskVertices[0] = mockExecutionVertex(jobVertex, 0);
		taskVertices[1] = mockExecutionVertex(jobVertex, 1);
		taskVertices[2] = mockExecutionVertex(jobVertex, 2);
		taskVertices[3] = mockExecutionVertex(jobVertex, 3);

		int numSamples = 100;
		FiniteDuration delayBetweenSamples = new FiniteDuration(100, TimeUnit.MILLISECONDS);

		BackPressureStatsTracker tracker = new BackPressureStatsTracker(
				sampleCoordinator, 9999, numSamples, delayBetweenSamples);

		// Trigger
		tracker.triggerStackTraceSample(jobVertex);

		verify(sampleCoordinator).triggerStackTraceSample(
				eq(taskVertices),
				eq(numSamples),
				eq(delayBetweenSamples),
				eq(BackPressureStatsTracker.MAX_STACK_TRACE_DEPTH));

		// Trigger again for pending request, should not fire
		tracker.triggerStackTraceSample(jobVertex);

		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isEmpty());

		verify(sampleCoordinator).triggerStackTraceSample(
				eq(taskVertices),
				eq(numSamples),
				eq(delayBetweenSamples),
				eq(BackPressureStatsTracker.MAX_STACK_TRACE_DEPTH));

		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isEmpty());

		// Complete the future
		Map<ExecutionAttemptID, List<StackTraceElement[]>> traces = new HashMap<>();
		for (ExecutionVertex vertex : taskVertices) {
			List<StackTraceElement[]> taskTraces = new ArrayList<>();

			for (int i = 0; i < taskVertices.length; i++) {
				// Traces until sub task index are back pressured
				taskTraces.add(createStackTrace(i <= vertex.getParallelSubtaskIndex()));
			}

			traces.put(vertex.getCurrentExecutionAttempt().getAttemptId(), taskTraces);
		}

		int sampleId = 1231;
		int endTime = 841;

		StackTraceSample sample = new StackTraceSample(
				sampleId,
				0,
				endTime,
				traces);

		// Succeed the promise
		samplePromise.success(sample);

		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isDefined());

		OperatorBackPressureStats stats = tracker.getOperatorBackPressureStats(jobVertex).get();

		// Verify the stats
		assertEquals(sampleId, stats.getSampleId());
		assertEquals(endTime, stats.getEndTimestamp());
		assertEquals(taskVertices.length, stats.getNumberOfSubTasks());
		
		for (int i = 0; i < taskVertices.length; i++) {
			double ratio = stats.getBackPressureRatio(i);
			// Traces until sub task index are back pressured
			assertEquals((i + 1) / ((double) 4), ratio, 0.0);
		}
	}

	private StackTraceElement[] createStackTrace(boolean isBackPressure) {
		if (isBackPressure) {
			return new StackTraceElement[] { new StackTraceElement(
					BackPressureStatsTracker.EXPECTED_CLASS_NAME,
					BackPressureStatsTracker.EXPECTED_METHOD_NAME,
					"LocalBufferPool.java",
					133) };
		} else {
			return Thread.currentThread().getStackTrace();
		}
	}

	private ExecutionVertex mockExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());

		JobVertexID id = jobVertex.getJobVertexId();

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(id);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getParallelSubtaskIndex()).thenReturn(subTaskIndex);

		return vertex;
	}

}
