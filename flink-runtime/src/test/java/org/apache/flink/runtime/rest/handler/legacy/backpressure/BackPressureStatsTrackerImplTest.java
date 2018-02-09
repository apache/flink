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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Tests for the BackPressureStatsTrackerImpl.
 */
public class BackPressureStatsTrackerImplTest extends TestLogger {

	/** Tests simple statistics with fake stack traces. */
	@Test
	@SuppressWarnings("unchecked")
	public void testTriggerStackTraceSample() throws Exception {
		CompletableFuture<StackTraceSample> sampleFuture = new CompletableFuture<>();

		StackTraceSampleCoordinator sampleCoordinator = Mockito.mock(StackTraceSampleCoordinator.class);
		Mockito.when(sampleCoordinator.triggerStackTraceSample(
				Matchers.any(ExecutionVertex[].class),
				Matchers.anyInt(),
				Matchers.any(Time.class),
				Matchers.anyInt())).thenReturn(sampleFuture);

		ExecutionGraph graph = Mockito.mock(ExecutionGraph.class);
		Mockito.when(graph.getState()).thenReturn(JobStatus.RUNNING);

		// Same Thread execution context
		Mockito.when(graph.getFutureExecutor()).thenReturn(new Executor() {

			@Override
			public void execute(Runnable runnable) {
				runnable.run();
			}
		});

		ExecutionVertex[] taskVertices = new ExecutionVertex[4];

		ExecutionJobVertex jobVertex = Mockito.mock(ExecutionJobVertex.class);
		Mockito.when(jobVertex.getJobId()).thenReturn(new JobID());
		Mockito.when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		Mockito.when(jobVertex.getGraph()).thenReturn(graph);
		Mockito.when(jobVertex.getTaskVertices()).thenReturn(taskVertices);

		taskVertices[0] = mockExecutionVertex(jobVertex, 0);
		taskVertices[1] = mockExecutionVertex(jobVertex, 1);
		taskVertices[2] = mockExecutionVertex(jobVertex, 2);
		taskVertices[3] = mockExecutionVertex(jobVertex, 3);

		int numSamples = 100;
		Time delayBetweenSamples = Time.milliseconds(100L);

		BackPressureStatsTrackerImpl tracker = new BackPressureStatsTrackerImpl(
				sampleCoordinator, 9999, numSamples, Integer.MAX_VALUE, delayBetweenSamples);

		// getOperatorBackPressureStats triggers stack trace sampling
		Assert.assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		Mockito.verify(sampleCoordinator, Mockito.times(1)).triggerStackTraceSample(
				Matchers.eq(taskVertices),
				Matchers.eq(numSamples),
				Matchers.eq(delayBetweenSamples),
				Matchers.eq(BackPressureStatsTrackerImpl.MAX_STACK_TRACE_DEPTH));

		// Request back pressure stats again. This should not trigger another sample request
		Assert.assertTrue(!tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		Mockito.verify(sampleCoordinator, Mockito.times(1)).triggerStackTraceSample(
				Matchers.eq(taskVertices),
				Matchers.eq(numSamples),
				Matchers.eq(delayBetweenSamples),
				Matchers.eq(BackPressureStatsTrackerImpl.MAX_STACK_TRACE_DEPTH));

		Assert.assertTrue(!tracker.getOperatorBackPressureStats(jobVertex).isPresent());

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
		sampleFuture.complete(sample);

		Assert.assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		OperatorBackPressureStats stats = tracker.getOperatorBackPressureStats(jobVertex).get();

		// Verify the stats
		Assert.assertEquals(sampleId, stats.getSampleId());
		Assert.assertEquals(endTime, stats.getEndTimestamp());
		Assert.assertEquals(taskVertices.length, stats.getNumberOfSubTasks());

		for (int i = 0; i < taskVertices.length; i++) {
			double ratio = stats.getBackPressureRatio(i);
			// Traces until sub task index are back pressured
			Assert.assertEquals((i + 1) / ((double) 4), ratio, 0.0);
		}
	}

	private StackTraceElement[] createStackTrace(boolean isBackPressure) {
		if (isBackPressure) {
			return new StackTraceElement[] { new StackTraceElement(
					BackPressureStatsTrackerImpl.EXPECTED_CLASS_NAME,
					BackPressureStatsTrackerImpl.EXPECTED_METHOD_NAME,
					"LocalBufferPool.java",
					133) };
		} else {
			return Thread.currentThread().getStackTrace();
		}
	}

	private ExecutionVertex mockExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex) {

		Execution exec = Mockito.mock(Execution.class);
		Mockito.when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());

		JobVertexID id = jobVertex.getJobVertexId();

		ExecutionVertex vertex = Mockito.mock(ExecutionVertex.class);
		Mockito.when(vertex.getJobvertexId()).thenReturn(id);
		Mockito.when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		Mockito.when(vertex.getParallelSubtaskIndex()).thenReturn(subTaskIndex);

		return vertex;
	}

}
