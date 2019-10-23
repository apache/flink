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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link BackPressureStatsTrackerImpl}.
 */
public class BackPressureStatsTrackerImplTest extends TestLogger {

	@Test
	public void testGetOperatorBackPressureStats() throws Exception {
		final ExecutionJobVertex jobVertex = createExecutionJobVertex(JobStatus.RUNNING);

		final int sampleId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			jobVertex.getTaskVertices(), sampleId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(
			600000, 10000, Time.milliseconds(50), backPressureStats);

		// trigger back pressure stats sampling
		tracker.getOperatorBackPressureStats(jobVertex);

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(jobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), sampleId, endTime, backPressureRatio, stats);
	}

	@Test
	public void testOperatorBackPressureStatsUpdate() throws Exception {
		final ExecutionJobVertex jobVertex = createExecutionJobVertex(JobStatus.RUNNING);
		final int backPressureStatsRefreshInterval = 2000;
		final long waitingTime = backPressureStatsRefreshInterval + 500;

		final int sampleId1 = 0;
		final long startTime1 = System.currentTimeMillis();
		final long endTime1 = startTime1 + 1;
		final double backPressureRatio1 = 0.1;

		final int sampleId2 = 1;
		final long startTime2 = System.currentTimeMillis() + waitingTime;
		final long endTime2 = startTime2 + 1;
		final double backPressureRatio2 = 0.2;

		final BackPressureStats backPressureStats1 = createBackPressureStats(
			jobVertex.getTaskVertices(), sampleId1, startTime1, endTime1, backPressureRatio1);
		final BackPressureStats backPressureStats2 = createBackPressureStats(
			jobVertex.getTaskVertices(), sampleId2, startTime2, endTime2, backPressureRatio2);
		final BackPressureStatsTracker tracker = createBackPressureTracker(
			600000, backPressureStatsRefreshInterval, Time.milliseconds(50), backPressureStats1, backPressureStats2);

		// trigger back pressure stats sampling
		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(jobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		final int sampleId = stats.getSampleId();
		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), sampleId1, endTime1, backPressureRatio1, stats);

		// should not trigger new back pressure stats sampling
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
		assertEquals(sampleId, tracker.getOperatorBackPressureStats(jobVertex).get().getSampleId());

		// ensure that we are ready for next sampling
		Thread.sleep(waitingTime);

		// trigger next back pressure stats sampling
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
		OperatorBackPressureStats newStats = tracker.getOperatorBackPressureStats(jobVertex).get();
		assertNotEquals(sampleId, newStats.getSampleId());

		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), sampleId2, endTime2, backPressureRatio2, newStats);
	}

	@Test
	public void testGetOperatorBackPressureStatsAfterShutDown() throws Exception {
		final ExecutionJobVertex jobVertex = createExecutionJobVertex(JobStatus.RUNNING);

		final int sampleId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			jobVertex.getTaskVertices(), sampleId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(
			600000, 10000, Time.milliseconds(50), backPressureStats);

		tracker.shutDown();

		// trigger back pressure stats sampling
		tracker.getOperatorBackPressureStats(jobVertex);

		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
	}

	@Test
	public void testStatsCleanup() throws Exception {
		final ExecutionJobVertex jobVertex = createExecutionJobVertex(JobStatus.RUNNING);
		final int cleanUpInterval = 2000;
		final long waitingTime = cleanUpInterval + 500;

		final int sampleId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			jobVertex.getTaskVertices(), sampleId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(
			2000, 10000, Time.milliseconds(50), backPressureStats);

		// trigger back pressure stats sampling
		tracker.getOperatorBackPressureStats(jobVertex);

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(jobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), sampleId, endTime, backPressureRatio, stats);

		tracker.cleanUpOperatorStatsCache();
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		// wait until we are ready to cleanup
		Thread.sleep(waitingTime);

		tracker.cleanUpOperatorStatsCache();
		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
	}

	private void checkOperatorBackPressureStats(
			ExecutionVertex[] taskVertices,
			int sampleId,
			long endTime,
			double backPressureRatio,
			OperatorBackPressureStats stats) {

		assertEquals(sampleId, stats.getSampleId());
		assertEquals(endTime, stats.getEndTimestamp());
		assertEquals(taskVertices.length, stats.getNumberOfSubTasks());
		for (int i = 0; i < stats.getNumberOfSubTasks(); i++) {
			assertEquals(backPressureRatio, stats.getBackPressureRatio(i), 0.0);
		}
	}

	private BackPressureStatsTracker createBackPressureTracker(
			int cleanUpInterval,
			int backPressureStatsRefreshInterval,
			Time delayBetweenSamples,
			BackPressureStats... stats) {

		final BackPressureSampleCoordinator coordinator = createBackPressureSampleCoordinator(stats);
		return new BackPressureStatsTrackerImpl(
				coordinator,
				cleanUpInterval,
				WebOptions.BACKPRESSURE_NUM_SAMPLES.defaultValue(),
				backPressureStatsRefreshInterval,
				delayBetweenSamples);
	}

	private ExecutionVertex createExecutionVertex(ExecutionJobVertex jobVertex, int subTaskIndex) {

		Execution exec = mock(Execution.class);
		when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());

		JobVertexID id = jobVertex.getJobVertexId();

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getJobvertexId()).thenReturn(id);
		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);
		when(vertex.getParallelSubtaskIndex()).thenReturn(subTaskIndex);

		return vertex;
	}

	private ExecutionJobVertex createExecutionJobVertex(JobStatus jobStatus) {
		ExecutionGraph graph = mock(ExecutionGraph.class);
		when(graph.getState()).thenReturn(jobStatus);
		when(graph.getFutureExecutor()).thenReturn(Runnable::run);

		ExecutionVertex[] taskVertices = new ExecutionVertex[4];

		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getJobId()).thenReturn(new JobID());
		when(jobVertex.getJobVertexId()).thenReturn(new JobVertexID());
		when(jobVertex.getGraph()).thenReturn(graph);
		when(jobVertex.getTaskVertices()).thenReturn(taskVertices);

		taskVertices[0] = createExecutionVertex(jobVertex, 0);
		taskVertices[1] = createExecutionVertex(jobVertex, 1);
		taskVertices[2] = createExecutionVertex(jobVertex, 2);
		taskVertices[3] = createExecutionVertex(jobVertex, 3);

		return jobVertex;
	}

	private BackPressureSampleCoordinator createBackPressureSampleCoordinator(BackPressureStats... stats) {

		BackPressureSampleCoordinator sampleCoordinator = mock(BackPressureSampleCoordinator.class);

		OngoingStubbing<CompletableFuture<BackPressureStats>> stubbing = when(
			sampleCoordinator.triggerTaskBackPressureSample(
				ArgumentMatchers.any(ExecutionVertex[].class),
				ArgumentMatchers.anyInt(),
				ArgumentMatchers.any(Time.class)));

		for (BackPressureStats backPressureStats: stats) {
			stubbing = stubbing.thenReturn(CompletableFuture.completedFuture(backPressureStats));
		}

		return sampleCoordinator;
	}

	private BackPressureStats createBackPressureStats(
			ExecutionVertex[] taskVertices, int sampleId, long startTime, long endTime, double backPressureRatio) {

		final Map<ExecutionAttemptID, Double> backPressureRatioByTask = new HashMap<>();
		for (ExecutionVertex vertex : taskVertices) {
			backPressureRatioByTask.put(vertex.getCurrentExecutionAttempt().getAttemptId(), backPressureRatio);
		}

		return new BackPressureStats(sampleId, startTime, endTime, backPressureRatioByTask);
	}
}
