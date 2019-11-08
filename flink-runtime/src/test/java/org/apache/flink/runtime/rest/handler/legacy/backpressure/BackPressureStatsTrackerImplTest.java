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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link BackPressureStatsTrackerImpl}.
 */
public class BackPressureStatsTrackerImplTest extends TestLogger {

	@Test
	public void testGetOperatorBackPressureStats() throws Exception {
		final ExecutionJobVertex executionJobVertex = BackPressureTrackerTestUtils.createExecutionJobVertex();
		final ExecutionVertex[] taskVertices = executionJobVertex.getTaskVertices();

		final int requestId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			taskVertices, requestId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(600000, 10000, backPressureStats);

		// trigger back pressure stats request
		tracker.getOperatorBackPressureStats(executionJobVertex);

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(executionJobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		checkOperatorBackPressureStats(taskVertices, requestId, endTime, backPressureRatio, stats);
	}

	@Test
	public void testOperatorBackPressureStatsUpdate() throws Exception {
		final ExecutionJobVertex jobVertex = BackPressureTrackerTestUtils.createExecutionJobVertex();
		final int backPressureStatsRefreshInterval = 2000;
		final long waitingTime = backPressureStatsRefreshInterval + 500;

		final int requestId1 = 0;
		final long startTime1 = System.currentTimeMillis();
		final long endTime1 = startTime1 + 1;
		final double backPressureRatio1 = 0.1;

		final int requestId2 = 1;
		final long startTime2 = System.currentTimeMillis() + waitingTime;
		final long endTime2 = startTime2 + 1;
		final double backPressureRatio2 = 0.2;

		final BackPressureStats backPressureStats1 = createBackPressureStats(
			jobVertex.getTaskVertices(), requestId1, startTime1, endTime1, backPressureRatio1);
		final BackPressureStats backPressureStats2 = createBackPressureStats(
			jobVertex.getTaskVertices(), requestId2, startTime2, endTime2, backPressureRatio2);
		final BackPressureStatsTracker tracker = createBackPressureTracker(
			600000, backPressureStatsRefreshInterval, backPressureStats1, backPressureStats2);

		// trigger back pressure stats request
		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(jobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		final int requestId = stats.getRequestId();
		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), requestId1, endTime1, backPressureRatio1, stats);

		// should not trigger new back pressure stats request
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
		assertEquals(requestId, tracker.getOperatorBackPressureStats(jobVertex).get().getRequestId());

		// ensure that we are ready for next request
		Thread.sleep(waitingTime);

		// trigger next back pressure stats request
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
		OperatorBackPressureStats newStats = tracker.getOperatorBackPressureStats(jobVertex).get();
		assertNotEquals(requestId, newStats.getRequestId());

		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), requestId2, endTime2, backPressureRatio2, newStats);
	}

	@Test
	public void testGetOperatorBackPressureStatsAfterShutDown() throws Exception {
		final ExecutionJobVertex jobVertex = BackPressureTrackerTestUtils.createExecutionJobVertex();

		final int requestId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			jobVertex.getTaskVertices(), requestId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(600000, 10000, backPressureStats);

		tracker.shutDown();

		// trigger back pressure stats request
		tracker.getOperatorBackPressureStats(jobVertex);

		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
	}

	@Test
	public void testStatsCleanup() throws Exception {
		final ExecutionJobVertex jobVertex = BackPressureTrackerTestUtils.createExecutionJobVertex();
		final int cleanUpInterval = 2000;
		final long waitingTime = cleanUpInterval + 500;

		final int requestId = 0;
		final long startTime = System.currentTimeMillis();
		final long endTime = startTime + 1;
		final double backPressureRatio = 0.1;

		final BackPressureStats backPressureStats = createBackPressureStats(
			jobVertex.getTaskVertices(), requestId, startTime, endTime, backPressureRatio);
		final BackPressureStatsTracker tracker = createBackPressureTracker(2000, 10000, backPressureStats);

		// trigger back pressure stats request
		tracker.getOperatorBackPressureStats(jobVertex);

		Optional<OperatorBackPressureStats> optionalStats = tracker.getOperatorBackPressureStats(jobVertex);
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		checkOperatorBackPressureStats(jobVertex.getTaskVertices(), requestId, endTime, backPressureRatio, stats);

		tracker.cleanUpOperatorStatsCache();
		assertTrue(tracker.getOperatorBackPressureStats(jobVertex).isPresent());

		// wait until we are ready to cleanup
		Thread.sleep(waitingTime);

		tracker.cleanUpOperatorStatsCache();
		assertFalse(tracker.getOperatorBackPressureStats(jobVertex).isPresent());
	}

	private void checkOperatorBackPressureStats(
			ExecutionVertex[] taskVertices,
			int requestId,
			long endTime,
			double backPressureRatio,
			OperatorBackPressureStats stats) {

		assertEquals(requestId, stats.getRequestId());
		assertEquals(endTime, stats.getEndTimestamp());
		assertEquals(taskVertices.length, stats.getNumberOfSubTasks());
		for (int i = 0; i < stats.getNumberOfSubTasks(); i++) {
			assertEquals(backPressureRatio, stats.getBackPressureRatio(i), 0.0);
		}
	}

	private BackPressureStatsTracker createBackPressureTracker(
			int cleanUpInterval,
			int backPressureStatsRefreshInterval,
			BackPressureStats... stats) {

		final BackPressureRequestCoordinator coordinator =
			new TestingBackPressureRequestCoordinator(Runnable::run, 10000, stats);
		return new BackPressureStatsTrackerImpl(
				coordinator,
				cleanUpInterval,
				backPressureStatsRefreshInterval);
	}

	private BackPressureStats createBackPressureStats(
			ExecutionVertex[] taskVertices,
			int requestId,
			long startTime,
			long endTime,
			double backPressureRatio) {

		final Map<ExecutionAttemptID, Double> backPressureRatiosByTask = new HashMap<>();
		for (ExecutionVertex vertex : taskVertices) {
			backPressureRatiosByTask.put(vertex.getCurrentExecutionAttempt().getAttemptId(), backPressureRatio);
		}

		return new BackPressureStats(requestId, startTime, endTime, backPressureRatiosByTask);
	}

	/**
	 * A {@link BackPressureRequestCoordinator} which returns the pre-generated back pressure stats directly.
	 */
	public static class TestingBackPressureRequestCoordinator extends BackPressureRequestCoordinator {

		private final BackPressureStats[] backPressureStats;
		private int counter = 0;

		public TestingBackPressureRequestCoordinator(
			Executor executor,
			long requestTimeout,
			BackPressureStats... backPressureStats) {
			super(executor, requestTimeout);
			this.backPressureStats = backPressureStats;
		}

		@Override
		CompletableFuture<BackPressureStats> triggerBackPressureRequest(ExecutionVertex[] tasks) {
			return CompletableFuture.completedFuture(backPressureStats[(counter++) % backPressureStats.length]);
		}
	}
}
