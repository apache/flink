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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertexTest;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link BackPressureStatsTrackerImpl}.
 */
public class BackPressureStatsTrackerImplTest extends TestLogger {

	private static final int requestId = 0;
	private static final double backPressureRatio = 0.1;
	private static final ExecutionJobVertex executionJobVertex = createExecutionJobVertex();
	private static final ExecutionVertex[] taskVertices = executionJobVertex.getTaskVertices();
	private static final BackPressureStats backPressureStats = createBackPressureStats(requestId, 1, backPressureRatio);
	private static final int cleanUpInterval = 60000;
	private static final int backPressureStatsRefreshInterval = 60000;
	private static final long timeGap = 60000;

	@Rule
	public Timeout caseTimeout = new Timeout(10, TimeUnit.SECONDS);

	@Test
	public void testGetOperatorBackPressureStats() throws Exception {
		doInitialRequestAndVerifyResult(createBackPressureTracker());
	}

	@Test
	public void testCachedStatsNotUpdatedWithinRefreshInterval() throws Exception {
		final double backPressureRatio2 = 0.2;
		final int requestId2 = 1;
		final BackPressureStats backPressureStats2 = createBackPressureStats(requestId2, timeGap, backPressureRatio2);

		final BackPressureStatsTracker tracker = createBackPressureTracker(
			cleanUpInterval, backPressureStatsRefreshInterval, backPressureStats, backPressureStats2);
		doInitialRequestAndVerifyResult(tracker);
		// verify that no new back pressure request is triggered
		checkOperatorBackPressureStats(tracker.getOperatorBackPressureStats(executionJobVertex));
	}

	@Test
	public void testCachedStatsUpdatedAfterRefreshInterval() throws Exception {
		final int backPressureStatsRefreshInterval2 = 10;
		final long waitingTime = backPressureStatsRefreshInterval2 + 10;
		final double backPressureRatio2 = 0.2;
		final int requestId2 = 1;
		final BackPressureStats backPressureStats2 = createBackPressureStats(requestId2, timeGap, backPressureRatio2);

		final BackPressureStatsTracker tracker = createBackPressureTracker(
			cleanUpInterval, backPressureStatsRefreshInterval2, backPressureStats, backPressureStats2);
		doInitialRequestAndVerifyResult(tracker);

		// ensure that we are ready for next request
		Thread.sleep(waitingTime);

		// trigger next back pressure stats request and verify the result
		assertTrue(tracker.getOperatorBackPressureStats(executionJobVertex).isPresent());
		checkOperatorBackPressureStats(backPressureRatio2, backPressureStats2, tracker.getOperatorBackPressureStats(executionJobVertex));
	}

	@Test
	public void testShutDown() throws Exception {
		final BackPressureStatsTracker tracker = createBackPressureTracker();
		doInitialRequestAndVerifyResult(tracker);

		// shutdown directly
		tracker.shutDown();

		// verify that the previous cached result is invalid and trigger another request
		assertFalse(tracker.getOperatorBackPressureStats(executionJobVertex).isPresent());
		// verify no response after shutdown
		assertFalse(tracker.getOperatorBackPressureStats(executionJobVertex).isPresent());
	}

	@Test
	public void testCachedStatsNotCleanedWithinCleanupInterval() throws Exception {
		final BackPressureStatsTracker tracker = createBackPressureTracker();
		doInitialRequestAndVerifyResult(tracker);

		tracker.cleanUpOperatorStatsCache();
		// the back pressure stats should be still there
		checkOperatorBackPressureStats(tracker.getOperatorBackPressureStats(executionJobVertex));
	}

	@Test
	public void testCachedStatsCleanedAfterCleanupInterval() throws Exception {
		final int cleanUpInterval2 = 10;
		final long waitingTime = cleanUpInterval2 + 10;

		final BackPressureStatsTracker tracker = createBackPressureTracker(
			cleanUpInterval2, backPressureStatsRefreshInterval, backPressureStats);
		doInitialRequestAndVerifyResult(tracker);

		// wait until we are ready to cleanup
		Thread.sleep(waitingTime);

		// cleanup the cached back pressure stats
		tracker.cleanUpOperatorStatsCache();
		assertFalse(tracker.getOperatorBackPressureStats(executionJobVertex).isPresent());
	}

	private void doInitialRequestAndVerifyResult(BackPressureStatsTracker tracker) {
		// trigger back pressure stats request
		assertFalse(tracker.getOperatorBackPressureStats(executionJobVertex).isPresent());
		//  verify the result
		checkOperatorBackPressureStats(tracker.getOperatorBackPressureStats(executionJobVertex));
	}

	private void checkOperatorBackPressureStats(Optional<OperatorBackPressureStats> optionalStats) {
		checkOperatorBackPressureStats(backPressureRatio, backPressureStats, optionalStats);
	}

	private void checkOperatorBackPressureStats(
			double backPressureRatio,
			BackPressureStats backPressureStats,
			Optional<OperatorBackPressureStats> optionalStats) {
		assertTrue(optionalStats.isPresent());
		OperatorBackPressureStats stats = optionalStats.get();

		assertEquals(backPressureStats.getRequestId(), stats.getRequestId());
		assertEquals(backPressureStats.getEndTime(), stats.getEndTimestamp());
		assertEquals(taskVertices.length, stats.getNumberOfSubTasks());

		for (int i = 0; i < stats.getNumberOfSubTasks(); i++) {
			assertEquals(backPressureRatio, stats.getBackPressureRatio(i), 0.0);
		}
	}

	private BackPressureStatsTracker createBackPressureTracker() {
		return createBackPressureTracker(cleanUpInterval, backPressureStatsRefreshInterval, backPressureStats);
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

	private static BackPressureStats createBackPressureStats(
			int requestId,
			long timeGap,
			double backPressureRatio) {
		long startTime = System.currentTimeMillis();
		long endTime = startTime + timeGap;

		final Map<ExecutionAttemptID, Double> backPressureRatiosByTask = new HashMap<>();
		for (ExecutionVertex vertex : taskVertices) {
			backPressureRatiosByTask.put(vertex.getCurrentExecutionAttempt().getAttemptId(), backPressureRatio);
		}

		return new BackPressureStats(requestId, startTime, endTime, backPressureRatiosByTask);
	}

	private static ExecutionJobVertex createExecutionJobVertex() {
		try {
			return ExecutionJobVertexTest.createExecutionJobVertex(4, 4);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create ExecutionJobVertex.");
		}
	}

	/**
	 * A {@link BackPressureRequestCoordinator} which returns the pre-generated back pressure stats directly.
	 */
	private static class TestingBackPressureRequestCoordinator extends BackPressureRequestCoordinator {

		private final BackPressureStats[] backPressureStats;
		private int counter = 0;

		TestingBackPressureRequestCoordinator(
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
