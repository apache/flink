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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Back pressure statistics tracker. See {@link org.apache.flink.runtime.taskexecutor.BackPressureSampleService}
 * for more details about how back pressure ratio of a task is calculated.
 */
public class BackPressureStatsTrackerImpl implements BackPressureStatsTracker {

	private static final Logger LOG = LoggerFactory.getLogger(BackPressureStatsTrackerImpl.class);

	/** Lock guarding trigger operations. */
	private final Object lock = new Object();

	/** Coordinator for back pressure stats request. */
	private final BackPressureRequestCoordinator coordinator;

	/**
	 * Completed stats. Important: Job vertex IDs need to be scoped by job ID,
	 * because they are potentially constant across runs which may mess up the
	 * cached data.
	 */
	private final Cache<ExecutionJobVertex, OperatorBackPressureStats> operatorStatsCache;

	/**
	 * Pending in progress stats. Important: Job vertex IDs need to be scoped
	 * by job ID, because they are potentially constant across runs which may
	 * mess up the cached data.
	 */
	private final Set<ExecutionJobVertex> pendingStats = new HashSet<>();

	/**
	 * Time interval, in milliseconds, after which the available back pressure
	 * stats are deprecated and need to be refreshed.
	 */
	private final int backPressureStatsRefreshInterval;

	/** Flag indicating whether the stats tracker has been shut down. */
	@GuardedBy("lock")
	private boolean shutDown;

	/**
	 * Creates a back pressure statistics tracker.
	 *
	 * @param coordinator Coordinator for back pressure stats request.
	 * @param cleanUpInterval Clean up interval for completed stats.
	 * @param refreshInterval Time interval after which the available back pressure
	 *                        stats are deprecated and need to be refreshed.
	 */
	public BackPressureStatsTrackerImpl(
			BackPressureRequestCoordinator coordinator,
			int cleanUpInterval,
			int refreshInterval) {
		checkArgument(cleanUpInterval >= 0, "The cleanup interval must be non-negative.");
		checkArgument(refreshInterval >= 0, "The back pressure stats refresh interval must be non-negative.");

		this.coordinator = checkNotNull(coordinator);
		this.backPressureStatsRefreshInterval = refreshInterval;
		this.operatorStatsCache = CacheBuilder.newBuilder()
				.concurrencyLevel(1)
				.expireAfterAccess(cleanUpInterval, TimeUnit.MILLISECONDS)
				.build();
	}

	/**
	 * Returns back pressure statistics for a operator. Automatically triggers task back pressure
	 * sampling if statistics are not available or outdated.
	 *
	 * @param vertex Operator to get the stats for.
	 * @return Back pressure statistics for an operator
	 */
	public Optional<OperatorBackPressureStats> getOperatorBackPressureStats(ExecutionJobVertex vertex) {
		synchronized (lock) {
			final OperatorBackPressureStats stats = operatorStatsCache.getIfPresent(vertex);
			if (stats == null || backPressureStatsRefreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
				triggerBackPressureRequestInternal(vertex);
			}
			return Optional.ofNullable(stats);
		}
	}

	/**
	 * Triggers a back pressure request for a vertex to gather the back pressure
	 * statistics. If there is a request in progress for the vertex, the call
	 * is ignored.
	 *
	 * @param vertex Vertex to get the stats for.
	 */
	private void triggerBackPressureRequestInternal(final ExecutionJobVertex vertex) {
		assert(Thread.holdsLock(lock));

		if (shutDown) {
			return;
		}

		if (!pendingStats.contains(vertex) && !vertex.getGraph().getState().isGloballyTerminalState()) {

			Executor executor = vertex.getGraph().getFutureExecutor();

			// Only trigger for still active job
			if (executor != null) {
				pendingStats.add(vertex);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Triggering back pressure request for tasks: " + Arrays.toString(vertex.getTaskVertices()));
				}

				CompletableFuture<BackPressureStats> statsFuture =
					coordinator.triggerBackPressureRequest(vertex.getTaskVertices());

				statsFuture.handleAsync(new BackPressureRequestCompletionCallback(vertex), executor);
			}
		}
	}

	/**
	 * Cleans up the operator stats cache if it contains timed out entries.
	 *
	 * <p>The Guava cache only evicts as maintenance during normal operations.
	 * If this handler is inactive, it will never be cleaned.
	 */
	public void cleanUpOperatorStatsCache() {
		operatorStatsCache.cleanUp();
	}

	/**
	 * Shuts down the stats tracker.
	 *
	 * <p>Invalidates the cache and clears all pending stats.
	 */
	public void shutDown() {
		synchronized (lock) {
			if (!shutDown) {
				operatorStatsCache.invalidateAll();
				pendingStats.clear();

				shutDown = true;
			}
		}
	}

	/**
	 * Callback on completed back pressure request.
	 */
	private class BackPressureRequestCompletionCallback implements BiFunction<BackPressureStats, Throwable, Void> {

		private final ExecutionJobVertex vertex;

		BackPressureRequestCompletionCallback(ExecutionJobVertex vertex) {
			this.vertex = vertex;
		}

		@Override
		public Void apply(BackPressureStats backPressureStats, Throwable throwable) {
			synchronized (lock) {
				try {
					if (shutDown) {
						return null;
					}

					// Job finished, ignore.
					JobStatus jobState = vertex.getGraph().getState();
					if (jobState.isGloballyTerminalState()) {
						LOG.debug("Ignoring stats, because job is in state " + jobState + ".");
					} else if (backPressureStats != null) {
						OperatorBackPressureStats stats = createOperatorBackPressureStats(backPressureStats);
						operatorStatsCache.put(vertex, stats);
					} else {
						LOG.debug("Failed to gather back pressure stats.", throwable);
					}
				} catch (Throwable t) {
					LOG.error("Error during stats completion.", t);
				} finally {
					pendingStats.remove(vertex);
				}

				return null;
			}
		}

		/**
		 * Creates {@link OperatorBackPressureStats} from {@link BackPressureStats}.
		 */
		private OperatorBackPressureStats createOperatorBackPressureStats(BackPressureStats stats) {
			Map<ExecutionAttemptID, Double> backPressureRatiosByTask = stats.getBackPressureRatios();

			// Map task ID to subtask index, because the web interface expects
			// it like that.
			Map<ExecutionAttemptID, Integer> subtaskIndexMap = Maps
					.newHashMapWithExpectedSize(backPressureRatiosByTask.size());

			Set<ExecutionAttemptID> tasks = backPressureRatiosByTask.keySet();

			for (ExecutionVertex task : vertex.getTaskVertices()) {
				ExecutionAttemptID taskId = task.getCurrentExecutionAttempt().getAttemptId();
				if (tasks.contains(taskId)) {
					subtaskIndexMap.put(taskId, task.getParallelSubtaskIndex());
				} else {
					LOG.debug("Outdated stats. A task, which is part of the " +
							"request has been reset.");
				}
			}

			// Back pressure ratios of all tasks. Array position corresponds
			// to sub task index.
			double[] backPressureRatios = new double[backPressureRatiosByTask.size()];

			for (Entry<ExecutionAttemptID, Double> entry : backPressureRatiosByTask.entrySet()) {
				int subtaskIndex = subtaskIndexMap.get(entry.getKey());
				backPressureRatios[subtaskIndex] = entry.getValue();
			}

			return new OperatorBackPressureStats(
					stats.getRequestId(),
					stats.getEndTime(),
					backPressureRatios);
		}
	}
}
