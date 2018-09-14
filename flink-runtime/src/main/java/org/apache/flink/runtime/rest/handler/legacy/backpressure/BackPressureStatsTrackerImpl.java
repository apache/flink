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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
 * Back pressure statistics tracker.
 *
 * <p>Back pressure is determined by sampling running tasks. If a task is
 * slowed down by back pressure it will be stuck in memory requests to a
 * {@link org.apache.flink.runtime.io.network.buffer.LocalBufferPool}.
 *
 * <p>The back pressured stack traces look like this:
 *
 * <pre>
 * java.lang.Object.wait(Native Method)
 * o.a.f.[...].LocalBufferPool.requestBuffer(LocalBufferPool.java:163)
 * o.a.f.[...].LocalBufferPool.requestBufferBlocking(LocalBufferPool.java:133) <--- BLOCKING
 * request
 * [...]
 * </pre>
 */
public class BackPressureStatsTrackerImpl implements BackPressureStatsTracker {

	private static final Logger LOG = LoggerFactory.getLogger(BackPressureStatsTrackerImpl.class);

	/** Maximum stack trace depth for samples. */
	static final int MAX_STACK_TRACE_DEPTH = 3;

	/** Expected class name for back pressure indicating stack trace element. */
	static final String EXPECTED_CLASS_NAME = "org.apache.flink.runtime.io.network.buffer.LocalBufferPool";

	/** Expected method name for back pressure indicating stack trace element. */
	static final String EXPECTED_METHOD_NAME = "requestBufferBuilderBlocking";

	/** Lock guarding trigger operations. */
	private final Object lock = new Object();

	/* Stack trace sample coordinator. */
	private final StackTraceSampleCoordinator coordinator;

	/**
	 * Completed stats. Important: Job vertex IDs need to be scoped by job ID,
	 * because they are potentially constant across runs messing up the cached
	 * data.
	 */
	private final Cache<ExecutionJobVertex, OperatorBackPressureStats> operatorStatsCache;

	/** Pending in progress stats. Important: Job vertex IDs need to be scoped
	 * by job ID, because they are potentially constant across runs messing up
	 * the cached data.*/
	private final Set<ExecutionJobVertex> pendingStats = new HashSet<>();

	/** Cleanup interval for completed stats cache. */
	private final int cleanUpInterval;

	private final int numSamples;

	private final int backPressureStatsRefreshInterval;

	private final Time delayBetweenSamples;

	/** Flag indicating whether the stats tracker has been shut down. */
	private boolean shutDown;

	/**
	 * Creates a back pressure statistics tracker.
	 *
	 * @param cleanUpInterval     Clean up interval for completed stats.
	 * @param numSamples          Number of stack trace samples when determining back pressure.
	 * @param delayBetweenSamples Delay between samples when determining back pressure.
	 */
	public BackPressureStatsTrackerImpl(
			StackTraceSampleCoordinator coordinator,
			int cleanUpInterval,
			int numSamples,
			int backPressureStatsRefreshInterval,
			Time delayBetweenSamples) {

		this.coordinator = checkNotNull(coordinator, "Stack trace sample coordinator");

		checkArgument(cleanUpInterval >= 0, "Clean up interval");
		this.cleanUpInterval = cleanUpInterval;

		checkArgument(numSamples >= 1, "Number of samples");
		this.numSamples = numSamples;

		checkArgument(
			backPressureStatsRefreshInterval >= 0,
			"backPressureStatsRefreshInterval must be greater than or equal to 0");
		this.backPressureStatsRefreshInterval = backPressureStatsRefreshInterval;

		this.delayBetweenSamples = checkNotNull(delayBetweenSamples, "Delay between samples");

		this.operatorStatsCache = CacheBuilder.newBuilder()
				.concurrencyLevel(1)
				.expireAfterAccess(cleanUpInterval, TimeUnit.MILLISECONDS)
				.build();
	}

	/** Cleanup interval for completed stats cache. */
	public long getCleanUpInterval() {
		return cleanUpInterval;
	}

	/**
	 * Returns back pressure statistics for a operator. Automatically triggers stack trace sampling
	 * if statistics are not available or outdated.
	 *
	 * @param vertex Operator to get the stats for.
	 * @return Back pressure statistics for an operator
	 */
	public Optional<OperatorBackPressureStats> getOperatorBackPressureStats(ExecutionJobVertex vertex) {
		synchronized (lock) {
			final OperatorBackPressureStats stats = operatorStatsCache.getIfPresent(vertex);
			if (stats == null || backPressureStatsRefreshInterval <= System.currentTimeMillis() - stats.getEndTimestamp()) {
				triggerStackTraceSampleInternal(vertex);
			}
			return Optional.ofNullable(stats);
		}
	}

	/**
	 * Triggers a stack trace sample for a operator to gather the back pressure
	 * statistics. If there is a sample in progress for the operator, the call
	 * is ignored.
	 *
	 * @param vertex Operator to get the stats for.
	 * @return Flag indicating whether a sample with triggered.
	 */
	private boolean triggerStackTraceSampleInternal(final ExecutionJobVertex vertex) {
		assert(Thread.holdsLock(lock));

		if (shutDown) {
			return false;
		}

		if (!pendingStats.contains(vertex) &&
			!vertex.getGraph().getState().isGloballyTerminalState()) {

			Executor executor = vertex.getGraph().getFutureExecutor();

			// Only trigger if still active job
			if (executor != null) {
				pendingStats.add(vertex);

				if (LOG.isDebugEnabled()) {
					LOG.debug("Triggering stack trace sample for tasks: " + Arrays.toString(vertex.getTaskVertices()));
				}

				CompletableFuture<StackTraceSample> sample = coordinator.triggerStackTraceSample(
					vertex.getTaskVertices(),
					numSamples,
					delayBetweenSamples,
					MAX_STACK_TRACE_DEPTH);

				sample.handleAsync(new StackTraceSampleCompletionCallback(vertex), executor);

				return true;
			}
		}

		return false;
	}

	/**
	 * Triggers a stack trace sample for a operator to gather the back pressure
	 * statistics. If there is a sample in progress for the operator, the call
	 * is ignored.
	 *
	 * @param vertex Operator to get the stats for.
	 * @return Flag indicating whether a sample with triggered.
	 * @deprecated {@link #getOperatorBackPressureStats(ExecutionJobVertex)} will trigger
	 * stack trace sampling automatically.
	 */
	@Deprecated
	public boolean triggerStackTraceSample(ExecutionJobVertex vertex) {
		synchronized (lock) {
			return triggerStackTraceSampleInternal(vertex);
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
	 * Invalidates the cache (irrespective of clean up interval).
	 */
	void invalidateOperatorStatsCache() {
		operatorStatsCache.invalidateAll();
	}

	/**
	 * Callback on completed stack trace sample.
	 */
	class StackTraceSampleCompletionCallback implements BiFunction<StackTraceSample, Throwable, Void> {

		private final ExecutionJobVertex vertex;

		public StackTraceSampleCompletionCallback(ExecutionJobVertex vertex) {
			this.vertex = vertex;
		}

		@Override
		public Void apply(StackTraceSample stackTraceSample, Throwable throwable) {
			synchronized (lock) {
				try {
					if (shutDown) {
						return null;
					}

					// Job finished, ignore.
					JobStatus jobState = vertex.getGraph().getState();
					if (jobState.isGloballyTerminalState()) {
						LOG.debug("Ignoring sample, because job is in state " + jobState + ".");
					} else if (stackTraceSample != null) {
						OperatorBackPressureStats stats = createStatsFromSample(stackTraceSample);
						operatorStatsCache.put(vertex, stats);
					} else {
						LOG.debug("Failed to gather stack trace sample.", throwable);
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
		 * Creates the back pressure stats from a stack trace sample.
		 *
		 * @param sample Stack trace sample to base stats on.
		 *
		 * @return Back pressure stats
		 */
		private OperatorBackPressureStats createStatsFromSample(StackTraceSample sample) {
			Map<ExecutionAttemptID, List<StackTraceElement[]>> traces = sample.getStackTraces();

			// Map task ID to subtask index, because the web interface expects
			// it like that.
			Map<ExecutionAttemptID, Integer> subtaskIndexMap = Maps
					.newHashMapWithExpectedSize(traces.size());

			Set<ExecutionAttemptID> sampledTasks = sample.getStackTraces().keySet();

			for (ExecutionVertex task : vertex.getTaskVertices()) {
				ExecutionAttemptID taskId = task.getCurrentExecutionAttempt().getAttemptId();
				if (sampledTasks.contains(taskId)) {
					subtaskIndexMap.put(taskId, task.getParallelSubtaskIndex());
				} else {
					LOG.debug("Outdated sample. A task, which is part of the " +
							"sample has been reset.");
				}
			}

			// Ratio of blocked samples to total samples per sub task. Array
			// position corresponds to sub task index.
			double[] backPressureRatio = new double[traces.size()];

			for (Entry<ExecutionAttemptID, List<StackTraceElement[]>> entry : traces.entrySet()) {
				int backPressureSamples = 0;

				List<StackTraceElement[]> taskTraces = entry.getValue();

				for (StackTraceElement[] trace : taskTraces) {
					for (int i = trace.length - 1; i >= 0; i--) {
						StackTraceElement elem = trace[i];

						if (elem.getClassName().equals(EXPECTED_CLASS_NAME) &&
								elem.getMethodName().equals(EXPECTED_METHOD_NAME)) {

							backPressureSamples++;
							break; // Continue with next stack trace
						}
					}
				}

				int subtaskIndex = subtaskIndexMap.get(entry.getKey());

				int size = taskTraces.size();
				double ratio = (size > 0)
						? ((double) backPressureSamples) / size
						: 0;

				backPressureRatio[subtaskIndex] = ratio;
			}

			return new OperatorBackPressureStats(
					sample.getSampleId(),
					sample.getEndTime(),
					backPressureRatio);
		}
	}
}
