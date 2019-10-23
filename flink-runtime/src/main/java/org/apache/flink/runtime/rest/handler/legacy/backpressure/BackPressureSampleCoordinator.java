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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.TaskBackPressureSampleResponse;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A coordinator for triggering and collecting back pressure stats
 * of running tasks.
 */
public class BackPressureSampleCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(BackPressureSampleCoordinator.class);

	private static final int NUM_GHOST_SAMPLE_IDS = 10;

	private final Object lock = new Object();

	/** Executor used to run the futures. */
	private final Executor executor;

	/** Time out after the expected sampling duration. */
	private final long sampleTimeout;

	/** In progress samples. */
	@GuardedBy("lock")
	private final Map<Integer, PendingTaskBackPressureStats> pendingSamples = new HashMap<>();

	/** A list of recent sample IDs to identify late messages vs. invalid ones. */
	private final ArrayDeque<Integer> recentPendingSamples = new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

	/** Sample ID counter. */
	@GuardedBy("lock")
	private int sampleIdCounter;

	/**
	 * Flag indicating whether the coordinator is still running.
	 */
	@GuardedBy("lock")
	private boolean isShutDown;

	/**
	 * Creates a new coordinator for the job.
	 *
	 * @param executor      Used to execute the futures.
	 * @param sampleTimeout Time out after the expected sampling duration.
	 *                      This is added to the expected duration of a
	 *                      sample, which is determined by the number of
	 *                      samples and the delay between each sample.
	 */
	public BackPressureSampleCoordinator(Executor executor, long sampleTimeout) {
		checkArgument(sampleTimeout >= 0L);
		this.executor = checkNotNull(executor);
		this.sampleTimeout = sampleTimeout;
	}

	/**
	 * Triggers a task back pressure stats sample to all tasks.
	 *
	 * @param tasksToSample       Tasks to sample.
	 * @param numSamples          Number of samples per task.
	 * @param delayBetweenSamples Delay between consecutive samples.
	 * @return A future of the completed task back pressure stats sample
	 */
	@SuppressWarnings("unchecked")
	CompletableFuture<BackPressureStats> triggerTaskBackPressureSample(
			ExecutionVertex[] tasksToSample,
			int numSamples,
			Time delayBetweenSamples) {

		checkNotNull(tasksToSample, "Tasks to sample should not be null");
		checkArgument(tasksToSample.length >= 1, "No tasks to sample");
		checkArgument(numSamples >= 1, "Illegal number of samples");

		// Execution IDs of running tasks
		ExecutionAttemptID[] triggerIds = new ExecutionAttemptID[tasksToSample.length];
		Execution[] executions = new Execution[tasksToSample.length];

		// Check that all tasks are RUNNING before triggering anything. The
		// triggering can still fail.
		for (int i = 0; i < triggerIds.length; i++) {
			Execution execution = tasksToSample[i].getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				executions[i] = execution;
				triggerIds[i] = execution.getAttemptId();
			} else {
				return FutureUtils.completedExceptionally(new IllegalStateException("Task " + tasksToSample[i]
					.getTaskNameWithSubtaskIndex() + " is not running."));
			}
		}

		synchronized (lock) {
			if (isShutDown) {
				return FutureUtils.completedExceptionally(new IllegalStateException("Shut down"));
			}

			final int sampleId = sampleIdCounter++;

			LOG.debug("Triggering task back pressure sample {}", sampleId);

			final PendingTaskBackPressureStats pending = new PendingTaskBackPressureStats(sampleId, triggerIds);

			// Discard the sample if it takes too long. We don't send cancel
			// messages to the task managers, but only wait for the responses
			// and then ignore them.
			long expectedDuration = numSamples * delayBetweenSamples.toMilliseconds();
			Time timeout = Time.milliseconds(expectedDuration + sampleTimeout);

			// Add the pending sample before scheduling the discard task to
			// prevent races with removing it again.
			pendingSamples.put(sampleId, pending);

			// Trigger all samples
			for (Execution execution: executions) {
				final CompletableFuture<TaskBackPressureSampleResponse> taskBackPressureFuture =
					execution.sampleTaskBackPressure(sampleId, numSamples, delayBetweenSamples, timeout);

				taskBackPressureFuture.handleAsync(
					(TaskBackPressureSampleResponse taskBackPressureSampleResponse, Throwable throwable) -> {
						if (taskBackPressureSampleResponse != null) {
							collectTaskBackPressureStat(
								taskBackPressureSampleResponse.getSampleId(),
								taskBackPressureSampleResponse.getExecutionAttemptID(),
								taskBackPressureSampleResponse.getBackPressureRatio());
						} else {
							cancelTaskBackPressureSample(sampleId, throwable);
						}

						return null;
					},
					executor);
			}

			return pending.getTaskBackPressureStatsFuture();
		}
	}

	/**
	 * Cancels a pending task back pressure sample.
	 *
	 * @param sampleId ID of the sample to cancel.
	 * @param cause    Cause of the cancelling (can be <code>null</code>).
	 */
	@VisibleForTesting
	void cancelTaskBackPressureSample(int sampleId, Throwable cause) {
		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			PendingTaskBackPressureStats sample = pendingSamples.remove(sampleId);
			if (sample != null) {
				if (cause != null) {
					LOG.info("Cancelling sample " + sampleId, cause);
				} else {
					LOG.info("Cancelling sample {}", sampleId);
				}

				sample.discard(cause);
				rememberRecentSampleId(sampleId);
			}
		}
	}

	/**
	 * Shuts down the coordinator.
	 *
	 * <p>After shut down, no further operations are executed.
	 */
	public void shutDown() {
		synchronized (lock) {
			if (!isShutDown) {
				LOG.info("Shutting down back pressure sample coordinator.");

				for (PendingTaskBackPressureStats pending : pendingSamples.values()) {
					pending.discard(new RuntimeException("Shut down."));
				}

				pendingSamples.clear();

				isShutDown = true;
			}
		}
	}

	/**
	 * Collects back pressure stat of a task.
	 *
	 * @param sampleId              ID of the sample.
	 * @param executionId           ID of the sampled task.
	 * @param taskBackPressureRatio Back pressure ratio of the sampled task.
	 *
	 * @throws IllegalStateException If unknown sample ID and not recently
	 *                               finished or cancelled sample.
	 */
	@VisibleForTesting
	void collectTaskBackPressureStat(
			int sampleId,
			ExecutionAttemptID executionId,
			double taskBackPressureRatio) {

		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Collecting back pressure sample {} of task {}", sampleId, executionId);
			}

			PendingTaskBackPressureStats pending = pendingSamples.get(sampleId);

			if (pending != null) {
				pending.collectBackPressureStats(executionId, taskBackPressureRatio);

				// Publish the sample
				if (pending.isComplete()) {
					pendingSamples.remove(sampleId);
					rememberRecentSampleId(sampleId);

					pending.completePromiseAndDiscard();
				}
			} else if (recentPendingSamples.contains(sampleId)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received late back pressure sample {} of task {}",
							sampleId, executionId);
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unknown sample ID " + sampleId);
				}
			}
		}
	}

	private void rememberRecentSampleId(int sampleId) {
		if (recentPendingSamples.size() >= NUM_GHOST_SAMPLE_IDS) {
			recentPendingSamples.removeFirst();
		}
		recentPendingSamples.addLast(sampleId);
	}

	@VisibleForTesting
	int getNumberOfPendingSamples() {
		synchronized (lock) {
			return pendingSamples.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending task back pressure stats, which collects task back pressure
	 * ratio and owns a {@link BackPressureStats} promise.
	 *
	 * <p>Access pending sample in lock scope.
	 */
	private static class PendingTaskBackPressureStats {

		private final int sampleId;
		private final long startTime;
		private final Set<ExecutionAttemptID> pendingTasks;
		private final Map<ExecutionAttemptID, Double> backPressureRatioByTask;
		private final CompletableFuture<BackPressureStats> taskBackPressureStatsFuture;

		private boolean isDiscarded;

		PendingTaskBackPressureStats(
				int sampleId,
				ExecutionAttemptID[] tasksToCollect) {

			this.sampleId = sampleId;
			this.startTime = System.currentTimeMillis();
			this.pendingTasks = new HashSet<>(Arrays.asList(tasksToCollect));
			this.backPressureRatioByTask = Maps.newHashMapWithExpectedSize(tasksToCollect.length);
			this.taskBackPressureStatsFuture = new CompletableFuture<>();
		}

		boolean isComplete() {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			return pendingTasks.isEmpty();
		}

		void discard(Throwable cause) {
			if (!isDiscarded) {
				pendingTasks.clear();
				backPressureRatioByTask.clear();

				taskBackPressureStatsFuture.completeExceptionally(new RuntimeException("Discarded", cause));

				isDiscarded = true;
			}
		}

		void collectBackPressureStats(ExecutionAttemptID executionId, double backPressureRatio) {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			if (pendingTasks.remove(executionId)) {
				backPressureRatioByTask.put(executionId, backPressureRatio);
			} else if (isComplete()) {
				throw new IllegalStateException("Completed");
			} else {
				throw new IllegalArgumentException("Unknown task " + executionId);
			}
		}

		void completePromiseAndDiscard() {
			if (isComplete()) {
				isDiscarded = true;

				long endTime = System.currentTimeMillis();

				BackPressureStats taskBackPressureStats = new BackPressureStats(
						sampleId,
						startTime,
						endTime,
						backPressureRatioByTask);

				taskBackPressureStatsFuture.complete(taskBackPressureStats);
			} else {
				throw new IllegalStateException("Not completed yet");
			}
		}

		CompletableFuture<BackPressureStats> getTaskBackPressureStatsFuture() {
			return taskBackPressureStatsFuture;
		}
	}
}
