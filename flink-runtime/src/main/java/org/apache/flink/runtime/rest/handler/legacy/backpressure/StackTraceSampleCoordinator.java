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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A coordinator for triggering and collecting stack traces of running tasks.
 */
public class StackTraceSampleCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(StackTraceSampleCoordinator.class);

	private static final int NUM_GHOST_SAMPLE_IDS = 10;

	private final Object lock = new Object();

	/** Executor used to run the futures. */
	private final Executor executor;

	/** Time out after the expected sampling duration. */
	private final long sampleTimeout;

	/** In progress samples (guarded by lock). */
	private final Map<Integer, PendingStackTraceSample> pendingSamples = new HashMap<>();

	/** A list of recent sample IDs to identify late messages vs. invalid ones. */
	private final ArrayDeque<Integer> recentPendingSamples = new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

	/** Sample ID counter (guarded by lock). */
	private int sampleIdCounter;

	/**
	 * Flag indicating whether the coordinator is still running (guarded by
	 * lock).
	 */
	private boolean isShutDown;

	/**
	 * Creates a new coordinator for the job.
	 *
	 * @param executor to use to execute the futures
	 * @param sampleTimeout Time out after the expected sampling duration.
	 *                      This is added to the expected duration of a
	 *                      sample, which is determined by the number of
	 *                      samples and the delay between each sample.
	 */
	public StackTraceSampleCoordinator(Executor executor, long sampleTimeout) {
		checkArgument(sampleTimeout >= 0L);
		this.executor = Preconditions.checkNotNull(executor);
		this.sampleTimeout = sampleTimeout;
	}

	/**
	 * Triggers a stack trace sample to all tasks.
	 *
	 * @param tasksToSample       Tasks to sample.
	 * @param numSamples          Number of stack trace samples to collect.
	 * @param delayBetweenSamples Delay between consecutive samples.
	 * @param maxStackTraceDepth  Maximum depth of the stack trace. 0 indicates
	 *                            no maximum and keeps the complete stack trace.
	 * @return A future of the completed stack trace sample
	 */
	@SuppressWarnings("unchecked")
	public CompletableFuture<StackTraceSample> triggerStackTraceSample(
			ExecutionVertex[] tasksToSample,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth) {

		checkNotNull(tasksToSample, "Tasks to sample");
		checkArgument(tasksToSample.length >= 1, "No tasks to sample");
		checkArgument(numSamples >= 1, "No number of samples");
		checkArgument(maxStackTraceDepth >= 0, "Negative maximum stack trace depth");

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

			LOG.debug("Triggering stack trace sample {}", sampleId);

			final PendingStackTraceSample pending = new PendingStackTraceSample(
					sampleId, triggerIds);

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
				final CompletableFuture<StackTraceSampleResponse> stackTraceSampleFuture = execution.requestStackTraceSample(
					sampleId,
					numSamples,
					delayBetweenSamples,
					maxStackTraceDepth,
					timeout);

				stackTraceSampleFuture.handleAsync(
					(StackTraceSampleResponse stackTraceSampleResponse, Throwable throwable) -> {
						if (stackTraceSampleResponse != null) {
							collectStackTraces(
								stackTraceSampleResponse.getSampleId(),
								stackTraceSampleResponse.getExecutionAttemptID(),
								stackTraceSampleResponse.getSamples());
						} else {
							cancelStackTraceSample(sampleId, throwable);
						}

						return null;
					},
					executor);
			}

			return pending.getStackTraceSampleFuture();
		}
	}

	/**
	 * Cancels a pending sample.
	 *
	 * @param sampleId ID of the sample to cancel.
	 * @param cause Cause of the cancelling (can be <code>null</code>).
	 */
	public void cancelStackTraceSample(int sampleId, Throwable cause) {
		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			PendingStackTraceSample sample = pendingSamples.remove(sampleId);
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
				LOG.info("Shutting down stack trace sample coordinator.");

				for (PendingStackTraceSample pending : pendingSamples.values()) {
					pending.discard(new RuntimeException("Shut down"));
				}

				pendingSamples.clear();

				isShutDown = true;
			}
		}
	}

	/**
	 * Collects stack traces of a task.
	 *
	 * @param sampleId    ID of the sample.
	 * @param executionId ID of the sampled task.
	 * @param stackTraces Stack traces of the sampled task.
	 *
	 * @throws IllegalStateException If unknown sample ID and not recently
	 *                               finished or cancelled sample.
	 */
	public void collectStackTraces(
			int sampleId,
			ExecutionAttemptID executionId,
			List<StackTraceElement[]> stackTraces) {

		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Collecting stack trace sample {} of task {}", sampleId, executionId);
			}

			PendingStackTraceSample pending = pendingSamples.get(sampleId);

			if (pending != null) {
				pending.collectStackTraces(executionId, stackTraces);

				// Publish the sample
				if (pending.isComplete()) {
					pendingSamples.remove(sampleId);
					rememberRecentSampleId(sampleId);

					pending.completePromiseAndDiscard();
				}
			} else if (recentPendingSamples.contains(sampleId)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received late stack trace sample {} of task {}",
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

	int getNumberOfPendingSamples() {
		synchronized (lock) {
			return pendingSamples.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending stack trace sample, which collects stack traces and owns a
	 * {@link StackTraceSample} promise.
	 *
	 * <p>Access pending sample in lock scope.
	 */
	private static class PendingStackTraceSample {

		private final int sampleId;
		private final long startTime;
		private final Set<ExecutionAttemptID> pendingTasks;
		private final Map<ExecutionAttemptID, List<StackTraceElement[]>> stackTracesByTask;
		private final CompletableFuture<StackTraceSample> stackTraceFuture;

		private boolean isDiscarded;

		PendingStackTraceSample(
				int sampleId,
				ExecutionAttemptID[] tasksToCollect) {

			this.sampleId = sampleId;
			this.startTime = System.currentTimeMillis();
			this.pendingTasks = new HashSet<>(Arrays.asList(tasksToCollect));
			this.stackTracesByTask = Maps.newHashMapWithExpectedSize(tasksToCollect.length);
			this.stackTraceFuture = new CompletableFuture<>();
		}

		int getSampleId() {
			return sampleId;
		}

		long getStartTime() {
			return startTime;
		}

		boolean isDiscarded() {
			return isDiscarded;
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
				stackTracesByTask.clear();

				stackTraceFuture.completeExceptionally(new RuntimeException("Discarded", cause));

				isDiscarded = true;
			}
		}

		void collectStackTraces(ExecutionAttemptID executionId, List<StackTraceElement[]> stackTraces) {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			if (pendingTasks.remove(executionId)) {
				stackTracesByTask.put(executionId, Collections.unmodifiableList(stackTraces));
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

				StackTraceSample stackTraceSample = new StackTraceSample(
						sampleId,
						startTime,
						endTime,
						stackTracesByTask);

				stackTraceFuture.complete(stackTraceSample);
			} else {
				throw new IllegalStateException("Not completed yet");
			}
		}

		@SuppressWarnings("unchecked")
		CompletableFuture<StackTraceSample> getStackTraceSampleFuture() {
			return stackTraceFuture;
		}
	}
}
