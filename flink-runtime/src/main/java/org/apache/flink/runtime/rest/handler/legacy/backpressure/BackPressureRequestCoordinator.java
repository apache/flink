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
import org.apache.flink.runtime.messages.TaskBackPressureResponse;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
public class BackPressureRequestCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(BackPressureRequestCoordinator.class);

	private static final int NUM_GHOST_REQUEST_IDS = 10;

	private final Object lock = new Object();

	/** Executor used to run the futures. */
	private final Executor executor;

	/** Request time out of a triggered back pressure request. */
	private final Time requestTimeout;

	/** In progress back pressure requests. */
	@GuardedBy("lock")
	private final Map<Integer, PendingBackPressureRequest> pendingRequests = new HashMap<>();

	/** A list of recent request IDs to identify late messages vs. invalid ones. */
	private final ArrayDeque<Integer> recentPendingRequests = new ArrayDeque<>(NUM_GHOST_REQUEST_IDS);

	/** Request ID counter. */
	@GuardedBy("lock")
	private int requestIdCounter;

	/** Flag indicating whether the coordinator is still running. */
	@GuardedBy("lock")
	private boolean isShutDown;

	/**
	 * Creates a new coordinator for the cluster.
	 *
	 * @param executor Used to execute the futures.
	 * @param requestTimeout Request time out of a triggered back pressure request.
	 */
	public BackPressureRequestCoordinator(
			Executor executor,
			long requestTimeout) {

		checkArgument(requestTimeout >= 0L, "Illegal request timeout: " + requestTimeout);

		this.executor = checkNotNull(executor);
		this.requestTimeout = Time.milliseconds(requestTimeout);
	}

	/**
	 * Triggers a task back pressure stats request to all tasks.
	 *
	 * @param tasks Tasks to request.
	 * @return A future of the completed task back pressure stats.
	 */
	CompletableFuture<BackPressureStats> triggerBackPressureRequest(ExecutionVertex[] tasks) {

		checkNotNull(tasks, "Tasks to request must not be null.");
		checkArgument(tasks.length >= 1, "No tasks to request.");

		// Execution IDs of running tasks
		ExecutionAttemptID[] triggerIds = new ExecutionAttemptID[tasks.length];
		Execution[] executions = new Execution[tasks.length];

		// Check that all tasks are RUNNING before triggering anything. The
		// triggering can still fail.
		for (int i = 0; i < triggerIds.length; i++) {
			Execution execution = tasks[i].getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				executions[i] = execution;
				triggerIds[i] = execution.getAttemptId();
			} else {
				return FutureUtils.completedExceptionally(new IllegalStateException("Task " + tasks[i]
					.getTaskNameWithSubtaskIndex() + " is not running."));
			}
		}

		synchronized (lock) {
			if (isShutDown) {
				return FutureUtils.completedExceptionally(new IllegalStateException("Shut down"));
			}

			final int requestId = requestIdCounter++;

			LOG.debug("Triggering task back pressure request {}", requestId);

			final PendingBackPressureRequest pending = new PendingBackPressureRequest(requestId, triggerIds);

			// Add the pending request before scheduling the discard task to
			// prevent races with removing it again.
			pendingRequests.put(requestId, pending);

			// Trigger all requests. The request will be discarded if it takes
			// too long. We don't send cancel messages to the task managers,
			// but only wait for the responses and then ignore them.
			for (Execution execution: executions) {
				final CompletableFuture<TaskBackPressureResponse> taskBackPressureFuture =
					execution.requestBackPressure(requestId, requestTimeout);

				taskBackPressureFuture.handleAsync(
					(TaskBackPressureResponse taskBackPressureResponse, Throwable throwable) -> {
						if (taskBackPressureResponse != null) {
							collectTaskBackPressureStat(
								taskBackPressureResponse.getRequestId(),
								taskBackPressureResponse.getExecutionAttemptID(),
								taskBackPressureResponse.getBackPressureRatio());
						} else {
							cancelBackPressureRequest(requestId, throwable);
						}

						return null;
					},
					executor);
			}

			return pending.getBackPressureStatsFuture();
		}
	}

	/**
	 * Cancels a pending task back pressure request.
	 *
	 * @param requestId ID of the request to cancel.
	 * @param cause Cause of the cancelling (can be <code>null</code>).
	 */
	@VisibleForTesting
	void cancelBackPressureRequest(int requestId, @Nullable Throwable cause) {
		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			PendingBackPressureRequest pendingRequest = pendingRequests.remove(requestId);
			if (pendingRequest != null) {
				if (cause != null) {
					LOG.info("Cancelling back pressure request " + requestId, cause);
				} else {
					LOG.info("Cancelling back pressure request {}", requestId);
				}

				pendingRequest.discard(cause);
				rememberRecentRequestId(requestId);
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
				LOG.info("Shutting down back pressure request coordinator.");

				for (PendingBackPressureRequest pending : pendingRequests.values()) {
					pending.discard(new RuntimeException("Shut down."));
				}

				pendingRequests.clear();

				isShutDown = true;
			}
		}
	}

	/**
	 * Collects back pressure stat of a task.
	 *
	 * @param requestId ID of the request.
	 * @param executionId ID of the task requested.
	 * @param taskBackPressureRatio Back pressure ratio of the task.
	 *
	 * @throws IllegalStateException If the request ID is unknown and not recently
	 *                               finished or the request has been cancelled.
	 */
	@VisibleForTesting
	void collectTaskBackPressureStat(
			int requestId,
			ExecutionAttemptID executionId,
			double taskBackPressureRatio) {

		synchronized (lock) {
			if (isShutDown) {
				return;
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Collecting back pressure request {} result of task {}.", requestId, executionId);
			}

			PendingBackPressureRequest pending = pendingRequests.get(requestId);

			if (pending != null) {
				pending.collectBackPressureStats(executionId, taskBackPressureRatio);

				// Publish the request result
				if (pending.isComplete()) {
					pendingRequests.remove(requestId);
					rememberRecentRequestId(requestId);

					pending.completePromiseAndDiscard();
				}
			} else if (recentPendingRequests.contains(requestId)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received late back pressure request {} result of task {}.",
							requestId, executionId);
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Unknown request ID " + requestId);
				}
			}
		}
	}

	private void rememberRecentRequestId(int requestId) {
		if (recentPendingRequests.size() >= NUM_GHOST_REQUEST_IDS) {
			recentPendingRequests.removeFirst();
		}
		recentPendingRequests.addLast(requestId);
	}

	@VisibleForTesting
	int getNumberOfPendingRequests() {
		synchronized (lock) {
			return pendingRequests.size();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A pending task back pressure request, which collects task back pressure
	 * ratio and owns a {@link BackPressureStats} promise.
	 *
	 * <p>Access pending request in lock scope.
	 */
	private static class PendingBackPressureRequest {

		private final int requestId;
		private final long startTime;
		private final Set<ExecutionAttemptID> pendingTasks;
		private final Map<ExecutionAttemptID, Double> backPressureRatios;
		private final CompletableFuture<BackPressureStats> backPressureStatsFuture;

		private boolean isDiscarded;

		PendingBackPressureRequest(
				int requestId,
				ExecutionAttemptID[] tasksToCollect) {

			this.requestId = requestId;
			this.startTime = System.currentTimeMillis();
			this.pendingTasks = new HashSet<>(Arrays.asList(tasksToCollect));
			this.backPressureRatios = Maps.newHashMapWithExpectedSize(tasksToCollect.length);
			this.backPressureStatsFuture = new CompletableFuture<>();
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
				backPressureRatios.clear();

				backPressureStatsFuture.completeExceptionally(new RuntimeException("Discarded", cause));

				isDiscarded = true;
			}
		}

		void collectBackPressureStats(ExecutionAttemptID executionId, double backPressureRatio) {
			if (isDiscarded) {
				throw new IllegalStateException("Discarded");
			}

			if (pendingTasks.remove(executionId)) {
				backPressureRatios.put(executionId, backPressureRatio);
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

				BackPressureStats backPressureStats = new BackPressureStats(
						requestId,
						startTime,
						endTime,
						backPressureRatios);

				backPressureStatsFuture.complete(backPressureStats);
			} else {
				throw new IllegalStateException("Not completed yet");
			}
		}

		CompletableFuture<BackPressureStats> getBackPressureStatsFuture() {
			return backPressureStatsFuture;
		}
	}
}
