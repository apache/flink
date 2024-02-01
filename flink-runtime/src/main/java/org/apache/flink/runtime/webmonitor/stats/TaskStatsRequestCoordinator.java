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

package org.apache.flink.runtime.webmonitor.stats;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Encapsulates the common functionality for requesting statistics from tasks and combining their
 * responses.
 *
 * @param <T> Type of the statistics to be gathered.
 * @param <V> Type of the combined response.
 */
public class TaskStatsRequestCoordinator<T, V> {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final int NUM_GHOST_SAMPLE_IDS = 10;

    protected final Object lock = new Object();

    /** Executor used to run the futures. */
    protected final Executor executor;

    /** Request time out of the triggered tasks stats request. */
    protected final Duration requestTimeout;

    /** In progress samples. */
    @GuardedBy("lock")
    protected final Map<Integer, PendingStatsRequest<T, V>> pendingRequests = new HashMap<>();

    /** A list of recent request IDs to identify late messages vs. invalid ones. */
    @GuardedBy("lock")
    protected final ArrayDeque<Integer> recentPendingRequestIds =
            new ArrayDeque<>(NUM_GHOST_SAMPLE_IDS);

    /** Sample ID counter. */
    @GuardedBy("lock")
    protected int requestIdCounter;

    /** Flag indicating whether the coordinator is still running. */
    @GuardedBy("lock")
    protected boolean isShutDown;

    /**
     * Creates a new coordinator for the cluster.
     *
     * @param executor Used to execute the futures.
     * @param requestTimeout Request time out of the triggered tasks stats request.
     */
    public TaskStatsRequestCoordinator(Executor executor, Duration requestTimeout) {
        checkNotNull(requestTimeout, "The request timeout must cannot be null.");
        checkArgument(requestTimeout.toMillis() >= 0L, "The request timeout must be non-negative.");
        this.executor = checkNotNull(executor);
        this.requestTimeout = requestTimeout;
    }

    /**
     * Handles the failed stats response by canceling the corresponding unfinished pending request.
     *
     * @param requestId ID of the request to cancel.
     * @param cause Cause of the cancelling (can be <code>null</code>).
     */
    public void handleFailedResponse(int requestId, @Nullable Throwable cause) {
        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            PendingStatsRequest<T, V> pendingRequest = pendingRequests.remove(requestId);
            if (pendingRequest != null) {
                log.info("Cancelling request {}", requestId, cause);

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
                log.info("Shutting down task stats request coordinator.");

                for (PendingStatsRequest<T, V> pending : pendingRequests.values()) {
                    pending.discard(new RuntimeException("Shut down"));
                }

                pendingRequests.clear();
                recentPendingRequestIds.clear();

                isShutDown = true;
            }
        }
    }

    /**
     * Handles the successfully returned tasks stats response by collecting the corresponding
     * subtask samples.
     *
     * @param requestId ID of the request.
     * @param executionIds ID of the sampled task.
     * @param result Result of stats request returned by an individual task.
     * @throws IllegalStateException If unknown request ID and not recently finished or cancelled
     *     sample.
     */
    public void handleSuccessfulResponse(
            int requestId, ImmutableSet<ExecutionAttemptID> executionIds, T result) {

        synchronized (lock) {
            if (isShutDown) {
                return;
            }

            final String ids =
                    executionIds.stream()
                            .map(ExecutionAttemptID::toString)
                            .collect(Collectors.joining(", "));

            if (log.isDebugEnabled()) {
                log.debug("Collecting stats sample {} of tasks {}", requestId, ids);
            }

            PendingStatsRequest<T, V> pending = pendingRequests.get(requestId);

            if (pending != null) {
                pending.collectTaskStats(executionIds, result);

                // Publish the sample
                if (pending.isComplete()) {
                    pendingRequests.remove(requestId);
                    rememberRecentRequestId(requestId);

                    pending.completePromiseAndDiscard();
                }
            } else if (recentPendingRequestIds.contains(requestId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Received late stats sample {} of tasks {}", requestId, ids);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Unknown request ID %d.", requestId));
                }
            }
        }
    }

    private void rememberRecentRequestId(int sampleId) {
        if (recentPendingRequestIds.size() >= NUM_GHOST_SAMPLE_IDS) {
            recentPendingRequestIds.removeFirst();
        }
        recentPendingRequestIds.addLast(sampleId);
    }

    @VisibleForTesting
    public int getNumberOfPendingRequests() {
        synchronized (lock) {
            return pendingRequests.size();
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A pending task stats request, which collects samples from individual tasks and completes the
     * response future upon gathering all of of them.
     *
     * <p>Has to be accessed in lock scope.
     *
     * @param <T> Type of the result collected from tasks.
     * @param <V> Type of the result assembled and returned when all tasks where sampled.
     */
    @NotThreadSafe
    protected abstract static class PendingStatsRequest<T, V> {

        /** ID of the sampling request to this coordinator. */
        protected final int requestId;

        /** The time when the request is created. */
        protected final long startTime;

        /** All tasks what did not yet return a result. */
        protected final Set<Set<ExecutionAttemptID>> pendingTasks;

        /**
         * Results returned by individual tasks and stored by the tasks' {@link ExecutionAttemptID}.
         */
        protected final Map<ImmutableSet<ExecutionAttemptID>, T> statsResultByTaskGroup;

        /** The future with the final result. */
        protected final CompletableFuture<V> resultFuture;

        protected boolean isDiscarded;

        /**
         * Creates new {@link PendingStatsRequest}.
         *
         * @param requestId ID of the request.
         * @param tasksToCollect tasks from which the stats responses are expected.
         */
        protected PendingStatsRequest(
                int requestId, Collection<? extends Set<ExecutionAttemptID>> tasksToCollect) {
            this.requestId = requestId;
            this.startTime = System.currentTimeMillis();
            this.pendingTasks = new HashSet<>(tasksToCollect);
            this.statsResultByTaskGroup = Maps.newHashMapWithExpectedSize(tasksToCollect.size());
            this.resultFuture = new CompletableFuture<>();
        }

        protected boolean isComplete() {
            checkDiscarded();

            return pendingTasks.isEmpty();
        }

        protected void discard(Throwable cause) {
            if (!isDiscarded) {
                pendingTasks.clear();
                statsResultByTaskGroup.clear();

                resultFuture.completeExceptionally(new RuntimeException("Discarded", cause));

                isDiscarded = true;
            }
        }

        /**
         * Collects result from one of the tasks.
         *
         * @param executionId ID of the Task.
         * @param taskStatsResult Result of the stats sample from the Task.
         */
        protected void collectTaskStats(
                ImmutableSet<ExecutionAttemptID> executionId, T taskStatsResult) {
            checkDiscarded();

            if (pendingTasks.remove(executionId)) {
                statsResultByTaskGroup.put(executionId, taskStatsResult);
            } else if (isComplete()) {
                throw new IllegalStateException("Completed");
            } else {
                throw new IllegalArgumentException("Unknown task " + executionId);
            }
        }

        protected void checkDiscarded() {
            if (isDiscarded) {
                throw new IllegalStateException("Discarded");
            }
        }

        protected void completePromiseAndDiscard() {
            if (isComplete()) {
                isDiscarded = true;

                long endTime = System.currentTimeMillis();

                V combinedStats = assembleCompleteStats(endTime);

                resultFuture.complete(combinedStats);
            } else {
                throw new IllegalStateException("Not completed yet");
            }
        }

        /**
         * A Future, which will either complete successfully if all of the samples from individual
         * tasks are collected or exceptionally, if at least one of the task responses fails.
         *
         * @return A future with the result of collecting tasks statistics.
         */
        public CompletableFuture<V> getStatsFuture() {
            return resultFuture;
        }

        /**
         * A method that is called when responses from all tasks were collected successfully. It is
         * responsible for assembling the final result from the individual tasks' samples. Those
         * samples can be accessed via the {@code Map<ExecutionAttemptID, T> statsResultByTask}
         * variable.
         *
         * @param endTime The time when the final sample was collected.
         * @return The final combined result, which will be returned as a future by the {@code
         *     CompletableFuture<V> getStatsFuture} method
         */
        protected abstract V assembleCompleteStats(long endTime);
    }
}
