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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.JobVertexStatsTracker;
import org.apache.flink.runtime.webmonitor.stats.Statistics;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tracker of thread infos for {@link ExecutionJobVertex}.
 *
 * @param <T> Type of the derived statistics to return.
 */
public class JobVertexThreadInfoTracker<T extends Statistics> implements JobVertexStatsTracker<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexThreadInfoTracker.class);

    /** Lock guarding trigger operations. */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final ThreadInfoRequestCoordinator coordinator;

    private final Function<JobVertexThreadInfoStats, T> createStatsFn;

    private final ExecutorService executor;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    @GuardedBy("lock")
    private final Cache<Key, T> vertexStatsCache;

    @GuardedBy("lock")
    private final Set<Key> pendingStats = new HashSet<>();

    private final int numSamples;

    private final Duration statsRefreshInterval;

    private final Duration delayBetweenSamples;

    private final int maxThreadInfoDepth;

    // Used for testing purposes
    private final CompletableFuture<Void> resultAvailableFuture = new CompletableFuture<>();

    /** Flag indicating whether the stats tracker has been shut down. */
    private boolean shutDown;

    private final Time rpcTimeout;

    JobVertexThreadInfoTracker(
            ThreadInfoRequestCoordinator coordinator,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            Function<JobVertexThreadInfoStats, T> createStatsFn,
            ScheduledExecutorService executor,
            Duration cleanUpInterval,
            int numSamples,
            Duration statsRefreshInterval,
            Duration delayBetweenSamples,
            int maxStackTraceDepth,
            Time rpcTimeout,
            Cache<Key, T> vertexStatsCache) {

        this.coordinator = checkNotNull(coordinator, "Thread info samples coordinator");
        this.resourceManagerGatewayRetriever =
                checkNotNull(resourceManagerGatewayRetriever, "Gateway retriever");
        this.createStatsFn = checkNotNull(createStatsFn, "Create stats function");
        this.executor = checkNotNull(executor, "Scheduled executor");
        this.statsRefreshInterval =
                checkNotNull(statsRefreshInterval, "Statistics refresh interval");
        this.rpcTimeout = rpcTimeout;

        checkArgument(cleanUpInterval.toMillis() > 0, "Clean up interval must be greater than 0");

        checkArgument(numSamples >= 1, "Number of samples");
        this.numSamples = numSamples;

        checkArgument(
                statsRefreshInterval.toMillis() > 0,
                "Stats refresh interval must be greater than 0");

        this.delayBetweenSamples = checkNotNull(delayBetweenSamples, "Delay between samples");

        checkArgument(maxStackTraceDepth > 0, "Max stack trace depth must be greater than 0");
        this.maxThreadInfoDepth = maxStackTraceDepth;

        this.vertexStatsCache = checkNotNull(vertexStatsCache, "Vertex stats cache");

        executor.scheduleWithFixedDelay(
                this::cleanUpVertexStatsCache,
                cleanUpInterval.toMillis(),
                cleanUpInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<T> getVertexStats(JobID jobId, AccessExecutionJobVertex vertex) {
        synchronized (lock) {
            final Key key = getKey(jobId, vertex);

            final T stats = vertexStatsCache.getIfPresent(key);
            if (stats == null
                    || System.currentTimeMillis()
                            >= stats.getEndTime() + statsRefreshInterval.toMillis()) {
                triggerThreadInfoSampleInternal(key, vertex);
            }
            return Optional.ofNullable(stats);
        }
    }

    /**
     * Triggers a request for a vertex to gather the thread info statistics. If there is a sample in
     * progress for the vertex, the call is ignored.
     *
     * @param key cache key
     * @param vertex Vertex to get the stats for.
     */
    private void triggerThreadInfoSampleInternal(
            final Key key, final AccessExecutionJobVertex vertex) {
        assert (Thread.holdsLock(lock));

        if (shutDown) {
            return;
        }

        if (!pendingStats.contains(key)) {
            pendingStats.add(key);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Triggering thread info sample for tasks: {}",
                        Arrays.toString(vertex.getTaskVertices()));
            }

            final AccessExecutionVertex[] executionVertices = vertex.getTaskVertices();
            final CompletableFuture<ResourceManagerGateway> gatewayFuture =
                    resourceManagerGatewayRetriever.getFuture();

            CompletableFuture<JobVertexThreadInfoStats> sample =
                    gatewayFuture.thenCompose(
                            (ResourceManagerGateway resourceManagerGateway) ->
                                    coordinator.triggerThreadInfoRequest(
                                            matchExecutionsWithGateways(
                                                    executionVertices, resourceManagerGateway),
                                            numSamples,
                                            delayBetweenSamples,
                                            maxThreadInfoDepth));

            sample.whenCompleteAsync(new ThreadInfoSampleCompletionCallback(key, vertex), executor);
        }
    }

    private Map<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>>
            matchExecutionsWithGateways(
                    AccessExecutionVertex[] executionVertices,
                    ResourceManagerGateway resourceManagerGateway) {

        Map<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionsWithGateways = new HashMap<>();

        for (AccessExecutionVertex executionVertex : executionVertices) {
            TaskManagerLocation tmLocation = executionVertex.getCurrentAssignedResourceLocation();

            if (tmLocation != null) {
                CompletableFuture<TaskExecutorThreadInfoGateway> taskExecutorGatewayFuture =
                        resourceManagerGateway.requestTaskExecutorThreadInfoGateway(
                                tmLocation.getResourceID(), rpcTimeout);

                if (executionVertex.getExecutionState() == ExecutionState.RUNNING) {
                    executionsWithGateways.put(
                            executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                            taskExecutorGatewayFuture);
                } else {
                    LOG.trace(
                            "{} not running, but {}; not sampling",
                            executionVertex.getTaskNameWithSubtaskIndex(),
                            executionVertex.getExecutionState());
                }
            } else {
                LOG.trace("ExecutionVertex {} is currently not assigned", executionVertex);
            }
        }

        return executionsWithGateways;
    }

    @VisibleForTesting
    void cleanUpVertexStatsCache() {
        vertexStatsCache.cleanUp();
    }

    @Override
    public void shutDown() {
        synchronized (lock) {
            if (!shutDown) {
                vertexStatsCache.invalidateAll();
                pendingStats.clear();

                shutDown = true;
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> getResultAvailableFuture() {
        return resultAvailableFuture;
    }

    private static Key getKey(JobID jobId, AccessExecutionJobVertex vertex) {
        return new Key(jobId, vertex.getJobVertexId());
    }

    static class Key {
        private final JobID jobId;
        private final JobVertexID jobVertexId;

        private Key(JobID jobId, JobVertexID jobVertexId) {
            this.jobId = jobId;
            this.jobVertexId = jobVertexId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(jobId, key.jobId) && Objects.equals(jobVertexId, key.jobVertexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, jobVertexId);
        }
    }

    /** Callback on completed thread info sample. */
    private class ThreadInfoSampleCompletionCallback
            implements BiConsumer<JobVertexThreadInfoStats, Throwable> {

        private final Key key;
        private final AccessExecutionJobVertex vertex;

        ThreadInfoSampleCompletionCallback(Key key, AccessExecutionJobVertex vertex) {
            this.key = key;
            this.vertex = vertex;
        }

        @Override
        public void accept(JobVertexThreadInfoStats threadInfoStats, Throwable throwable) {
            synchronized (lock) {
                try {
                    if (shutDown) {
                        return;
                    }
                    if (threadInfoStats != null) {
                        vertexStatsCache.put(key, createStatsFn.apply(threadInfoStats));
                        resultAvailableFuture.complete(null);
                    } else {
                        LOG.debug(
                                "Failed to gather a thread info sample for {}",
                                vertex.getName(),
                                throwable);
                    }
                } catch (Throwable t) {
                    LOG.error("Error during stats completion.", t);
                } finally {
                    pendingStats.remove(key);
                }
            }
        }
    }
}
