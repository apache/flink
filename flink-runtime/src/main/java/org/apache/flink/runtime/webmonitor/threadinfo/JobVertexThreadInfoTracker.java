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
import org.apache.flink.runtime.executiongraph.AccessExecution;
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

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

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
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Tracker of thread infos for {@link ExecutionJobVertex}. */
public class JobVertexThreadInfoTracker implements JobVertexStatsTracker<VertexThreadInfoStats> {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexThreadInfoTracker.class);

    /** Lock guarding trigger operations. */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final ThreadInfoRequestCoordinator coordinator;

    private final ExecutorService executor;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    @GuardedBy("lock")
    private final Cache<JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache;

    @GuardedBy("lock")
    private final Set<JobVertexKey> pendingJobVertexStats = new HashSet<>();

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
            ScheduledExecutorService executor,
            Duration cleanUpInterval,
            int numSamples,
            Duration statsRefreshInterval,
            Duration delayBetweenSamples,
            int maxStackTraceDepth,
            Time rpcTimeout,
            Cache<JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache) {

        this.coordinator = checkNotNull(coordinator, "Thread info samples coordinator");
        this.resourceManagerGatewayRetriever =
                checkNotNull(resourceManagerGatewayRetriever, "Gateway retriever");
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

        this.jobVertexStatsCache = checkNotNull(jobVertexStatsCache, "Job vertex stats cache");

        executor.scheduleWithFixedDelay(
                this::cleanUpStatsCache,
                cleanUpInterval.toMillis(),
                cleanUpInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Optional<VertexThreadInfoStats> getJobVertexStats(
            JobID jobId, AccessExecutionJobVertex vertex) {
        synchronized (lock) {
            final JobVertexKey jobVertexKey = getKey(jobId, vertex);

            final VertexThreadInfoStats stats = jobVertexStatsCache.getIfPresent(jobVertexKey);
            if (stats == null
                    || System.currentTimeMillis()
                            >= stats.getEndTime() + statsRefreshInterval.toMillis()) {
                triggerThreadInfoSampleInternal(jobVertexKey, vertex);
            }
            return Optional.ofNullable(stats);
        }
    }

    /**
     * Triggers a request for a vertex to gather the thread info statistics. If there is a sample in
     * progress for the vertex, the call is ignored.
     *
     * @param jobVertexKey cache key
     * @param vertex Vertex to get the stats for.
     */
    private void triggerThreadInfoSampleInternal(
            final JobVertexKey jobVertexKey, final AccessExecutionJobVertex vertex) {
        assert (Thread.holdsLock(lock));

        if (shutDown) {
            return;
        }

        if (!pendingJobVertexStats.contains(jobVertexKey)) {
            pendingJobVertexStats.add(jobVertexKey);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Triggering thread info sample for tasks: {}",
                        Arrays.toString(vertex.getTaskVertices()));
            }

            final AccessExecutionVertex[] executionVertices = vertex.getTaskVertices();
            final CompletableFuture<ResourceManagerGateway> gatewayFuture =
                    resourceManagerGatewayRetriever.getFuture();

            CompletableFuture<VertexThreadInfoStats> sample =
                    gatewayFuture.thenCompose(
                            (ResourceManagerGateway resourceManagerGateway) ->
                                    coordinator.triggerThreadInfoRequest(
                                            matchExecutionsWithGateways(
                                                    executionVertices, resourceManagerGateway),
                                            numSamples,
                                            delayBetweenSamples,
                                            maxThreadInfoDepth));

            sample.whenCompleteAsync(
                    new ThreadInfoSampleCompletionCallback(jobVertexKey, vertex), executor);
        }
    }

    private Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
            matchExecutionsWithGateways(
                    AccessExecutionVertex[] executionVertices,
                    ResourceManagerGateway resourceManagerGateway) {

        // Group executions by their TaskManagerLocation to be able to issue one sampling
        // request per TaskManager for all relevant tasks at once
        final Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> executionsByLocation =
                groupExecutionsByLocation(executionVertices);

        return mapExecutionsToGateways(resourceManagerGateway, executionsByLocation);
    }

    private Map<ImmutableSet<ExecutionAttemptID>, CompletableFuture<TaskExecutorThreadInfoGateway>>
            mapExecutionsToGateways(
                    ResourceManagerGateway resourceManagerGateway,
                    Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> verticesByLocation) {

        final Map<
                        ImmutableSet<ExecutionAttemptID>,
                        CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionsWithGateways = new HashMap<>();

        for (Map.Entry<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> entry :
                verticesByLocation.entrySet()) {
            TaskManagerLocation tmLocation = entry.getKey();
            ImmutableSet<ExecutionAttemptID> attemptIds = entry.getValue();

            CompletableFuture<TaskExecutorThreadInfoGateway> taskExecutorGatewayFuture =
                    resourceManagerGateway.requestTaskExecutorThreadInfoGateway(
                            tmLocation.getResourceID(), rpcTimeout);

            executionsWithGateways.put(attemptIds, taskExecutorGatewayFuture);
        }
        return executionsWithGateways;
    }

    private Map<TaskManagerLocation, ImmutableSet<ExecutionAttemptID>> groupExecutionsByLocation(
            AccessExecutionVertex[] executionVertices) {

        final Map<TaskManagerLocation, Set<ExecutionAttemptID>> executionAttemptsByLocation =
                new HashMap<>();

        for (AccessExecutionVertex executionVertex : executionVertices) {
            if (executionVertex.getExecutionState() != ExecutionState.RUNNING) {
                LOG.trace(
                        "{} not running, but {}; not sampling",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        executionVertex.getExecutionState());
                continue;
            }
            for (AccessExecution execution : executionVertex.getCurrentExecutions()) {
                TaskManagerLocation tmLocation = execution.getAssignedResourceLocation();
                if (tmLocation == null) {
                    LOG.trace("ExecutionVertex {} is currently not assigned", executionVertex);
                    continue;
                }
                Set<ExecutionAttemptID> groupedAttemptIds =
                        executionAttemptsByLocation.getOrDefault(tmLocation, new HashSet<>());

                ExecutionAttemptID attemptId = execution.getAttemptId();
                groupedAttemptIds.add(attemptId);
                executionAttemptsByLocation.put(tmLocation, groupedAttemptIds);
            }
        }

        return executionAttemptsByLocation.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, e -> ImmutableSet.copyOf(e.getValue())));
    }

    @VisibleForTesting
    void cleanUpStatsCache() {
        jobVertexStatsCache.cleanUp();
    }

    @Override
    public void shutDown() {
        synchronized (lock) {
            if (!shutDown) {
                jobVertexStatsCache.invalidateAll();
                pendingJobVertexStats.clear();

                shutDown = true;
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> getResultAvailableFuture() {
        return resultAvailableFuture;
    }

    private static JobVertexKey getKey(JobID jobId, AccessExecutionJobVertex vertex) {
        return new JobVertexKey(jobId, vertex.getJobVertexId());
    }

    static class JobVertexKey {
        private final JobID jobId;
        private final JobVertexID jobVertexId;

        private JobVertexKey(JobID jobId, JobVertexID jobVertexId) {
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
            JobVertexKey jobVertexKey = (JobVertexKey) o;
            return Objects.equals(jobId, jobVertexKey.jobId)
                    && Objects.equals(jobVertexId, jobVertexKey.jobVertexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, jobVertexId);
        }
    }

    /** Callback on completed thread info sample. */
    private class ThreadInfoSampleCompletionCallback
            implements BiConsumer<VertexThreadInfoStats, Throwable> {

        private final JobVertexKey jobVertexKey;
        private final AccessExecutionJobVertex vertex;

        ThreadInfoSampleCompletionCallback(
                JobVertexKey jobVertexKey, AccessExecutionJobVertex vertex) {
            this.jobVertexKey = jobVertexKey;
            this.vertex = vertex;
        }

        @Override
        public void accept(VertexThreadInfoStats threadInfoStats, Throwable throwable) {
            synchronized (lock) {
                try {
                    if (shutDown) {
                        return;
                    }
                    if (threadInfoStats != null) {
                        jobVertexStatsCache.put(jobVertexKey, threadInfoStats);
                        resultAvailableFuture.complete(null);
                    } else {
                        LOG.error(
                                "Failed to gather a thread info sample for {}",
                                vertex.getName(),
                                throwable);
                    }
                } catch (Throwable t) {
                    LOG.error("Error during stats completion.", t);
                } finally {
                    pendingJobVertexStats.remove(jobVertexKey);
                }
            }
        }
    }
}
