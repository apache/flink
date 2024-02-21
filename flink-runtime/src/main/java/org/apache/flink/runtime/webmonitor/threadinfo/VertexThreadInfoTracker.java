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
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.VertexStatsTracker;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
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
public class VertexThreadInfoTracker implements VertexStatsTracker<VertexThreadInfoStats> {

    private static final Logger LOG = LoggerFactory.getLogger(VertexThreadInfoTracker.class);

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

    /**
     * The stats collected for job vertex and execution vertex will be collected into the
     * executionVertexStatsCache.
     */
    @GuardedBy("lock")
    private final Cache<ExecutionVertexKey, VertexThreadInfoStats> executionVertexStatsCache;

    @GuardedBy("lock")
    private final Set<ExecutionVertexKey> pendingExecutionVertexStats = new HashSet<>();

    private final int numSamples;

    private final Duration statsRefreshInterval;

    private final Duration delayBetweenSamples;

    private final int maxThreadInfoDepth;

    // Used for testing purposes
    private final CompletableFuture<Void> resultAvailableFuture = new CompletableFuture<>();

    /** Flag indicating whether the stats tracker has been shut down. */
    private boolean shutDown;

    private final Time rpcTimeout;

    VertexThreadInfoTracker(
            ThreadInfoRequestCoordinator coordinator,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            ScheduledExecutorService executor,
            Duration cleanUpInterval,
            int numSamples,
            Duration statsRefreshInterval,
            Duration delayBetweenSamples,
            int maxStackTraceDepth,
            Time rpcTimeout,
            Cache<JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache,
            Cache<ExecutionVertexKey, VertexThreadInfoStats> executionVertexStatsCache) {

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
        this.executionVertexStatsCache =
                checkNotNull(executionVertexStatsCache, "Execution vertex stats cache");

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

    @Override
    public Optional<VertexThreadInfoStats> getExecutionVertexStats(
            JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex) {
        synchronized (lock) {
            ExecutionVertexKey executionVertexKey = getKey(jobId, vertex, subtaskIndex);
            final VertexThreadInfoStats stats =
                    executionVertexStatsCache.getIfPresent(executionVertexKey);
            if (stats == null
                    || System.currentTimeMillis()
                            >= stats.getEndTime() + statsRefreshInterval.toMillis()) {
                triggerThreadInfoSampleInternal(executionVertexKey, vertex);
            }
            return Optional.ofNullable(stats);
        }
    }

    /**
     * Triggers a request for a job vertex to gather the thread info statistics. If there is a
     * sample in progress for the vertex, the call is ignored.
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

        if (pendingJobVertexStats.contains(jobVertexKey)) {
            return;
        }

        pendingJobVertexStats.add(jobVertexKey);
        triggerThreadInfoRequestForVertices(
                new JobVertexThreadInfoSampleCompletionCallback(jobVertexKey, vertex.getName()),
                vertex.getTaskVertices());
    }

    /**
     * Triggers a request for a execution vertex to gather the thread info statistics. If there is a
     * sample in progress for the execution vertex or job vertex, the call is ignored.
     *
     * @param executionVertexKey cache key
     * @param vertex Vertex to get the stats for.
     */
    private void triggerThreadInfoSampleInternal(
            final ExecutionVertexKey executionVertexKey, final AccessExecutionJobVertex vertex) {
        assert (Thread.holdsLock(lock));

        if (shutDown) {
            return;
        }

        if (pendingJobVertexStats.contains(executionVertexKey.getJobVertexKey())
                || pendingExecutionVertexStats.contains(executionVertexKey)) {
            return;
        }

        pendingExecutionVertexStats.add(executionVertexKey);
        final AccessExecutionVertex[] executionVertices =
                Arrays.stream(vertex.getTaskVertices())
                        .filter(
                                executionVertex ->
                                        executionVertex.getParallelSubtaskIndex()
                                                == executionVertexKey.subtaskIndex)
                        .toArray(AccessExecutionVertex[]::new);

        if (executionVertices.length == 0) {
            return;
        }
        triggerThreadInfoRequestForVertices(
                new ExecutionVertexThreadInfoSampleCompletionCallback(
                        executionVertexKey, executionVertices[0].getTaskNameWithSubtaskIndex()),
                executionVertices);
    }

    private void triggerThreadInfoRequestForVertices(
            ThreadInfoSampleCompletionCallback completionCallback,
            AccessExecutionVertex[] executionVertices) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Triggering thread info sample for tasks: {}",
                    Arrays.toString(executionVertices));
        }
        final CompletableFuture<ResourceManagerGateway> gatewayFuture =
                resourceManagerGatewayRetriever.getFuture();

        gatewayFuture
                .thenCompose(
                        (ResourceManagerGateway resourceManagerGateway) ->
                                coordinator.triggerThreadInfoRequest(
                                        matchExecutionsWithGateways(
                                                executionVertices, resourceManagerGateway),
                                        numSamples,
                                        delayBetweenSamples,
                                        maxThreadInfoDepth))
                .whenCompleteAsync(completionCallback, executor);
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
            if (executionVertex.getExecutionState() != ExecutionState.RUNNING
                    && executionVertex.getExecutionState() != ExecutionState.INITIALIZING) {
                LOG.trace(
                        "{} not running or initializing, but {}; not sampling",
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
        executionVertexStatsCache.cleanUp();
    }

    @Override
    public void shutDown() {
        synchronized (lock) {
            if (!shutDown) {
                jobVertexStatsCache.invalidateAll();
                pendingJobVertexStats.clear();

                executionVertexStatsCache.invalidateAll();
                pendingExecutionVertexStats.clear();

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

    private static ExecutionVertexKey getKey(
            JobID jobId, AccessExecutionJobVertex vertex, int subtaskIndex) {
        return new ExecutionVertexKey(jobId, vertex.getJobVertexId(), subtaskIndex);
    }

    static class JobVertexKey {
        private final JobID jobId;
        private final JobVertexID jobVertexId;

        private JobVertexKey(JobID jobId, JobVertexID jobVertexId) {
            this.jobId = jobId;
            this.jobVertexId = jobVertexId;
        }

        private ExecutionVertexKey toExecutionVertexKey(int subtaskIndex) {
            return new ExecutionVertexKey(jobId, jobVertexId, subtaskIndex);
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

    static class ExecutionVertexKey {
        private final JobVertexKey jobVertexKey;
        private final int subtaskIndex;

        private ExecutionVertexKey(JobID jobId, JobVertexID jobVertexId, int subtaskIndex) {
            this.jobVertexKey = new JobVertexKey(jobId, jobVertexId);
            this.subtaskIndex = subtaskIndex;
        }

        private JobVertexKey getJobVertexKey() {
            return jobVertexKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExecutionVertexKey executionVertexKey = (ExecutionVertexKey) o;
            return Objects.equals(jobVertexKey, executionVertexKey.jobVertexKey)
                    && Objects.equals(subtaskIndex, executionVertexKey.subtaskIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobVertexKey, subtaskIndex);
        }
    }

    private abstract class ThreadInfoSampleCompletionCallback
            implements BiConsumer<VertexThreadInfoStats, Throwable> {

        private final String sampleName;

        protected ThreadInfoSampleCompletionCallback(String sampleName) {
            this.sampleName = sampleName;
        }

        protected abstract void handleResult(VertexThreadInfoStats threadInfoStats);

        protected abstract void doFinally();

        @Override
        public void accept(VertexThreadInfoStats threadInfoStats, Throwable throwable) {
            synchronized (lock) {
                try {
                    if (shutDown) {
                        return;
                    }
                    if (threadInfoStats == null) {
                        LOG.error(
                                "Failed to gather a thread info sample for {}",
                                sampleName,
                                throwable);
                        return;
                    }
                    handleResult(threadInfoStats);
                    resultAvailableFuture.complete(null);
                } catch (Throwable t) {
                    LOG.error("Error during stats completion.", t);
                } finally {
                    doFinally();
                }
            }
        }
    }

    /** Callback on completed thread info sample for job vertex. */
    private class JobVertexThreadInfoSampleCompletionCallback
            extends ThreadInfoSampleCompletionCallback {

        private final JobVertexKey jobVertexKey;

        JobVertexThreadInfoSampleCompletionCallback(JobVertexKey jobVertexKey, String sampleName) {
            super(sampleName);
            this.jobVertexKey = jobVertexKey;
        }

        @Override
        protected void handleResult(VertexThreadInfoStats threadInfoStats) {
            jobVertexStatsCache.put(jobVertexKey, threadInfoStats);

            for (Map.Entry<ExecutionAttemptID, Collection<ThreadInfoSample>> entry :
                    threadInfoStats.getSamplesBySubtask().entrySet()) {
                ExecutionAttemptID executionAttemptID = entry.getKey();
                ExecutionVertexKey executionVertexKey =
                        jobVertexKey.toExecutionVertexKey(executionAttemptID.getSubtaskIndex());

                VertexThreadInfoStats oldStats =
                        executionVertexStatsCache.getIfPresent(executionVertexKey);
                if (oldStats == null || oldStats.getRequestId() < threadInfoStats.getRequestId()) {
                    executionVertexStatsCache.put(
                            executionVertexKey,
                            generateExecutionVertexStats(
                                    threadInfoStats, executionAttemptID, entry.getValue()));
                    continue;
                }

                if (oldStats.getRequestId() == threadInfoStats.getRequestId()) {
                    // When the same ExecutionVertex has multiple attempts.
                    Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samples =
                            oldStats.getSamplesBySubtask();
                    samples.put(executionAttemptID, entry.getValue());
                }
            }
        }

        private VertexThreadInfoStats generateExecutionVertexStats(
                VertexThreadInfoStats threadInfoStats,
                ExecutionAttemptID executionAttemptID,
                Collection<ThreadInfoSample> sample) {
            HashMap<ExecutionAttemptID, Collection<ThreadInfoSample>> samples = new HashMap<>();
            samples.put(executionAttemptID, sample);
            return new VertexThreadInfoStats(
                    threadInfoStats.getRequestId(),
                    threadInfoStats.getStartTime(),
                    threadInfoStats.getEndTime(),
                    samples);
        }

        @Override
        protected void doFinally() {
            pendingJobVertexStats.remove(jobVertexKey);
        }
    }

    /** Callback on completed thread info sample for execution vertex. */
    private class ExecutionVertexThreadInfoSampleCompletionCallback
            extends ThreadInfoSampleCompletionCallback {

        private final ExecutionVertexKey executionVertexKey;

        ExecutionVertexThreadInfoSampleCompletionCallback(
                ExecutionVertexKey executionVertexKey, String sampleName) {
            super(sampleName);
            this.executionVertexKey = executionVertexKey;
        }

        @Override
        protected void handleResult(VertexThreadInfoStats threadInfoStats) {
            executionVertexStatsCache.put(executionVertexKey, threadInfoStats);
        }

        @Override
        protected void doFinally() {
            pendingExecutionVertexStats.remove(executionVertexKey);
        }
    }
}
