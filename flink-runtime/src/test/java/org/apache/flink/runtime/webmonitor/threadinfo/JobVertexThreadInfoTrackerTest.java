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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createScheduler;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for the {@link JobVertexThreadInfoTracker}. */
public class JobVertexThreadInfoTrackerTest extends TestLogger {

    private static final int REQUEST_ID = 0;
    private static final ExecutionJobVertex EXECUTION_JOB_VERTEX = createExecutionJobVertex();
    private static final ExecutionVertex[] TASK_VERTICES = EXECUTION_JOB_VERTEX.getTaskVertices();
    private static final Set<ExecutionAttemptID> ATTEMPT_IDS =
            Arrays.stream(TASK_VERTICES)
                    .map(
                            executionVertex ->
                                    executionVertex.getCurrentExecutionAttempt().getAttemptId())
                    .collect(Collectors.toSet());
    private static final JobID JOB_ID = new JobID();

    private static VertexThreadInfoStats threadInfoStatsDefaultSample;

    private static final Duration CLEAN_UP_INTERVAL = Duration.ofSeconds(60);
    private static final Duration STATS_REFRESH_INTERVAL = Duration.ofSeconds(60);
    private static final Duration TIME_GAP = Duration.ofSeconds(60);
    private static final Duration SMALL_TIME_GAP = Duration.ofMillis(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private static final int NUMBER_OF_SAMPLES = 1;
    private static final int MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    private static ScheduledExecutorService executor;

    @BeforeAll
    public static void setUp() {
        // Time gap determines endTime of stats, which controls if the "refresh" is triggered:
        // now >= stats.getEndTime() + statsRefreshInterval
        // Using a small gap to be able to test cache updates without much delay.
        threadInfoStatsDefaultSample = createThreadInfoStats(REQUEST_ID, SMALL_TIME_GAP);
        executor = Executors.newScheduledThreadPool(1);
    }

    @AfterAll
    public static void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /** Tests successful thread info stats request. */
    @Test
    public void testGetThreadInfoStats() throws Exception {
        doInitialRequestAndVerifyResult(createThreadInfoTracker());
    }

    /** Tests that cached result is reused within refresh interval. */
    @Test
    public void testCachedStatsNotUpdatedWithinRefreshInterval() throws Exception {
        final VertexThreadInfoStats unusedThreadInfoStats = createThreadInfoStats(1, TIME_GAP);

        final JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        STATS_REFRESH_INTERVAL,
                        threadInfoStatsDefaultSample,
                        unusedThreadInfoStats);
        // stores threadInfoStatsDefaultSample in cache
        doInitialRequestAndVerifyResult(tracker);
        Optional<VertexThreadInfoStats> result =
                tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX);
        // cached result is returned instead of unusedThreadInfoStats
        assertThat(result).isPresent().hasValue(threadInfoStatsDefaultSample);
    }

    /** Tests that cached result is NOT reused after refresh interval. */
    @Test
    public void testCachedStatsUpdatedAfterRefreshInterval() throws Exception {
        final Duration shortRefreshInterval = Duration.ofMillis(1);

        // first entry is in the past, so refresh is triggered immediately upon fetching it
        final VertexThreadInfoStats initialThreadInfoStats =
                createThreadInfoStats(
                        Instant.now().minus(10, ChronoUnit.SECONDS),
                        REQUEST_ID,
                        Duration.ofMillis(5));
        final VertexThreadInfoStats threadInfoStatsAfterRefresh =
                createThreadInfoStats(1, TIME_GAP);

        // register a CountDownLatch with the cache so we can await refresh of the entry
        CountDownLatch cacheRefreshed = new CountDownLatch(1);
        Cache<JobVertexThreadInfoTracker.Key, VertexThreadInfoStats> vertexStatsCache =
                createCache(CLEAN_UP_INTERVAL, new LatchRemovalListener<>(cacheRefreshed));
        final JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL,
                        shortRefreshInterval,
                        vertexStatsCache,
                        initialThreadInfoStats,
                        threadInfoStatsAfterRefresh);

        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();

        // retrieve the entry, triggering the refresh as side effect
        assertExpectedEqualsReceived(
                initialThreadInfoStats, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));

        // wait until the entry is refreshed
        cacheRefreshed.await();

        // verify that we get the second result on the next request
        Optional<VertexThreadInfoStats> result =
                tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX);
        assertExpectedEqualsReceived(threadInfoStatsAfterRefresh, result);
    }

    /** Tests that cached results are removed within the cleanup interval. */
    @Test
    public void testCachedStatsCleanedAfterCleanupInterval() throws Exception {
        final Duration shortCleanUpInterval = Duration.ofMillis(1);

        // register a CountDownLatch with the cache so we can await expiry of the entry
        CountDownLatch cacheExpired = new CountDownLatch(1);
        Cache<JobVertexThreadInfoTracker.Key, VertexThreadInfoStats> vertexStatsCache =
                createCache(shortCleanUpInterval, new LatchRemovalListener<>(cacheExpired));
        final JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        shortCleanUpInterval,
                        STATS_REFRESH_INTERVAL,
                        vertexStatsCache,
                        threadInfoStatsDefaultSample);

        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
        // wait until one eviction was registered
        cacheExpired.await();

        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
    }

    /** Tests that cached results are NOT removed within the cleanup interval. */
    @Test
    public void testCachedStatsNotCleanedWithinCleanupInterval() throws Exception {
        final JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker = createThreadInfoTracker();

        doInitialRequestAndVerifyResult(tracker);

        tracker.cleanUpVertexStatsCache();
        // the thread info stats with the same requestId should still be there
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));
    }

    /** Tests that cached results are not served after the shutdown. */
    @Test
    public void testShutDown() throws Exception {
        final JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker = createThreadInfoTracker();
        doInitialRequestAndVerifyResult(tracker);

        // shutdown directly
        tracker.shutDown();

        // verify that the previous cached result is invalid and trigger another request
        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
        // verify no response after shutdown
        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
    }

    private Cache<JobVertexThreadInfoTracker.Key, VertexThreadInfoStats> createCache(
            Duration cleanUpInterval,
            RemovalListener<JobVertexThreadInfoTracker.Key, VertexThreadInfoStats>
                    removalListener) {
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .build();
    }

    private void doInitialRequestAndVerifyResult(
            JobVertexThreadInfoTracker<VertexThreadInfoStats> tracker)
            throws InterruptedException, ExecutionException {
        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX)).isNotPresent();
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));
    }

    private static void assertExpectedEqualsReceived(
            VertexThreadInfoStats expected, Optional<VertexThreadInfoStats> receivedOptional) {
        assertThat(receivedOptional).isPresent();
        VertexThreadInfoStats received = receivedOptional.get();

        assertThat(expected.getRequestId()).isEqualTo(received.getRequestId());
        assertThat(expected.getEndTime()).isEqualTo(received.getEndTime());

        assertThat(TASK_VERTICES.length).isEqualTo(received.getNumberOfSubtasks());

        for (Collection<ThreadInfoSample> samples : received.getSamplesBySubtask().values()) {
            assertThat(samples.isEmpty()).isFalse();
        }
    }

    private JobVertexThreadInfoTracker<VertexThreadInfoStats> createThreadInfoTracker() {
        return createThreadInfoTracker(STATS_REFRESH_INTERVAL, threadInfoStatsDefaultSample);
    }

    private JobVertexThreadInfoTracker<VertexThreadInfoStats> createThreadInfoTracker(
            Duration statsRefreshInterval, VertexThreadInfoStats... stats) {
        return createThreadInfoTracker(CLEAN_UP_INTERVAL, statsRefreshInterval, null, stats);
    }

    private JobVertexThreadInfoTracker<VertexThreadInfoStats> createThreadInfoTracker(
            Duration cleanUpInterval,
            Duration statsRefreshInterval,
            Cache<JobVertexThreadInfoTracker.Key, VertexThreadInfoStats> vertexStatsCache,
            VertexThreadInfoStats... stats) {
        final ThreadInfoRequestCoordinator coordinator =
                new TestingThreadInfoRequestCoordinator(Runnable::run, REQUEST_TIMEOUT, stats);

        return JobVertexThreadInfoTrackerBuilder.newBuilder(
                        JobVertexThreadInfoTrackerTest::createMockResourceManagerGateway,
                        Function.identity(),
                        executor,
                        TestingUtils.TIMEOUT)
                .setCoordinator(coordinator)
                .setCleanUpInterval(cleanUpInterval)
                .setNumSamples(NUMBER_OF_SAMPLES)
                .setStatsRefreshInterval(statsRefreshInterval)
                .setDelayBetweenSamples(DELAY_BETWEEN_SAMPLES)
                .setMaxThreadInfoDepth(MAX_STACK_TRACE_DEPTH)
                .setVertexStatsCache(vertexStatsCache)
                .build();
    }

    private static VertexThreadInfoStats createThreadInfoStats(int requestId, Duration timeGap) {
        return createThreadInfoStats(Instant.now(), requestId, timeGap);
    }

    private static VertexThreadInfoStats createThreadInfoStats(
            Instant startTime, int requestId, Duration timeGap) {
        Instant endTime = startTime.plus(timeGap);

        final Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samples = new HashMap<>();

        for (ExecutionVertex vertex : TASK_VERTICES) {
            Optional<ThreadInfoSample> threadInfoSample =
                    JvmUtils.createThreadInfoSample(
                            Thread.currentThread().getId(), MAX_STACK_TRACE_DEPTH);
            checkState(threadInfoSample.isPresent(), "The threadInfoSample should be empty.");
            samples.put(
                    vertex.getCurrentExecutionAttempt().getAttemptId(),
                    Collections.singletonList(threadInfoSample.get()));
        }

        return new VertexThreadInfoStats(
                requestId, startTime.toEpochMilli(), endTime.toEpochMilli(), samples);
    }

    private static ExecutionJobVertex createExecutionJobVertex() {
        try {
            JobVertex jobVertex = new JobVertex("testVertex");
            jobVertex.setParallelism(10);
            jobVertex.setInvokableClass(AbstractInvokable.class);

            final SchedulerBase scheduler =
                    createScheduler(
                            JobGraphTestUtils.streamingJobGraph(jobVertex),
                            ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                            new DirectScheduledExecutorService());
            final ExecutionGraph eg = scheduler.getExecutionGraph();
            scheduler.startScheduling();
            ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);
            return scheduler.getExecutionJobVertex(jobVertex.getID());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ExecutionJobVertex.");
        }
    }

    private static CompletableFuture<ResourceManagerGateway> createMockResourceManagerGateway() {
        // ignored in TestingThreadInfoRequestCoordinator
        Function<ResourceID, CompletableFuture<TaskExecutorThreadInfoGateway>> function =
                (resourceID) -> CompletableFuture.completedFuture(null);

        TestingResourceManagerGateway testingResourceManagerGateway =
                new TestingResourceManagerGateway();
        testingResourceManagerGateway.setRequestTaskExecutorGatewayFunction(function);
        return CompletableFuture.completedFuture(testingResourceManagerGateway);
    }

    /**
     * A {@link ThreadInfoRequestCoordinator} which returns the pre-generated thread info stats
     * directly.
     */
    private static class TestingThreadInfoRequestCoordinator extends ThreadInfoRequestCoordinator {

        private final VertexThreadInfoStats[] vertexThreadInfoStats;
        private int counter = 0;

        TestingThreadInfoRequestCoordinator(
                Executor executor,
                Duration requestTimeout,
                VertexThreadInfoStats... vertexThreadInfoStats) {
            super(executor, requestTimeout);
            this.vertexThreadInfoStats = vertexThreadInfoStats;
        }

        @Override
        public CompletableFuture<VertexThreadInfoStats> triggerThreadInfoRequest(
                Map<
                                ImmutableSet<ExecutionAttemptID>,
                                CompletableFuture<TaskExecutorThreadInfoGateway>>
                        executionsWithGateways,
                int ignored2,
                Duration ignored3,
                int ignored4) {
            assertThat(executionsWithGateways.size() == 1).isTrue();
            assertThat(executionsWithGateways.keySet().iterator().next()).isEqualTo(ATTEMPT_IDS);

            return CompletableFuture.completedFuture(
                    vertexThreadInfoStats[(counter++) % vertexThreadInfoStats.length]);
        }
    }

    private static class LatchRemovalListener<K, V> implements RemovalListener<K, V> {

        private final CountDownLatch latch;

        private LatchRemovalListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onRemoval(@Nonnull RemovalNotification<K, V> removalNotification) {
            latch.countDown();
        }
    }
}
