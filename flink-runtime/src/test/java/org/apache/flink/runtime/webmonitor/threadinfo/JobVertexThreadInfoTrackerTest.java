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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava18.com.google.common.cache.RemovalNotification;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link JobVertexThreadInfoTracker}. */
public class JobVertexThreadInfoTrackerTest extends TestLogger {

    private static final int REQUEST_ID = 0;
    private static final ExecutionJobVertex EXECUTION_JOB_VERTEX = createExecutionJobVertex();
    private static final ExecutionVertex[] TASK_VERTICES = EXECUTION_JOB_VERTEX.getTaskVertices();
    private static final JobID JOB_ID = new JobID();

    private static ThreadInfoSample threadInfoSample;
    private static JobVertexThreadInfoStats threadInfoStatsDefaultSample;

    private static final Duration CLEAN_UP_INTERVAL = Duration.ofSeconds(60);
    private static final Duration STATS_REFRESH_INTERVAL = Duration.ofSeconds(60);
    private static final Duration TIME_GAP = Duration.ofSeconds(60);
    private static final Duration SMALL_TIME_GAP = Duration.ofMillis(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private static final int NUMBER_OF_SAMPLES = 1;
    private static final int MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    @Rule public Timeout caseTimeout = new Timeout(10, TimeUnit.SECONDS);

    private static ScheduledExecutorService executor;

    @BeforeClass
    public static void setUp() {
        // Time gap determines endTime of stats, which controls if the "refresh" is triggered:
        // now >= stats.getEndTime() + statsRefreshInterval
        // Using a small gap to be able to test cache updates without much delay.
        threadInfoSample =
                JvmUtils.createThreadInfoSample(
                                Thread.currentThread().getId(), MAX_STACK_TRACE_DEPTH)
                        .get();
        threadInfoStatsDefaultSample =
                createThreadInfoStats(
                        REQUEST_ID, SMALL_TIME_GAP, Collections.singletonList(threadInfoSample));
        executor = Executors.newScheduledThreadPool(1);
    }

    @AfterClass
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
        final JobVertexThreadInfoStats unusedThreadInfoStats =
                createThreadInfoStats(1, TIME_GAP, null);

        final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        STATS_REFRESH_INTERVAL,
                        threadInfoStatsDefaultSample,
                        unusedThreadInfoStats);
        // stores threadInfoStatsDefaultSample in cache
        doInitialRequestAndVerifyResult(tracker);
        Optional<JobVertexThreadInfoStats> result =
                tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX);
        // cached result is returned instead of unusedThreadInfoStats
        assertEquals(threadInfoStatsDefaultSample, result.get());
    }

    /** Tests that cached result is NOT reused after refresh interval. */
    @Test
    public void testCachedStatsUpdatedAfterRefreshInterval() throws Exception {
        final Duration shortRefreshInterval = Duration.ofMillis(1);

        // first entry is in the past, so refresh is triggered immediately upon fetching it
        final JobVertexThreadInfoStats initialThreadInfoStats =
                createThreadInfoStats(
                        Instant.now().minus(10, ChronoUnit.SECONDS),
                        REQUEST_ID,
                        Duration.ofMillis(5),
                        Collections.singletonList(threadInfoSample));
        final JobVertexThreadInfoStats threadInfoStatsAfterRefresh =
                createThreadInfoStats(1, TIME_GAP, Collections.singletonList(threadInfoSample));

        // register a CountDownLatch with the cache so we can await refresh of the entry
        CountDownLatch cacheRefreshed = new CountDownLatch(1);
        Cache<JobVertexThreadInfoTracker.Key, JobVertexThreadInfoStats> vertexStatsCache =
                createCache(CLEAN_UP_INTERVAL, new LatchRemovalListener<>(cacheRefreshed));
        final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL,
                        shortRefreshInterval,
                        vertexStatsCache,
                        initialThreadInfoStats,
                        threadInfoStatsAfterRefresh);

        // no stats yet, but the request triggers async collection of stats
        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();

        // retrieve the entry, triggering the refresh as side effect
        assertExpectedEqualsReceived(
                initialThreadInfoStats, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));

        // wait until the entry is refreshed
        cacheRefreshed.await();

        // verify that we get the second result on the next request
        Optional<JobVertexThreadInfoStats> result =
                tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX);
        assertExpectedEqualsReceived(threadInfoStatsAfterRefresh, result);
    }

    /** Tests that cached results are removed within the cleanup interval. */
    @Test
    public void testCachedStatsCleanedAfterCleanupInterval() throws Exception {
        final Duration shortCleanUpInterval = Duration.ofMillis(1);

        // register a CountDownLatch with the cache so we can await expiry of the entry
        CountDownLatch cacheExpired = new CountDownLatch(1);
        Cache<JobVertexThreadInfoTracker.Key, JobVertexThreadInfoStats> vertexStatsCache =
                createCache(shortCleanUpInterval, new LatchRemovalListener<>(cacheExpired));
        final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker =
                createThreadInfoTracker(
                        shortCleanUpInterval,
                        STATS_REFRESH_INTERVAL,
                        vertexStatsCache,
                        threadInfoStatsDefaultSample);

        // no stats yet, but the request triggers async collection of stats
        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
        // wait until one eviction was registered
        cacheExpired.await();

        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
    }

    /** Tests that cached results are NOT removed within the cleanup interval. */
    @Test
    public void testCachedStatsNotCleanedWithinCleanupInterval() throws Exception {
        final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker =
                createThreadInfoTracker();

        doInitialRequestAndVerifyResult(tracker);

        tracker.cleanUpVertexStatsCache();
        // the thread info stats with the same requestId should still be there
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));
    }

    /** Tests that cached results are not served after the shutdown. */
    @Test
    public void testShutDown() throws Exception {
        final JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker =
                createThreadInfoTracker();
        doInitialRequestAndVerifyResult(tracker);

        // shutdown directly
        tracker.shutDown();

        // verify that the previous cached result is invalid and trigger another request
        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
        // verify no response after shutdown
        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
    }

    private Cache<JobVertexThreadInfoTracker.Key, JobVertexThreadInfoStats> createCache(
            Duration cleanUpInterval,
            RemovalListener<JobVertexThreadInfoTracker.Key, JobVertexThreadInfoStats>
                    removalListener) {
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .build();
    }

    private void doInitialRequestAndVerifyResult(
            JobVertexThreadInfoTracker<JobVertexThreadInfoStats> tracker)
            throws InterruptedException, ExecutionException {
        // no stats yet, but the request triggers async collection of stats
        assertFalse(tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX).isPresent());
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample, tracker.getVertexStats(JOB_ID, EXECUTION_JOB_VERTEX));
    }

    private static void assertExpectedEqualsReceived(
            JobVertexThreadInfoStats expected,
            Optional<JobVertexThreadInfoStats> receivedOptional) {
        assertTrue(receivedOptional.isPresent());
        JobVertexThreadInfoStats received = receivedOptional.get();

        assertEquals(expected.getRequestId(), received.getRequestId());
        assertEquals(expected.getEndTime(), received.getEndTime());

        assertEquals(TASK_VERTICES.length, received.getNumberOfSubtasks());

        for (List<ThreadInfoSample> samples : received.getSamplesBySubtask().values()) {
            assertThat(samples.isEmpty(), is(false));
        }
    }

    private JobVertexThreadInfoTracker<JobVertexThreadInfoStats> createThreadInfoTracker() {
        return createThreadInfoTracker(STATS_REFRESH_INTERVAL, threadInfoStatsDefaultSample);
    }

    private JobVertexThreadInfoTracker<JobVertexThreadInfoStats> createThreadInfoTracker(
            Duration statsRefreshInterval, JobVertexThreadInfoStats... stats) {
        return createThreadInfoTracker(CLEAN_UP_INTERVAL, statsRefreshInterval, null, stats);
    }

    private JobVertexThreadInfoTracker<JobVertexThreadInfoStats> createThreadInfoTracker(
            Duration cleanUpInterval,
            Duration statsRefreshInterval,
            Cache<JobVertexThreadInfoTracker.Key, JobVertexThreadInfoStats> vertexStatsCache,
            JobVertexThreadInfoStats... stats) {
        final ThreadInfoRequestCoordinator coordinator =
                new TestingThreadInfoRequestCoordinator(Runnable::run, REQUEST_TIMEOUT, stats);

        return JobVertexThreadInfoTrackerBuilder.newBuilder(
                        JobVertexThreadInfoTrackerTest::createMockResourceManagerGateway,
                        Function.identity(),
                        executor,
                        TestingUtils.TIMEOUT())
                .setCoordinator(coordinator)
                .setCleanUpInterval(cleanUpInterval)
                .setNumSamples(NUMBER_OF_SAMPLES)
                .setStatsRefreshInterval(statsRefreshInterval)
                .setDelayBetweenSamples(DELAY_BETWEEN_SAMPLES)
                .setMaxThreadInfoDepth(MAX_STACK_TRACE_DEPTH)
                .setVertexStatsCache(vertexStatsCache)
                .build();
    }

    private static JobVertexThreadInfoStats createThreadInfoStats(
            int requestId, Duration timeGap, List<ThreadInfoSample> threadInfoSamples) {
        return createThreadInfoStats(Instant.now(), requestId, timeGap, threadInfoSamples);
    }

    private static JobVertexThreadInfoStats createThreadInfoStats(
            Instant startTime,
            int requestId,
            Duration timeGap,
            List<ThreadInfoSample> threadInfoSamples) {
        Instant endTime = startTime.plus(timeGap);

        final Map<ExecutionAttemptID, List<ThreadInfoSample>> threadInfoRatiosByTask =
                new HashMap<>();

        for (ExecutionVertex vertex : TASK_VERTICES) {
            threadInfoRatiosByTask.put(
                    vertex.getCurrentExecutionAttempt().getAttemptId(), threadInfoSamples);
        }

        return new JobVertexThreadInfoStats(
                requestId,
                startTime.toEpochMilli(),
                endTime.toEpochMilli(),
                threadInfoRatiosByTask);
    }

    private static ExecutionJobVertex createExecutionJobVertex() {
        try {
            JobVertex jobVertex = new JobVertex("testVertex");
            jobVertex.setInvokableClass(AbstractInvokable.class);
            return ExecutionGraphTestUtils.getExecutionJobVertex(jobVertex);
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

        private final JobVertexThreadInfoStats[] jobVertexThreadInfoStats;
        private int counter = 0;

        TestingThreadInfoRequestCoordinator(
                Executor executor,
                Duration requestTimeout,
                JobVertexThreadInfoStats... jobVertexThreadInfoStats) {
            super(executor, requestTimeout);
            this.jobVertexThreadInfoStats = jobVertexThreadInfoStats;
        }

        @Override
        public CompletableFuture<JobVertexThreadInfoStats> triggerThreadInfoRequest(
                Map<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>> ignored1,
                int ignored2,
                Duration ignored3,
                int ignored4) {
            return CompletableFuture.completedFuture(
                    jobVertexThreadInfoStats[(counter++) % jobVertexThreadInfoStats.length]);
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
