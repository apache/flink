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
import org.apache.flink.runtime.execution.ExecutionState;
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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalNotification;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createScheduler;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for the {@link VertexThreadInfoTracker}. */
@ExtendWith(ParameterizedTestExtension.class)
class VertexThreadInfoTrackerTest {

    private static final int REQUEST_ID = 0;
    private static final int PARALLELISM = 10;
    private static final JobID JOB_ID = new JobID();

    private static final Duration CLEAN_UP_INTERVAL = Duration.ofSeconds(60);
    private static final Duration STATS_REFRESH_INTERVAL = Duration.ofSeconds(60);
    private static final Duration TIME_GAP = Duration.ofSeconds(60);
    private static final Duration SMALL_TIME_GAP = Duration.ofMillis(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    private static final int NUMBER_OF_SAMPLES = 1;
    private static final int MAX_STACK_TRACE_DEPTH = 100;
    private static final Duration DELAY_BETWEEN_SAMPLES = Duration.ofMillis(50);

    private static ScheduledExecutorService executor;

    private ExecutionJobVertex executionJobVertex;

    private ExecutionAttemptID[] attemptIDS;

    private VertexThreadInfoStats threadInfoStatsDefaultSample;

    @Parameter public ExecutionState executionState;

    @Parameters(name = "executionState={0}")
    private static Collection<ExecutionState> parameters() {
        return Arrays.asList(ExecutionState.RUNNING, ExecutionState.INITIALIZING);
    }

    @BeforeEach
    void setUp() {
        executionJobVertex = createExecutionJobVertex();
        attemptIDS =
                Arrays.stream(executionJobVertex.getTaskVertices())
                        .map(
                                executionVertex ->
                                        executionVertex.getCurrentExecutionAttempt().getAttemptId())
                        .sorted(Comparator.comparingInt(ExecutionAttemptID::getSubtaskIndex))
                        .toArray(ExecutionAttemptID[]::new);
        // Time gap determines endTime of stats, which controls if the "refresh" is triggered:
        // now >= stats.getEndTime() + statsRefreshInterval
        // Using a small gap to be able to test cache updates without much delay.
        threadInfoStatsDefaultSample = createThreadInfoStats(REQUEST_ID, SMALL_TIME_GAP);
        executor = Executors.newScheduledThreadPool(1);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /** Tests successful thread info stats request. */
    @TestTemplate
    void testGetThreadInfoStats() throws Exception {
        doInitialJobVertexRequestAndVerifyResult(createThreadInfoTracker());
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            doInitialExecutionVertexRequestAndVerifyResult(createThreadInfoTracker(), subtaskIndex);
        }
    }

    /** Tests that cached result is reused within refresh interval. */
    @TestTemplate
    void testCachedStatsNotUpdatedWithinRefreshInterval() throws Exception {
        final VertexThreadInfoStats unusedThreadInfoStats = createThreadInfoStats(1, TIME_GAP);

        // Test for trigger request at job vertex level
        final VertexThreadInfoTracker tracker =
                createThreadInfoTracker(
                        STATS_REFRESH_INTERVAL,
                        threadInfoStatsDefaultSample,
                        unusedThreadInfoStats);
        // stores threadInfoStatsDefaultSample in job vertex cache and execution vertex cache
        doInitialJobVertexRequestAndVerifyResult(tracker);
        Optional<VertexThreadInfoStats> result =
                tracker.getJobVertexStats(JOB_ID, executionJobVertex);
        // cached result is returned instead of unusedThreadInfoStats
        assertExpectedEqualsReceived(threadInfoStatsDefaultSample, result);
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            threadInfoStatsDefaultSample, attemptIDS[subtaskIndex]),
                    tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));
        }

        // Test for trigger request at execution vertex level
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            final VertexThreadInfoTracker tracker1 =
                    createThreadInfoTracker(
                            STATS_REFRESH_INTERVAL,
                            threadInfoStatsDefaultSample,
                            unusedThreadInfoStats);
            // stores threadInfoStatsDefaultSample in cache
            doInitialExecutionVertexRequestAndVerifyResult(tracker1, subtaskIndex);
            // cached result is returned instead of unusedThreadInfoStats
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            threadInfoStatsDefaultSample, attemptIDS[0]),
                    tracker1.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));
        }
    }

    /** Tests that cached job vertex result is NOT reused after refresh interval. */
    @TestTemplate
    void testJobVertexCachedStatsUpdatedAfterRefreshInterval() throws Exception {
        final Duration shortRefreshInterval = Duration.ofMillis(1000);

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
        Cache<VertexThreadInfoTracker.JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache =
                createCache(CLEAN_UP_INTERVAL, new LatchRemovalListener<>(cacheRefreshed));
        final VertexThreadInfoTracker tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL,
                        shortRefreshInterval,
                        jobVertexStatsCache,
                        null,
                        initialThreadInfoStats,
                        threadInfoStatsAfterRefresh);

        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();

        // retrieve the entry, triggering the refresh as side effect
        assertExpectedEqualsReceived(
                initialThreadInfoStats, tracker.getJobVertexStats(JOB_ID, executionJobVertex));

        // wait until the entry is refreshed
        cacheRefreshed.await();

        // verify that we get the second result on the next request
        Optional<VertexThreadInfoStats> result =
                tracker.getJobVertexStats(JOB_ID, executionJobVertex);
        assertExpectedEqualsReceived(threadInfoStatsAfterRefresh, result);
    }

    /** Tests that cached execution vertex result is NOT reused after refresh interval. */
    @TestTemplate
    void testExecutionVertexCachedStatsUpdatedAfterRefreshInterval() throws Exception {
        final Duration shortRefreshInterval = Duration.ofMillis(1000);

        // first entry is in the past, so refresh is triggered immediately upon fetching it
        final VertexThreadInfoStats initialThreadInfoStats =
                createThreadInfoStats(
                        Instant.now().minus(10, ChronoUnit.SECONDS),
                        REQUEST_ID,
                        Duration.ofMillis(5));
        final VertexThreadInfoStats threadInfoStatsAfterRefresh =
                createThreadInfoStats(1, TIME_GAP);

        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            // register a CountDownLatch with the cache so we can await refresh of the entry
            CountDownLatch cacheRefreshed = new CountDownLatch(1);
            Cache<VertexThreadInfoTracker.ExecutionVertexKey, VertexThreadInfoStats>
                    executionVertexStatsCache =
                            createCache(
                                    CLEAN_UP_INTERVAL, new LatchRemovalListener<>(cacheRefreshed));
            final VertexThreadInfoTracker tracker =
                    createThreadInfoTracker(
                            CLEAN_UP_INTERVAL,
                            shortRefreshInterval,
                            null,
                            executionVertexStatsCache,
                            initialThreadInfoStats,
                            threadInfoStatsAfterRefresh);

            // no stats yet, but the request triggers async collection of stats
            assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex))
                    .isNotPresent();
            // block until the async call completes and the first result is available
            tracker.getResultAvailableFuture().get();

            // retrieve the entry, triggering the refresh as side effect
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            initialThreadInfoStats, attemptIDS[subtaskIndex]),
                    tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));

            // wait until the entry is refreshed
            cacheRefreshed.await();

            // verify that we get the second result on the next request
            Optional<VertexThreadInfoStats> result =
                    tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex);
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            threadInfoStatsAfterRefresh, attemptIDS[subtaskIndex]),
                    result);
        }
    }

    /**
     * Tests that the execution vertex request should be ignored when the corresponding job vertex
     * request is pending.
     */
    @TestTemplate
    void testExecutionVertexShouldBeIgnoredWhenJobVertexIsPending() throws Exception {
        CompletableFuture<VertexThreadInfoStats> statsFuture = new CompletableFuture<>();
        TestingBlockingAndCountableCoordinator coordinator =
                new TestingBlockingAndCountableCoordinator(statsFuture);
        final VertexThreadInfoTracker tracker =
                createThreadInfoTracker(
                        CLEAN_UP_INTERVAL, STATS_REFRESH_INTERVAL, null, null, coordinator);

        // Request the job vertex stats and keep pending
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        assertThat(coordinator.getTriggerCounter()).isOne();

        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            // These execution vertex requests shouldn't return any result, and should be ignored
            // directly due to the corresponding job vertex is pending.
            assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex))
                    .isNotPresent();
            assertThat(tracker.getResultAvailableFuture()).isNotCompleted();
            assertThat(coordinator.getTriggerCounter()).isOne();
        }

        // Complete the job vertex request
        statsFuture.complete(threadInfoStatsDefaultSample);
        tracker.getResultAvailableFuture().get();

        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex))
                    .isPresent();
            // These execution vertex requests still should be ignored due to cached result is
            // reused within refresh interval.
            assertThat(coordinator.getTriggerCounter()).isOne();
        }
    }

    /** Tests that cached results are removed within the cleanup interval. */
    @TestTemplate
    void testCachedStatsCleanedAfterCleanupInterval() throws Exception {
        final Duration shortCleanUpInterval = Duration.ofMillis(1);

        // register a CountDownLatch with the cache so we can await expiry of the entry
        CountDownLatch cacheExpired = new CountDownLatch(1);
        // Test for trigger request at job vertex level
        Cache<VertexThreadInfoTracker.JobVertexKey, VertexThreadInfoStats> jobVertexStatsCache =
                createCache(shortCleanUpInterval, new LatchRemovalListener<>(cacheExpired));
        final VertexThreadInfoTracker tracker =
                createThreadInfoTracker(
                        shortCleanUpInterval,
                        STATS_REFRESH_INTERVAL,
                        jobVertexStatsCache,
                        null,
                        threadInfoStatsDefaultSample);

        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        // wait until one eviction was registered
        cacheExpired.await();

        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();

        // Test for trigger request at execution vertex level
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            // register a CountDownLatch with the cache so we can await expiry of the entry
            CountDownLatch executionCacheExpired = new CountDownLatch(1);
            Cache<VertexThreadInfoTracker.ExecutionVertexKey, VertexThreadInfoStats>
                    executionVertexStatsCache =
                            createCache(
                                    shortCleanUpInterval,
                                    new LatchRemovalListener<>(executionCacheExpired));
            final VertexThreadInfoTracker executionTracker =
                    createThreadInfoTracker(
                            shortCleanUpInterval,
                            STATS_REFRESH_INTERVAL,
                            null,
                            executionVertexStatsCache,
                            threadInfoStatsDefaultSample);

            // no stats yet, but the request triggers async collection of stats
            assertThat(
                            executionTracker.getExecutionVertexStats(
                                    JOB_ID, executionJobVertex, subtaskIndex))
                    .isNotPresent();
            // wait until one eviction was registered
            executionCacheExpired.await();

            assertThat(
                            executionTracker.getExecutionVertexStats(
                                    JOB_ID, executionJobVertex, subtaskIndex))
                    .isNotPresent();
        }
    }

    /** Tests that cached results are NOT removed within the cleanup interval. */
    @TestTemplate
    void testCachedStatsNotCleanedWithinCleanupInterval() throws Exception {
        final VertexThreadInfoTracker tracker = createThreadInfoTracker();

        doInitialJobVertexRequestAndVerifyResult(tracker);

        tracker.cleanUpStatsCache();
        // the thread info stats with the same requestId should still be there
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample,
                tracker.getJobVertexStats(JOB_ID, executionJobVertex));
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            threadInfoStatsDefaultSample, attemptIDS[subtaskIndex]),
                    tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));
        }
    }

    /** Tests that cached results are not served after the shutdown. */
    @TestTemplate
    void testShutDown() throws Exception {
        final VertexThreadInfoTracker tracker = createThreadInfoTracker();
        doInitialJobVertexRequestAndVerifyResult(tracker);

        // shutdown directly
        tracker.shutDown();

        // verify that the previous cached result is invalid and trigger another request
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, 0)).isNotPresent();
        // verify no response after shutdown
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, 0)).isNotPresent();
    }

    private <K> Cache<K, VertexThreadInfoStats> createCache(
            Duration cleanUpInterval, RemovalListener<K, VertexThreadInfoStats> removalListener) {
        return CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .expireAfterAccess(cleanUpInterval.toMillis(), TimeUnit.MILLISECONDS)
                .removalListener(removalListener)
                .build();
    }

    private void doInitialJobVertexRequestAndVerifyResult(VertexThreadInfoTracker tracker)
            throws InterruptedException, ExecutionException {
        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getJobVertexStats(JOB_ID, executionJobVertex)).isNotPresent();
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();
        assertExpectedEqualsReceived(
                threadInfoStatsDefaultSample,
                tracker.getJobVertexStats(JOB_ID, executionJobVertex));
        for (int subtaskIndex = 0; subtaskIndex < PARALLELISM; subtaskIndex++) {
            assertExpectedEqualsReceived(
                    generateThreadInfoStatsForExecutionVertex(
                            threadInfoStatsDefaultSample, attemptIDS[subtaskIndex]),
                    tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));
        }
    }

    private void doInitialExecutionVertexRequestAndVerifyResult(
            VertexThreadInfoTracker tracker, int subtaskIndex)
            throws InterruptedException, ExecutionException {
        // no stats yet, but the request triggers async collection of stats
        assertThat(tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex))
                .isNotPresent();
        // block until the async call completes and the first result is available
        tracker.getResultAvailableFuture().get();
        assertExpectedEqualsReceived(
                generateThreadInfoStatsForExecutionVertex(
                        threadInfoStatsDefaultSample, attemptIDS[subtaskIndex]),
                tracker.getExecutionVertexStats(JOB_ID, executionJobVertex, subtaskIndex));
    }

    private static VertexThreadInfoStats generateThreadInfoStatsForExecutionVertex(
            VertexThreadInfoStats jobVertexStats, ExecutionAttemptID executionAttemptID) {
        return generateThreadInfoStatsForExecutionVertices(
                jobVertexStats, Collections.singleton(executionAttemptID));
    }

    private static VertexThreadInfoStats generateThreadInfoStatsForExecutionVertices(
            VertexThreadInfoStats jobVertexStats, Set<ExecutionAttemptID> executionAttemptIDS) {
        if (executionAttemptIDS.equals(jobVertexStats.getSamplesBySubtask().keySet())) {
            return jobVertexStats;
        }
        return new VertexThreadInfoStats(
                jobVertexStats.getRequestId(),
                jobVertexStats.getStartTime(),
                jobVertexStats.getEndTime(),
                jobVertexStats.getSamplesBySubtask().entrySet().stream()
                        .filter(entry -> executionAttemptIDS.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static void assertExpectedEqualsReceived(
            VertexThreadInfoStats expected, Optional<VertexThreadInfoStats> receivedOptional) {
        assertThat(receivedOptional).isPresent();
        VertexThreadInfoStats received = receivedOptional.get();

        assertThat(expected.getRequestId()).isEqualTo(received.getRequestId());
        assertThat(expected.getEndTime()).isEqualTo(received.getEndTime());

        assertThat(expected.getNumberOfSubtasks()).isEqualTo(received.getNumberOfSubtasks());

        for (Collection<ThreadInfoSample> samples : received.getSamplesBySubtask().values()) {
            assertThat(samples.isEmpty()).isFalse();
        }
    }

    private VertexThreadInfoTracker createThreadInfoTracker() {
        return createThreadInfoTracker(STATS_REFRESH_INTERVAL, threadInfoStatsDefaultSample);
    }

    private VertexThreadInfoTracker createThreadInfoTracker(
            Duration statsRefreshInterval, VertexThreadInfoStats... stats) {
        return createThreadInfoTracker(CLEAN_UP_INTERVAL, statsRefreshInterval, null, null, stats);
    }

    private VertexThreadInfoTracker createThreadInfoTracker(
            Duration cleanUpInterval,
            Duration statsRefreshInterval,
            @Nullable
                    Cache<VertexThreadInfoTracker.JobVertexKey, VertexThreadInfoStats>
                            jobVertexStatsCache,
            @Nullable
                    Cache<VertexThreadInfoTracker.ExecutionVertexKey, VertexThreadInfoStats>
                            executionVertexStatsCache,
            VertexThreadInfoStats... stats) {
        final ThreadInfoRequestCoordinator coordinator =
                new TestingThreadInfoRequestCoordinator(Runnable::run, REQUEST_TIMEOUT, stats);
        return createThreadInfoTracker(
                cleanUpInterval,
                statsRefreshInterval,
                jobVertexStatsCache,
                executionVertexStatsCache,
                coordinator);
    }

    private VertexThreadInfoTracker createThreadInfoTracker(
            Duration cleanUpInterval,
            Duration statsRefreshInterval,
            @Nullable
                    Cache<VertexThreadInfoTracker.JobVertexKey, VertexThreadInfoStats>
                            jobVertexStatsCache,
            @Nullable
                    Cache<VertexThreadInfoTracker.ExecutionVertexKey, VertexThreadInfoStats>
                            executionVertexStatsCache,
            ThreadInfoRequestCoordinator coordinator) {

        return VertexThreadInfoTrackerBuilder.newBuilder(
                        VertexThreadInfoTrackerTest::createMockResourceManagerGateway,
                        executor,
                        TestingUtils.TIMEOUT)
                .setCoordinator(coordinator)
                .setCleanUpInterval(cleanUpInterval)
                .setNumSamples(NUMBER_OF_SAMPLES)
                .setStatsRefreshInterval(statsRefreshInterval)
                .setDelayBetweenSamples(DELAY_BETWEEN_SAMPLES)
                .setMaxThreadInfoDepth(MAX_STACK_TRACE_DEPTH)
                .setJobVertexStatsCache(jobVertexStatsCache)
                .setExecutionVertexStatsCache(executionVertexStatsCache)
                .build();
    }

    private VertexThreadInfoStats createThreadInfoStats(int requestId, Duration timeGap) {
        return createThreadInfoStats(Instant.now(), requestId, timeGap);
    }

    private VertexThreadInfoStats createThreadInfoStats(
            Instant startTime, int requestId, Duration timeGap) {
        Instant endTime = startTime.plus(timeGap);

        final Map<ExecutionAttemptID, Collection<ThreadInfoSample>> samples = new HashMap<>();

        for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
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

    private ExecutionJobVertex createExecutionJobVertex() {
        try {
            JobVertex jobVertex = new JobVertex("testVertex");
            jobVertex.setParallelism(PARALLELISM);
            jobVertex.setInvokableClass(AbstractInvokable.class);

            final SchedulerBase scheduler =
                    createScheduler(
                            JobGraphTestUtils.streamingJobGraph(jobVertex),
                            ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                            new DirectScheduledExecutorService());
            final ExecutionGraph eg = scheduler.getExecutionGraph();
            scheduler.startScheduling();
            switch (executionState) {
                case RUNNING:
                    ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);
                    break;
                case INITIALIZING:
                    ExecutionGraphTestUtils.switchAllVerticesToInitializing(eg);
                    break;
                default:
                    throw new IllegalArgumentException("Just support RUNNING and INITIALIZING.");
            }
            return scheduler.getExecutionJobVertex(jobVertex.getID());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ExecutionJobVertex.", e);
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

        private final VertexThreadInfoStats[] jobVertexThreadInfoStats;
        private int counter = 0;

        TestingThreadInfoRequestCoordinator(
                Executor executor,
                Duration requestTimeout,
                VertexThreadInfoStats... jobVertexThreadInfoStats) {
            super(executor, requestTimeout);
            this.jobVertexThreadInfoStats = jobVertexThreadInfoStats;
        }

        private VertexThreadInfoStats getVertexThreadInfoStats() {
            return jobVertexThreadInfoStats[(counter++) % jobVertexThreadInfoStats.length];
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

            ImmutableSet<ExecutionAttemptID> executionAttemptIDS =
                    executionsWithGateways.keySet().iterator().next();
            return CompletableFuture.completedFuture(
                    generateThreadInfoStatsForExecutionVertices(
                            getVertexThreadInfoStats(), executionAttemptIDS));
        }
    }

    /**
     * A {@link ThreadInfoRequestCoordinator} which supports block and returns the number of calls.
     */
    private static class TestingBlockingAndCountableCoordinator
            extends ThreadInfoRequestCoordinator {

        private final CompletableFuture<VertexThreadInfoStats> blockingFuture;

        private final AtomicLong triggerCounter;

        TestingBlockingAndCountableCoordinator(
                CompletableFuture<VertexThreadInfoStats> blockingFuture) {
            super(Runnable::run, REQUEST_TIMEOUT);
            this.blockingFuture = blockingFuture;
            this.triggerCounter = new AtomicLong(0);
        }

        public long getTriggerCounter() {
            return triggerCounter.get();
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
            triggerCounter.getAndIncrement();
            return blockingFuture.thenApply(
                    stats ->
                            generateThreadInfoStatsForExecutionVertices(
                                    stats, executionsWithGateways.keySet().iterator().next()));
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
