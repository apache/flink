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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.offerSlots;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the ExecutionGraph-creation retry behavior of the {@link AdaptiveScheduler} (the {@link
 * RetryingExecutionGraphCreation} state and the dedicated retry strategy).
 */
class AdaptiveSchedulerExecutionGraphCreationRetryTest extends AdaptiveSchedulerTestBase {

    /**
     * With the ExecutionGraph-creation retry strategy left at its no-restart default, even a
     * transient creation failure fails the job.
     */
    @Test
    void testExecutionGraphCreationRetryDisabledByDefaultFailsJob() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setExecutionGraphFactoryDecorator(
                                alwaysThrowingExecutionGraphFactory(
                                        () ->
                                                new FlinkRuntimeException(
                                                        "Failed to create checkpoint storage",
                                                        new IOException("read timed out"))))
                        .build();

        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(PARALLELISM);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        assertThat(scheduler.getJobTerminationFuture().get()).isEqualTo(JobStatus.FAILED);
        assertThat(taskManagerGateway.submittedTasks).isEmpty();
    }

    @Test
    void testFailureEnrichersRunOnExecutionGraphCreationFailure() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final TestingFailureEnricher failureEnricher = new TestingFailureEnricher();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setFailureEnrichers(Collections.singletonList(failureEnricher))
                        .setExecutionGraphFactoryDecorator(
                                alwaysThrowingExecutionGraphFactory(
                                        () ->
                                                new FlinkRuntimeException(
                                                        "Failed to create checkpoint storage",
                                                        new IOException("read timed out"))))
                        .build();

        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(PARALLELISM);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        assertThat(scheduler.getJobTerminationFuture().get()).isEqualTo(JobStatus.FAILED);
        assertThat(failureEnricher.getSeenThrowables()).isNotEmpty();
    }

    @Test
    void testTransientExecutionGraphCreationFailureIsRetriedThenSucceeds() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final int minimumExecutionGraphCreationFailures = 1;
        final AtomicInteger executionGraphCreationAttempts = new AtomicInteger();

        final CompletableFuture<Gauge<Long>> numRetriesMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    if (MetricNames.NUM_EXECUTION_GRAPH_CREATION_RETRIES.equals(
                                            name)) {
                                        numRetriesMetricFuture.complete((Gauge<Long>) metric);
                                    }
                                })
                        .build();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobManagerJobMetricGroup(
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                                metricRegistry, "localhost")
                                        .addJob(new JobID(), "jobName"))
                        .setExecutionGraphRetryBackoffTimeStrategy(
                                new TestRestartBackoffTimeStrategy(true, 0L))
                        .setExecutionGraphFactoryDecorator(
                                throwingForFirstAttempts(
                                        executionGraphCreationAttempts,
                                        minimumExecutionGraphCreationFailures,
                                        () ->
                                                new FlinkRuntimeException(
                                                        "Failed to create checkpoint storage",
                                                        new IOException("read timed out"))))
                        .build();

        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(PARALLELISM);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        awaitJobReachingParallelism(taskManagerGateway, scheduler, PARALLELISM);
        assertThat(executionGraphCreationAttempts.get())
                .isGreaterThanOrEqualTo(minimumExecutionGraphCreationFailures + 1);

        // Each recoverable creation failure transitions through RetryingExecutionGraphCreation and
        // is reflected in the numExecutionGraphCreationRetries metric.
        final Gauge<Long> numRetriesMetric = numRetriesMetricFuture.get();
        assertThat(numRetriesMetric.getValue())
                .isEqualTo((long) minimumExecutionGraphCreationFailures);
    }

    /**
     * A non-transient ExecutionGraph-creation failure (here a {@link SuppressRestartsException})
     * terminates the job — it is never eligible for retry.
     */
    @Test
    void testUnrecoverableExecutionGraphCreationFailureFailsJob() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .setExecutionGraphFactoryDecorator(
                                alwaysThrowingExecutionGraphFactory(
                                        () ->
                                                new SuppressRestartsException(
                                                        new FlinkException(
                                                                "fatal EG creation failure"))))
                        .build();

        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(PARALLELISM);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        assertThat(scheduler.getJobTerminationFuture().get()).isEqualTo(JobStatus.FAILED);
    }

    private static JobGraph createJobGraph() {
        return streamingJobGraph(JOB_VERTEX);
    }

    private static DefaultDeclarativeSlotPool createDeclarativeSlotPool(
            JobID jobId, ComponentMainThreadExecutor mainThreadExecutor) {
        return new DefaultDeclarativeSlotPool(
                jobId,
                new DefaultAllocatedSlotPool(),
                ignored -> {},
                DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT,
                Duration.ZERO,
                mainThreadExecutor);
    }

    private void awaitJobReachingParallelism(
            AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway,
            SchedulerNG scheduler,
            int parallelism)
            throws Exception {
        // Wait for all tasks to be submitted
        taskManagerGateway.waitForSubmissions(parallelism);

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .get();

        assertThat(executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism())
                .isEqualTo(parallelism);
    }

    private static UnaryOperator<ExecutionGraphFactory> alwaysThrowingExecutionGraphFactory(
            Supplier<? extends RuntimeException> exceptionSupplier) {
        return delegate ->
                (jg, ccs, cc, cic, cst, plc, its, vans, vps, esul, mpfs, epsc, log) -> {
                    throw exceptionSupplier.get();
                };
    }

    private static UnaryOperator<ExecutionGraphFactory> throwingForFirstAttempts(
            AtomicInteger attemptCounter,
            int failingAttempts,
            Supplier<? extends RuntimeException> exceptionSupplier) {
        return delegate ->
                (jg, ccs, cc, cic, cst, plc, its, vans, vps, esul, mpfs, epsc, log) -> {
                    if (attemptCounter.getAndIncrement() < failingAttempts) {
                        throw exceptionSupplier.get();
                    }
                    return delegate.createAndRestoreExecutionGraph(
                            jg, ccs, cc, cic, cst, plc, its, vans, vps, esul, mpfs, epsc, log);
                };
    }
}
