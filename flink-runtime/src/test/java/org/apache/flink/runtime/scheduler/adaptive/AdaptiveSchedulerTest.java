/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsListener;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.NoOpCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraphTest;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingDeclarativeSlotPoolBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.TestingFreeSlotTracker;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.DefaultSchedulerTest;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlotAllocator;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.offerSlots;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.enableCheckpointing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link AdaptiveScheduler}. */
public class AdaptiveSchedulerTest {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofHours(1);
    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX = createNoOpVertex("v1", PARALLELISM);

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveSchedulerTest.class);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> TEST_EXECUTOR_RESOURCE =
            new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);

    private final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    private final ComponentMainThreadExecutor singleThreadMainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                    TEST_EXECUTOR_RESOURCE.getExecutor());

    private final ClassLoader classLoader = ClassLoader.getSystemClassLoader();

    private AdaptiveScheduler scheduler;

    @BeforeEach
    void before() {
        scheduler = null;
    }

    @AfterEach
    void after() {
        closeInExecutorService(scheduler, singleThreadMainThreadExecutor);
    }

    private static void closeInExecutorService(
            @Nullable AdaptiveScheduler scheduler, Executor executor) {
        if (scheduler != null) {
            final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
            executor.execute(
                    () -> {
                        try {
                            scheduler.cancel();

                            FutureUtils.forward(scheduler.closeAsync(), closeFuture);
                        } catch (Throwable t) {
                            closeFuture.completeExceptionally(t);
                        }
                    });

            // we have to wait for the job termination outside the main thread because the
            // cancellation tasks are scheduled on the main thread as well.
            scheduler
                    .getJobTerminationFuture()
                    .whenCompleteAsync(
                            (jobStatus, error) -> {
                                assertThat(scheduler.getState().getClass())
                                        .isEqualTo(Finished.class);

                                if (error != null) {
                                    closeFuture.completeExceptionally(error);
                                } else {
                                    try {
                                        FutureUtils.forward(scheduler.closeAsync(), closeFuture);
                                    } catch (Throwable t) {
                                        closeFuture.completeExceptionally(t);
                                    }
                                }
                            },
                            executor);
            assertThatFuture(closeFuture).eventuallySucceeds();
        }
    }

    private void startTestInstanceInMainThread() {
        runInMainThread(() -> scheduler.startScheduling());
    }

    private void runInMainThread(Runnable callback) {
        CompletableFuture.runAsync(callback, singleThreadMainThreadExecutor).join();
    }

    @Test
    void testInitialState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThat(scheduler.getState()).isInstanceOf(Created.class);
    }

    @Test
    void testArchivedCheckpointingSettingsNotNullIfCheckpointingIsEnabled() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();
        final ArchivedExecutionGraph archivedExecutionGraph =
                scheduler.getArchivedExecutionGraph(JobStatus.INITIALIZING, null);

        ArchivedExecutionGraphTest.assertContainsCheckpointSettings(archivedExecutionGraph);
    }

    @Test
    void testArchivedJobVerticesPresent() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();
        final ArchivedExecutionGraph archivedExecutionGraph =
                scheduler.getArchivedExecutionGraph(JobStatus.INITIALIZING, null);

        ArchivedExecutionJobVertex jobVertex =
                archivedExecutionGraph.getJobVertex(JOB_VERTEX.getID());
        assertThat(jobVertex)
                .isNotNull()
                .satisfies(
                        archived -> {
                            assertThat(archived.getParallelism())
                                    .isEqualTo(JOB_VERTEX.getParallelism());
                            // JOB_VERTEX.maxP == -1, but we want the actual maxP determined by the
                            // scheduler
                            assertThat(archived.getMaxParallelism()).isEqualTo(128);
                        });

        ArchivedExecutionGraphTest.assertContainsCheckpointSettings(archivedExecutionGraph);
    }

    @Test
    void testIsState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        final State state = scheduler.getState();

        assertThat(scheduler.isState(state)).isTrue();
        assertThat(scheduler.isState(new DummyState(scheduler))).isFalse();
    }

    @Test
    void testRunIfState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(scheduler.getState(), () -> ran.set(true));
        assertThat(ran.get()).isTrue();
    }

    @Test
    void testRunIfStateWithStateMismatch() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(new DummyState(scheduler), () -> ran.set(true));
        assertThat(ran.get()).isFalse();
    }

    @Test
    void testHasEnoughResourcesReturnsFalseIfUnsatisfied() {
        final ResourceCounter resourceRequirement =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);
        assertThat(
                        AdaptiveScheduler.hasDesiredResources(
                                resourceRequirement, Collections.emptyList()))
                .isFalse();
    }

    @Test
    void testHasEnoughResourcesReturnsTrueIfSatisfied() {
        final ResourceCounter resourceRequirement =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);
        final Collection<TestingSlot> freeSlots =
                createSlotsForResourceRequirements(resourceRequirement);
        assertThat(AdaptiveScheduler.hasDesiredResources(resourceRequirement, freeSlots)).isTrue();
    }

    private Collection<TestingSlot> createSlotsForResourceRequirements(
            ResourceCounter resourceRequirements) {
        final Collection<TestingSlot> slotInfos = new ArrayList<>();

        for (Map.Entry<ResourceProfile, Integer> resourceProfileCount :
                resourceRequirements.getResourcesWithCount()) {
            for (int i = 0; i < resourceProfileCount.getValue(); i++) {
                slotInfos.add(new TestingSlot(resourceProfileCount.getKey()));
            }
        }

        return slotInfos;
    }

    @Test
    void testHasEnoughResourcesUsesUnmatchedSlotsAsUnknown() {
        final int numRequiredSlots = 1;
        final ResourceCounter requiredResources =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, numRequiredSlots);
        final ResourceCounter providedResources =
                ResourceCounter.withResource(
                        ResourceProfile.newBuilder().setCpuCores(1).build(), numRequiredSlots);

        final Collection<TestingSlot> freeSlots =
                createSlotsForResourceRequirements(providedResources);

        assertThat(AdaptiveScheduler.hasDesiredResources(requiredResources, freeSlots)).isTrue();
    }

    @Test
    void testExecutionGraphGenerationWithAvailableResources() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofMillis(1L));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final int numAvailableSlots = 2;

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(numAvailableSlots);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, numAvailableSlots)),
                            taskManagerGateway);
                });

        // wait for all tasks to be submitted
        taskManagerGateway.waitForSubmissions(numAvailableSlots);

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .join();

        assertThat(executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism())
                .isEqualTo(numAvailableSlots);

        assertThat(
                        JacksonMapperFactory.createObjectMapper()
                                .readTree(executionGraph.getJsonPlan())
                                .get("nodes")
                                .size())
                .isOne();
    }

    @Test
    void testExecutionGraphGenerationSetsInitializationTimestamp() throws Exception {
        final long initializationTimestamp = 42L;
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofMillis(1L));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setInitializationTimestamp(initializationTimestamp)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(PARALLELISM);

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

        // Wait for just the first submission to indicate the execution graph is ready
        taskManagerGateway.waitForSubmissions(1);

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .join();

        assertThat(executionGraph.getStatusTimestamp(JobStatus.INITIALIZING))
                .isEqualTo(initializationTimestamp);
    }

    @Test
    void testInitializationTimestampForwarding() throws Exception {
        final long expectedInitializationTimestamp = 42L;

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setInitializationTimestamp(expectedInitializationTimestamp)
                        .build();

        final long initializationTimestamp =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getStatusTimestamp(JobStatus.INITIALIZING);

        assertThat(initializationTimestamp).isEqualTo(expectedInitializationTimestamp);
    }

    @Test
    void testFatalErrorsForwardedToFatalErrorHandler() throws Exception {
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setFatalErrorHandler(fatalErrorHandler)
                        .build();

        final RuntimeException exception = new RuntimeException();

        scheduler.runIfState(
                scheduler.getState(),
                () -> {
                    throw exception;
                });

        assertThat(fatalErrorHandler.getException()).isEqualTo(exception);
    }

    @Test
    void testResourceTimeout() throws Exception {
        final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
                new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());
        final Duration resourceTimeout = Duration.ofMinutes(1234);
        final Configuration configuration = new Configuration();

        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT, resourceTimeout);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                mainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .build();

        scheduler.startScheduling();

        // check whether some task was scheduled with the expected timeout
        // this is technically not really safe, but the chosen timeout value
        // is odd enough that it realistically won't cause issues.
        // With this approach we don't have to make assumption as to how many
        // tasks are being scheduled.
        final boolean b =
                mainThreadExecutor.getActiveNonPeriodicScheduledTask().stream()
                        .anyMatch(
                                scheduledTask ->
                                        scheduledTask.getDelay(TimeUnit.MINUTES)
                                                == resourceTimeout.toMinutes());
        assertThat(b).isTrue();
    }

    @Test
    void testNumRescalesAndStartsMetricForRescale() throws Exception {
        final CompletableFuture<Gauge<Long>> numRescalesMetricFuture = new CompletableFuture<>();
        final CompletableFuture<Gauge<Long>> numRestartsMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    if (MetricNames.NUM_RESCALES.equals(name)) {
                                        numRescalesMetricFuture.complete((Gauge<Long>) metric);
                                    } else if (MetricNames.NUM_RESTARTS.equals(name)) {
                                        numRestartsMetricFuture.complete((Gauge<Long>) metric);
                                    }
                                })
                        .build();

        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Duration.ofMinutes(10),
                        Duration.ofMinutes(10),
                        Duration.ZERO,
                        mainThreadExecutor);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(createConfigurationWithNoTimeouts())
                        .setJobManagerJobMetricGroup(
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                                metricRegistry, "localhost")
                                        .addJob(new JobID(), "jobName"))
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        final Gauge<Long> numRescalesMetric = numRescalesMetricFuture.get();
        final Gauge<Long> numRestartsMetric = numRestartsMetricFuture.get();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1 + PARALLELISM);

        taskManagerGateway.setCancelConsumer(createCancelConsumer(scheduler));

        startTestInstanceInMainThread();
        runInMainThread(
                () ->
                        declarativeSlotPool.offerSlots(
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                                new LocalTaskManagerLocation(),
                                taskManagerGateway,
                                System.currentTimeMillis()));

        // wait for the first task submission
        taskManagerGateway.waitForSubmissions(1);

        assertThat(numRescalesMetric.getValue()).isEqualTo(0L);
        assertThat(numRestartsMetric.getValue()).isEqualTo(0L);

        // offer more slots, which will cause a restart in order to scale up
        runInMainThread(
                () ->
                        offerSlots(
                                declarativeSlotPool,
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(
                                                ResourceProfile.UNKNOWN, PARALLELISM)),
                                taskManagerGateway));

        // wait for the second task submissions
        taskManagerGateway.waitForSubmissions(PARALLELISM);

        assertThat(numRescalesMetric.getValue()).isEqualTo(1L);
        assertThat(numRestartsMetric.getValue()).isEqualTo(1L);
    }

    @Test
    void testNumRescalesAndStartsMetricForFailureRecovery() throws Exception {
        final CompletableFuture<Gauge<Long>> numRescalesMetricFuture = new CompletableFuture<>();
        final CompletableFuture<Gauge<Long>> numRestartsMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    if (MetricNames.NUM_RESCALES.equals(name)) {
                                        numRescalesMetricFuture.complete((Gauge<Long>) metric);
                                    } else if (MetricNames.NUM_RESTARTS.equals(name)) {
                                        numRestartsMetricFuture.complete((Gauge<Long>) metric);
                                    }
                                })
                        .build();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(createConfigurationWithNoTimeouts())
                        .setJobManagerJobMetricGroup(
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                                metricRegistry, "localhost")
                                        .addJob(new JobID(), "jobName"))
                        .build();

        try (MockRestartingContext ctx = new MockRestartingContext()) {
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();

            final ExecutionGraphHandler executionGraphHandler =
                    new ExecutionGraphHandler(
                            executionGraph,
                            LOG,
                            ctx.getMainThreadExecutor(),
                            ctx.getMainThreadExecutor());
            final OperatorCoordinatorHandler operatorCoordinatorHandler =
                    new TestingOperatorCoordinatorHandler();
            executionGraph.transitionToRunning();

            List<ExceptionHistoryEntry> failureCollection =
                    List.of(
                            ExceptionHistoryEntry.createGlobal(
                                    new Exception("test"),
                                    CompletableFuture.completedFuture(Collections.emptyMap())));
            runInMainThread(
                    () ->
                            scheduler.goToRestarting(
                                    executionGraph,
                                    executionGraphHandler,
                                    operatorCoordinatorHandler,
                                    Duration.ZERO,
                                    true,
                                    failureCollection));
        }

        final Gauge<Long> numRestartsMetric = numRestartsMetricFuture.get();
        assertThat(numRestartsMetric.getValue()).isEqualTo(1L);

        final Gauge<Long> numScalesMetric = numRescalesMetricFuture.get();
        assertThat(numScalesMetric.getValue()).isEqualTo(0L);
    }

    @Test
    void testStatusMetrics() throws Exception {
        final CompletableFuture<UpTimeGauge> upTimeMetricFuture = new CompletableFuture<>();
        final CompletableFuture<DownTimeGauge> downTimeMetricFuture = new CompletableFuture<>();
        // restartingTime acts as a stand-in for generic status time metrics
        final CompletableFuture<Gauge<Long>> restartTimeMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    switch (name) {
                                        case UpTimeGauge.METRIC_NAME:
                                            upTimeMetricFuture.complete((UpTimeGauge) metric);
                                            break;
                                        case DownTimeGauge.METRIC_NAME:
                                            downTimeMetricFuture.complete((DownTimeGauge) metric);
                                            break;
                                        case "restartingTimeTotal":
                                            restartTimeMetricFuture.complete((Gauge<Long>) metric);
                                            break;
                                    }
                                })
                        .build();

        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = createConfigurationWithNoTimeouts();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofMillis(10L));
        configuration.set(
                MetricOptions.JOB_STATUS_METRICS,
                Arrays.asList(MetricOptions.JobStatusMetrics.TOTAL_TIME));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                                metricRegistry, "localhost")
                                        .addJob(new JobID(), "jobName"))
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        final UpTimeGauge upTimeGauge = upTimeMetricFuture.get();
        final DownTimeGauge downTimeGauge = downTimeMetricFuture.get();
        final Gauge<Long> restartTimeGauge = restartTimeMetricFuture.get();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1 + PARALLELISM);

        taskManagerGateway.setCancelConsumer(createCancelConsumer(scheduler));

        startTestInstanceInMainThread();
        runInMainThread(
                () ->
                        offerSlots(
                                declarativeSlotPool,
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                                taskManagerGateway));

        // wait for the first task submission
        taskManagerGateway.waitForSubmissions(1);

        CommonTestUtils.waitUntilCondition(() -> upTimeGauge.getValue() > 0L);
        assertThat(downTimeGauge.getValue()).isEqualTo(0L);
        assertThat(restartTimeGauge.getValue()).isEqualTo(0L);

        // offer more slots, which will cause a restart in order to scale up
        runInMainThread(
                () ->
                        offerSlots(
                                declarativeSlotPool,
                                createSlotOffersForResourceRequirements(
                                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                                taskManagerGateway));

        // wait for the second task submissions
        taskManagerGateway.waitForSubmissions(2);

        CommonTestUtils.waitUntilCondition(() -> upTimeGauge.getValue() > 0L);
        assertThat(downTimeGauge.getValue()).isEqualTo(0L);
        // can be zero if the restart is very quick
        assertThat(restartTimeGauge.getValue()).isGreaterThanOrEqualTo(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // State transition tests
    // ---------------------------------------------------------------------------------------------

    @Test
    void testStartSchedulingTransitionsToWaitingForResources() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        startTestInstanceInMainThread();

        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
    }

    @Test
    void testStartSchedulingSetsResourceRequirementsForDefaultMode() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        startTestInstanceInMainThread();

        assertThat(declarativeSlotPool.getResourceRequirements())
                .contains(ResourceRequirement.create(ResourceProfile.UNKNOWN, PARALLELISM));
    }

    @Test
    void testStartSchedulingSetsResourceRequirementsForReactiveMode() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        startTestInstanceInMainThread();

        // should request the max possible resources
        final int expectedParallelism =
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(PARALLELISM);
        assertThat(declarativeSlotPool.getResourceRequirements())
                .contains(ResourceRequirement.create(ResourceProfile.UNKNOWN, expectedParallelism));
    }

    /** Tests that the listener for new slots is properly set up. */
    @Test
    void testResourceAcquisitionTriggersJobExecution() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofMillis(1L));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(PARALLELISM);

        CompletableFuture<State> startingStateFuture = new CompletableFuture<>();
        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    startingStateFuture.complete(scheduler.getState());
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        assertThat(startingStateFuture.get()).isInstanceOf(WaitingForResources.class);

        // Wait for all tasks to be submitted
        taskManagerGateway.waitForSubmissions(PARALLELISM);

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .get();

        assertThat(executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism())
                .isEqualTo(PARALLELISM);
    }

    @Test
    void testGoToFinished() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();

        runInMainThread(() -> scheduler.goToFinished(archivedExecutionGraph));

        assertThat(scheduler.getState()).isInstanceOf(Finished.class);
    }

    @Test
    void testJobStatusListenerOnlyCalledIfJobStatusChanges() throws Exception {
        final AtomicInteger numStatusUpdates = new AtomicInteger();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobStatusListener(
                                (jobId, newJobStatus, timestamp) ->
                                        numStatusUpdates.incrementAndGet())
                        .build();

        // sanity check
        assertThat(scheduler.requestJobStatus())
                .withFailMessage("Assumption about job status for Scheduler@Created is incorrect.")
                .isEqualTo(JobStatus.INITIALIZING);

        // transition into next state, for which the job state is still INITIALIZING
        runInMainThread(
                () ->
                        scheduler.transitionToState(
                                new DummyState.Factory(scheduler, JobStatus.INITIALIZING)));

        assertThat(numStatusUpdates).hasValue(0);
    }

    @Test
    void testJobStatusListenerNotifiedOfJobStatusChanges() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofMillis(1L));

        final CompletableFuture<Void> jobCreatedNotification = new CompletableFuture<>();
        final CompletableFuture<Void> jobRunningNotification = new CompletableFuture<>();
        final CompletableFuture<Void> jobFinishedNotification = new CompletableFuture<>();
        final CompletableFuture<JobStatus> unexpectedJobStatusNotification =
                new CompletableFuture<>();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setJobStatusListener(
                                (jobId, newJobStatus, timestamp) -> {
                                    switch (newJobStatus) {
                                        case CREATED:
                                            jobCreatedNotification.complete(null);
                                            break;
                                        case RUNNING:
                                            jobRunningNotification.complete(null);
                                            break;
                                        case FINISHED:
                                            jobFinishedNotification.complete(null);
                                            break;
                                        default:
                                            unexpectedJobStatusNotification.complete(newJobStatus);
                                    }
                                })
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1 + PARALLELISM);

        runInMainThread(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                            taskManagerGateway);
                });

        // wait for the task submission
        final TaskDeploymentDescriptor submittedTask = taskManagerGateway.submittedTasks.take();

        // let the job finish
        runInMainThread(
                () ->
                        scheduler.updateTaskExecutionState(
                                new TaskExecutionState(
                                        submittedTask.getExecutionAttemptId(),
                                        ExecutionState.FINISHED)));

        jobCreatedNotification.get();
        jobRunningNotification.get();
        jobFinishedNotification.get();
        assertThat(unexpectedJobStatusNotification.isDone()).isFalse();
    }

    @Test
    void testCloseShutsDownCheckpointingComponents() throws Exception {
        final CompletableFuture<JobStatus> completedCheckpointStoreShutdownFuture =
                new CompletableFuture<>();
        final CompletedCheckpointStore completedCheckpointStore =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(
                                completedCheckpointStoreShutdownFuture);

        final CompletableFuture<JobStatus> checkpointIdCounterShutdownFuture =
                new CompletableFuture<>();
        final CheckpointIDCounter checkpointIdCounter =
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        checkpointIdCounterShutdownFuture);

        final JobGraph jobGraph = createJobGraph();
        // checkpointing components are only created if checkpointing is enabled
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(
                                        completedCheckpointStore, checkpointIdCounter))
                        .build();

        startTestInstanceInMainThread();
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        runInMainThread(
                () -> {
                    // transition into the FAILED state
                    scheduler.handleGlobalFailure(new FlinkException("Test exception"));
                    // we shouldn't block the closeAsync call here because it will trigger
                    // additional task on the main thread internally
                    FutureUtils.forward(scheduler.closeAsync(), closeFuture);
                });
        closeFuture.join();

        assertThat(completedCheckpointStoreShutdownFuture.get()).isEqualTo(JobStatus.FAILED);
        assertThat(checkpointIdCounterShutdownFuture.get()).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void testTransitionToStateCallsOnLeave() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        final LifecycleMethodCapturingState firstState =
                new LifecycleMethodCapturingState(scheduler);

        runInMainThread(() -> scheduler.transitionToState(new StateInstanceFactory(firstState)));

        firstState.reset();

        runInMainThread(() -> scheduler.transitionToState(new DummyState.Factory(scheduler)));

        assertThat(firstState.onLeaveCalled).isTrue();
        assertThat(firstState.onLeaveNewStateArgument.equals(DummyState.class)).isTrue();
    }

    @Test
    void testConsistentMaxParallelism() throws Exception {
        final int parallelism = 240;
        final int expectedMaxParallelism =
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        final JobVertex vertex = createNoOpVertex(parallelism);
        final JobGraph jobGraph = streamingJobGraph(vertex);

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(createConfigurationWithNoTimeouts())
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1 + parallelism);
        taskManagerGateway.setCancelConsumer(createCancelConsumer(scheduler));

        // offer just enough resources to run at the lowest possible parallelism
        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                            taskManagerGateway);
                });

        // Wait for task to be submitted
        taskManagerGateway.waitForSubmissions(1);

        ArchivedExecutionGraph executionGraph =
                getArchivedExecutionGraphForRunningJob(scheduler).get();
        ArchivedExecutionJobVertex archivedVertex = executionGraph.getJobVertex(vertex.getID());

        // ensure that the parallelism was submitted based on what is available
        assertThat(archivedVertex.getParallelism()).isOne();
        // and that the max parallelism was submitted based on what was configured
        assertThat(archivedVertex.getMaxParallelism()).isEqualTo(expectedMaxParallelism);

        // offer the resources to run at full parallelism
        singleThreadMainThreadExecutor.execute(
                () -> {
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, parallelism)),
                            taskManagerGateway);
                });

        // wait for the job to be re-submitted
        taskManagerGateway.waitForSubmissions(parallelism);

        ArchivedExecutionGraph resubmittedExecutionGraph =
                getArchivedExecutionGraphForRunningJob(scheduler).get();
        ArchivedExecutionJobVertex resubmittedArchivedVertex =
                resubmittedExecutionGraph.getJobVertex(vertex.getID());

        // ensure that the parallelism was submitted based on what is available
        assertThat(resubmittedArchivedVertex.getParallelism()).isEqualTo(parallelism);
        // and that the max parallelism was submitted based on what was configured
        assertThat(resubmittedArchivedVertex.getMaxParallelism()).isEqualTo(expectedMaxParallelism);
    }

    @Test
    void testRequirementIncreaseTriggersScaleUp() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler = createSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool);

        final int scaledUpParallelism = PARALLELISM * 2;

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(scaledUpParallelism, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, PARALLELISM);
        awaitJobReachingParallelism(taskManagerGateway, scheduler, PARALLELISM);

        JobResourceRequirements newJobResourceRequirements =
                createRequirementsWithUpperParallelism(scaledUpParallelism);
        singleThreadMainThreadExecutor.execute(
                () -> {
                    // first update requirements as otherwise slots are rejected!
                    scheduler.updateJobResourceRequirements(newJobResourceRequirements);
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        awaitJobReachingParallelism(taskManagerGateway, scheduler, scaledUpParallelism);
    }

    @Test
    void testRequirementDecreaseTriggersScaleDown() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler = createSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool);

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(PARALLELISM, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, PARALLELISM);
        awaitJobReachingParallelism(taskManagerGateway, scheduler, PARALLELISM);

        int scaledDownParallelism = PARALLELISM - 1;
        JobResourceRequirements newJobResourceRequirements =
                createRequirementsWithUpperParallelism(scaledDownParallelism);
        singleThreadMainThreadExecutor.execute(
                () -> scheduler.updateJobResourceRequirements(newJobResourceRequirements));

        awaitJobReachingParallelism(taskManagerGateway, scheduler, scaledDownParallelism);
    }

    @Test
    void testRequirementLowerBoundIncreaseBelowCurrentParallelismDoesNotTriggerRescale()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler = createSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool);

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(PARALLELISM, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, PARALLELISM);
        awaitJobReachingParallelism(taskManagerGateway, scheduler, PARALLELISM);

        final JobResourceRequirements newJobResourceRequirements =
                createRequirementsWithEqualLowerAndUpperParallelism(PARALLELISM);

        final CompletableFuture<Void> asyncAssertion =
                CompletableFuture.runAsync(
                        () -> {
                            State state = scheduler.getState();
                            scheduler.updateJobResourceRequirements(newJobResourceRequirements);

                            // scheduler shouldn't change states
                            assertThat(scheduler.getState()).isSameAs(state);
                            // no new tasks should have been scheduled
                            assertThat(taskManagerGateway.submittedTasks).isEmpty();
                        },
                        singleThreadMainThreadExecutor);

        FlinkAssertions.assertThatFuture(asyncAssertion).eventuallySucceeds();
    }

    @Test
    void testRequirementLowerBoundIncreaseBeyondCurrentParallelismKeepsJobRunning()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        scheduler = createSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool);
        int scaledUpParallelism = PARALLELISM * 10;

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(scaledUpParallelism, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, PARALLELISM);
        awaitJobReachingParallelism(taskManagerGateway, scheduler, PARALLELISM);

        JobResourceRequirements newJobResourceRequirements =
                createRequirementsWithEqualLowerAndUpperParallelism(scaledUpParallelism);

        FlinkAssertions.assertThatFuture(
                        CompletableFuture.runAsync(
                                () -> {
                                    final State originalState = scheduler.getState();
                                    scheduler.updateJobResourceRequirements(
                                            newJobResourceRequirements);
                                    assertThat(scheduler.getState()).isSameAs(originalState);
                                },
                                singleThreadMainThreadExecutor))
                .eventuallySucceeds();

        // adding a few slots does not cause rescale or failure
        FlinkAssertions.assertThatFuture(
                        CompletableFuture.runAsync(
                                () -> {
                                    final State originalState = scheduler.getState();
                                    offerSlots(
                                            declarativeSlotPool,
                                            createSlotOffersForResourceRequirements(
                                                    ResourceCounter.withResource(
                                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                                            taskManagerGateway);
                                    assertThat(scheduler.getState()).isSameAs(originalState);
                                },
                                singleThreadMainThreadExecutor))
                .eventuallySucceeds();

        // adding enough slots to reach minimum causes rescale
        FlinkAssertions.assertThatFuture(
                        CompletableFuture.runAsync(
                                () ->
                                        offerSlots(
                                                declarativeSlotPool,
                                                createSlotOffersForResourceRequirements(
                                                        ResourceCounter.withResource(
                                                                ResourceProfile.UNKNOWN,
                                                                PARALLELISM * 8)),
                                                taskManagerGateway),
                                singleThreadMainThreadExecutor))
                .eventuallySucceeds();

        awaitJobReachingParallelism(taskManagerGateway, scheduler, scaledUpParallelism);
    }

    @Test
    void testInitialRequirementLowerBoundBeyondAvailableSlotsCausesImmediateFailure()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final int availableSlots = 1;
        JobResourceRequirements initialJobResourceRequirements =
                createRequirementsWithEqualLowerAndUpperParallelism(PARALLELISM);

        scheduler =
                prepareSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool)
                        .withConfigurationOverride(
                                conf -> {
                                    conf.set(
                                            JobManagerOptions
                                                    .SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                                            Duration.ofMillis(1));
                                    return conf;
                                })
                        .setJobResourceRequirements(initialJobResourceRequirements)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(PARALLELISM, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, availableSlots);

        // the job will fail because not enough slots are available
        FlinkAssertions.assertThatFuture(scheduler.getJobTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(JobStatus.FAILED);
        // no task was ever submitted because we failed immediately
        assertThat(taskManagerGateway.submittedTasks).isEmpty();
    }

    @Test
    void testRequirementLowerBoundDecreaseAfterResourceScarcityBelowAvailableSlots()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID(), singleThreadMainThreadExecutor);

        final int availableSlots = 1;
        JobResourceRequirements initialJobResourceRequirements =
                createRequirementsWithEqualLowerAndUpperParallelism(PARALLELISM);

        scheduler =
                prepareSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool)
                        .setJobResourceRequirements(initialJobResourceRequirements)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                createSubmissionBufferingTaskManagerGateway(PARALLELISM, scheduler);

        startJobWithSlotsMatchingParallelism(
                scheduler, declarativeSlotPool, taskManagerGateway, availableSlots);

        // unlock job by decreasing the parallelism
        JobResourceRequirements newJobResourceRequirements =
                createRequirementsWithLowerAndUpperParallelism(availableSlots, PARALLELISM);
        singleThreadMainThreadExecutor.execute(
                () -> scheduler.updateJobResourceRequirements(newJobResourceRequirements));

        awaitJobReachingParallelism(taskManagerGateway, scheduler, availableSlots);
    }

    private static Configuration createConfigurationWithNoTimeouts() {
        return new Configuration()
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                        Duration.ofMillis(-1L))
                .set(
                        JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                        Duration.ofMillis(1L))
                .set(
                        JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                        Duration.ofMillis(1L))
                .set(
                        JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                        Duration.ofMillis(1L))
                .set(JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_MAX_DELAY, Duration.ZERO);
    }

    private AdaptiveSchedulerBuilder prepareSchedulerWithNoTimeouts(
            JobGraph jobGraph, DeclarativeSlotPool declarativeSlotPool) {
        return new AdaptiveSchedulerBuilder(
                        jobGraph, singleThreadMainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setDeclarativeSlotPool(declarativeSlotPool)
                .setJobMasterConfiguration(createConfigurationWithNoTimeouts());
    }

    private AdaptiveScheduler createSchedulerWithNoTimeouts(
            JobGraph jobGraph, DeclarativeSlotPool declarativeSlotPool) throws Exception {
        return prepareSchedulerWithNoTimeouts(jobGraph, declarativeSlotPool).build();
    }

    private SubmissionBufferingTaskManagerGateway createSubmissionBufferingTaskManagerGateway(
            int parallelism, SchedulerNG scheduler) {
        SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(parallelism);
        taskManagerGateway.setCancelConsumer(
                executionAttemptID ->
                        singleThreadMainThreadExecutor.execute(
                                () ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        executionAttemptID,
                                                        ExecutionState.CANCELED))));
        return taskManagerGateway;
    }

    private void startJobWithSlotsMatchingParallelism(
            SchedulerNG scheduler,
            DeclarativeSlotPool declarativeSlotPool,
            TaskManagerGateway taskManagerGateway,
            int parallelism) {
        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, parallelism)),
                            taskManagerGateway);
                });
    }

    private void awaitJobReachingParallelism(
            SubmissionBufferingTaskManagerGateway taskManagerGateway,
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

    private static JobResourceRequirements createRequirementsWithUpperParallelism(int parallelism) {
        return createRequirementsWithLowerAndUpperParallelism(1, parallelism);
    }

    private static JobResourceRequirements createRequirementsWithEqualLowerAndUpperParallelism(
            int parallelism) {
        return createRequirementsWithLowerAndUpperParallelism(parallelism, parallelism);
    }

    private static JobResourceRequirements createRequirementsWithLowerAndUpperParallelism(
            int lowerParallelism, int upperParallelism) {
        return new JobResourceRequirements(
                Collections.singletonMap(
                        JOB_VERTEX.getID(),
                        new JobVertexResourceRequirements(
                                new JobVertexResourceRequirements.Parallelism(
                                        lowerParallelism, upperParallelism))));
    }

    // ---------------------------------------------------------------------------------------------
    // Failure handling tests
    // ---------------------------------------------------------------------------------------------

    @Test
    void testHowToHandleFailureRejectedByStrategy() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(TraceOptions.REPORT_EVENTS_AS_SPANS, Boolean.TRUE);
        final List<Span> spanCollector = new ArrayList<>(1);
        final UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup testMetricGroup =
                createTestMetricGroup(spanCollector);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                mainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setRestartBackoffTimeStrategy(NoRestartBackoffTimeStrategy.INSTANCE)
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(testMetricGroup)
                        .build();

        assertThat(
                        scheduler
                                .howToHandleFailure(
                                        new Exception("test"), createFailureLabelsFuture())
                                .canRestart())
                .isFalse();

        assertThat(spanCollector).isEmpty();
        mainThreadExecutor.trigger();
        checkMetrics(spanCollector, false);
    }

    @Test
    void testHowToHandleFailureAllowedByStrategy() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(TraceOptions.REPORT_EVENTS_AS_SPANS, Boolean.TRUE);
        final List<Span> spanCollector = new ArrayList<>(1);
        final UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup testMetricGroup =
                createTestMetricGroup(spanCollector);
        final TestRestartBackoffTimeStrategy restartBackoffTimeStrategy =
                new TestRestartBackoffTimeStrategy(true, 1234);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                mainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setRestartBackoffTimeStrategy(restartBackoffTimeStrategy)
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(testMetricGroup)
                        .build();

        final FailureResult failureResult =
                scheduler.howToHandleFailure(new Exception("test"), createFailureLabelsFuture());

        assertThat(failureResult.canRestart()).isTrue();
        assertThat(failureResult.getBackoffTime().toMillis())
                .isEqualTo(restartBackoffTimeStrategy.getBackoffTime());

        assertThat(spanCollector).isEmpty();
        mainThreadExecutor.trigger();
        checkMetrics(spanCollector, true);
    }

    @Test
    void testHowToHandleFailureUnrecoverableFailure() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(TraceOptions.REPORT_EVENTS_AS_SPANS, Boolean.TRUE);
        final List<Span> spanCollector = new ArrayList<>(1);
        final UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup testMetricGroup =
                createTestMetricGroup(spanCollector);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                mainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(testMetricGroup)
                        .build();

        assertThat(
                        scheduler
                                .howToHandleFailure(
                                        new SuppressRestartsException(new Exception("test")),
                                        createFailureLabelsFuture())
                                .canRestart())
                .isFalse();

        assertThat(spanCollector).isEmpty();
        mainThreadExecutor.trigger();
        checkMetrics(spanCollector, false);
    }

    @Test
    void testExceptionHistoryWithGlobalFailureLabels() throws Exception {
        final Exception expectedException = new Exception("Global Exception to label");
        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> scheduler.handleGlobalFailure(expectedException);

        final TestingFailureEnricher failureEnricher = new TestingFailureEnricher();
        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .withFailureEnrichers(Collections.singletonList(failureEnricher))
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();
        assertThat(failure.getTaskManagerLocation()).isNull();
        assertThat(failure.getFailingTaskName()).isNull();
        assertThat(failureEnricher.getSeenThrowables()).containsExactly(expectedException);
        assertThat(failure.getFailureLabels()).isEqualTo(failureEnricher.getFailureLabels());

        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException);
    }

    @Test
    void testExceptionHistoryWithGlobalFailure() throws Exception {
        final Exception expectedException = new Exception("Expected Global Exception");
        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> scheduler.handleGlobalFailure(expectedException);

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();
        assertThat(failure.getTaskManagerLocation()).isNull();
        assertThat(failure.getFailingTaskName()).isNull();

        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException);
    }

    /** Verify AdaptiveScheduler propagates failure labels as generated by Failure Enrichers. */
    @Test
    void testExceptionHistoryWithTaskFailureLabels() throws Exception {
        final Exception taskException = new Exception("Task Exception");
        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    final ExecutionAttemptID attemptId = attemptIds.get(1);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId, ExecutionState.FAILED, taskException)));
                };

        final TestingFailureEnricher failureEnricher = new TestingFailureEnricher();
        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withFailureEnrichers(Collections.singletonList(failureEnricher))
                        .withTestLogic(testLogic)
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();

        assertThat(failure.getException().deserializeError(classLoader)).isEqualTo(taskException);
        assertThat(failure.getFailureLabels()).isEqualTo(failureEnricher.getFailureLabels());
    }

    @Test
    void testExceptionHistoryWithTaskFailure() throws Exception {
        final Exception expectedException = new Exception("Expected Local Exception");
        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    final ExecutionAttemptID attemptId = attemptIds.get(1);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId, ExecutionState.FAILED, expectedException)));
                };

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();

        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException);
    }

    @Test
    void testExceptionHistoryWithTaskFailureWithRestart() throws Exception {
        final Exception expectedException = new Exception("Expected Local Exception");
        final Consumer<AdaptiveSchedulerBuilder> setupScheduler =
                builder ->
                        builder.setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategy
                                                .FixedDelayRestartBackoffTimeStrategyFactory(1, 100)
                                        .create());
        final BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    final ExecutionAttemptID attemptId = attemptIds.get(1);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId, ExecutionState.FAILED, expectedException)));
                };
        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .withModifiedScheduler(setupScheduler)
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();

        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException);
    }

    @Test
    void testExceptionHistoryWithTaskFailureFromStopWithSavepoint() throws Exception {
        final Exception expectedException = new Exception("Expected Local Exception");
        Consumer<JobGraph> setupJobGraph =
                jobGraph ->
                        jobGraph.setSnapshotSettings(
                                new JobCheckpointingSettings(
                                        // set a large checkpoint interval so we can easily deduce
                                        // the savepoints checkpoint id
                                        CheckpointCoordinatorConfiguration.builder()
                                                .setCheckpointInterval(Long.MAX_VALUE)
                                                .build(),
                                        null));
        final CompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final CheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();
        final CheckpointsCleaner checkpointCleaner = new CheckpointsCleaner();
        TestingCheckpointRecoveryFactory checkpointRecoveryFactory =
                new TestingCheckpointRecoveryFactory(completedCheckpointStore, checkpointIDCounter);

        Consumer<AdaptiveSchedulerBuilder> setupScheduler =
                builder ->
                        builder.setCheckpointRecoveryFactory(checkpointRecoveryFactory)
                                .setCheckpointCleaner(checkpointCleaner);

        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    final ExecutionAttemptID attemptId = attemptIds.get(1);

                    scheduler.stopWithSavepoint(
                            "file:///tmp/target", true, SavepointFormatType.CANONICAL);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId, ExecutionState.FAILED, expectedException)));

                    // fail the savepoint so that the job terminates
                    for (ExecutionAttemptID id : attemptIds) {
                        scheduler.declineCheckpoint(
                                new DeclineCheckpoint(
                                        scheduler.requestJob().getJobId(),
                                        id,
                                        checkpointIDCounter.get() - 1,
                                        new CheckpointException(
                                                CheckpointFailureReason.IO_EXCEPTION)));
                    }
                };

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .withModifiedScheduler(setupScheduler)
                        .withModifiedJobGraph(setupJobGraph)
                        .run();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();

        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException);
    }

    @Test
    void testExceptionHistoryWithTaskConcurrentGlobalFailure() throws Exception {
        final Exception expectedException1 = new Exception("Expected Global Exception 1");
        final Exception expectedException2 = new Exception("Expected Global Exception 2");
        final BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    scheduler.handleGlobalFailure(expectedException1);
                    scheduler.handleGlobalFailure(expectedException2);
                };

        final Iterable<RootExceptionHistoryEntry> entries =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .run();
        assertThat(entries).hasSize(1);
        final RootExceptionHistoryEntry failure = entries.iterator().next();
        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException1);
        final Iterable<ExceptionHistoryEntry> concurrentExceptions =
                failure.getConcurrentExceptions();
        final List<Throwable> foundExceptions =
                IterableUtils.toStream(concurrentExceptions)
                        .map(ExceptionHistoryEntry::getException)
                        .map(exception -> exception.deserializeError(classLoader))
                        .collect(Collectors.toList());

        assertThat(foundExceptions).containsExactly(expectedException2);
    }

    @Test
    void testExceptionHistoryWithTaskConcurrentFailure() throws Exception {
        final Exception expectedException1 = new Exception("Expected Local Exception 1");
        final Exception expectedException2 = new Exception("Expected Local Exception 2");
        BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attemptIds) -> {
                    final ExecutionAttemptID attemptId = attemptIds.remove(0);
                    final ExecutionAttemptID attemptId2 = attemptIds.remove(0);

                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId, ExecutionState.FAILED, expectedException1)));
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionStateTransition(
                                    new TaskExecutionState(
                                            attemptId2,
                                            ExecutionState.FAILED,
                                            expectedException2)));
                };

        final Iterable<RootExceptionHistoryEntry> entries =
                new ExceptionHistoryTester(singleThreadMainThreadExecutor)
                        .withTestLogic(testLogic)
                        .run();
        assertThat(entries).hasSize(1);
        final RootExceptionHistoryEntry failure = entries.iterator().next();
        assertThat(failure.getException().deserializeError(classLoader))
                .isEqualTo(expectedException1);
        final Iterable<ExceptionHistoryEntry> concurrentExceptions =
                failure.getConcurrentExceptions();
        final List<Throwable> foundExceptions =
                IterableUtils.toStream(concurrentExceptions)
                        .map(ExceptionHistoryEntry::getException)
                        .map(exception -> exception.deserializeError(classLoader))
                        .collect(Collectors.toList());

        // In the future, concurrent local failures should be stored.
        assertThat(foundExceptions).isEmpty();
    }

    @Test
    void testRepeatedTransitionIntoCurrentStateFails() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        final State state = scheduler.getState();

        // safeguard for this test
        assertThat(state).isInstanceOf(Created.class);

        assertThatThrownBy(() -> scheduler.transitionToState(new Created.Factory(scheduler, LOG)))
                .isInstanceOf(IllegalStateException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Illegal state behavior tests
    // ---------------------------------------------------------------------------------------------

    @Test
    void testTriggerSavepointFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatFuture(
                        scheduler.triggerSavepoint(
                                "some directory", false, SavepointFormatType.CANONICAL))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(CheckpointException.class);
    }

    @Test
    void testStopWithSavepointFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatFuture(
                        scheduler.triggerSavepoint(
                                "some directory", false, SavepointFormatType.CANONICAL))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(CheckpointException.class);
    }

    @Test
    void testDeliverOperatorEventToCoordinatorFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatThrownBy(
                        () ->
                                scheduler.deliverOperatorEventToCoordinator(
                                        createExecutionAttemptId(),
                                        new OperatorID(),
                                        new TestOperatorEvent()))
                .isInstanceOf(TaskNotRunningException.class);
    }

    @Test
    void testDeliverCoordinationRequestToCoordinatorFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatFuture(
                        scheduler.deliverCoordinationRequestToCoordinator(
                                new OperatorID(), new CoordinationRequest() {}))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkException.class);
    }

    @Test
    void testUpdateTaskExecutionStateReturnsFalseInIllegalState() throws Throwable {
        final JobGraph jobGraph = createJobGraph();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThat(
                        scheduler.updateTaskExecutionState(
                                new TaskExecutionStateTransition(
                                        new TaskExecutionState(
                                                createExecutionAttemptId(),
                                                ExecutionState.FAILED))))
                .isFalse();
    }

    @Test
    void testRequestNextInputSplitFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatThrownBy(
                        () ->
                                scheduler.requestNextInputSplit(
                                        JOB_VERTEX.getID(), createExecutionAttemptId()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testRequestPartitionStateFailsInIllegalState() throws Exception {
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .build();

        assertThatThrownBy(
                        () ->
                                scheduler.requestPartitionState(
                                        new IntermediateDataSetID(), new ResultPartitionID()))
                .isInstanceOf(PartitionProducerDisposedException.class);
    }

    @Test
    void testTryToAssignSlotsReturnsNotPossibleIfExpectedResourcesAreNotAvailable()
            throws Exception {

        final TestingSlotAllocator slotAllocator =
                TestingSlotAllocator.newBuilder()
                        .setTryReserveResourcesFunction(ignored -> Optional.empty())
                        .build();

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setSlotAllocator(slotAllocator)
                        .build();

        final CreatingExecutionGraph.AssignmentResult assignmentResult =
                scheduler.tryToAssignSlots(
                        CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create(
                                new StateTrackingMockExecutionGraph(), JobSchedulingPlan.empty()));

        assertThat(assignmentResult.isSuccess()).isFalse();
    }

    @Test
    void testComputeVertexParallelismStoreForExecutionInReactiveMode() {
        JobVertex v1 = createNoOpVertex("v1", 1, 50);
        JobVertex v2 = createNoOpVertex("v2", 50, 50);
        JobGraph graph = streamingJobGraph(v1, v2);

        VertexParallelismStore parallelismStore =
                AdaptiveScheduler.computeVertexParallelismStoreForExecution(
                        graph,
                        SchedulerExecutionMode.REACTIVE,
                        SchedulerBase::getDefaultMaxParallelism);

        for (JobVertex vertex : graph.getVertices()) {
            VertexParallelismInformation info = parallelismStore.getParallelismInfo(vertex.getID());

            assertThat(info.getParallelism()).isEqualTo(vertex.getParallelism());
            assertThat(info.getMaxParallelism()).isEqualTo(vertex.getMaxParallelism());
        }
    }

    @Test
    void testComputeVertexParallelismStoreForExecutionInDefaultMode() {
        JobVertex v1 = createNoOpVertex("v1", 1, 50);
        JobVertex v2 = createNoOpVertex("v2", 50, 50);
        JobGraph graph = streamingJobGraph(v1, v2);

        VertexParallelismStore parallelismStore =
                AdaptiveScheduler.computeVertexParallelismStoreForExecution(
                        graph, null, SchedulerBase::getDefaultMaxParallelism);

        for (JobVertex vertex : graph.getVertices()) {
            VertexParallelismInformation info = parallelismStore.getParallelismInfo(vertex.getID());

            assertThat(info.getParallelism()).isEqualTo(vertex.getParallelism());
            assertThat(info.getMaxParallelism()).isEqualTo(vertex.getMaxParallelism());
        }
    }

    @Test
    void testCheckpointCleanerIsClosedAfterCheckpointServices() throws Exception {
        final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        try {
            DefaultSchedulerTest.doTestCheckpointCleanerIsClosedAfterCheckpointServices(
                    (checkpointRecoveryFactory, checkpointCleaner) -> {
                        final JobGraph jobGraph = createJobGraph();
                        enableCheckpointing(jobGraph);
                        try {
                            return new AdaptiveSchedulerBuilder(
                                            jobGraph,
                                            ComponentMainThreadExecutorServiceAdapter
                                                    .forSingleThreadExecutor(executorService),
                                            EXECUTOR_RESOURCE.getExecutor())
                                    .setCheckpointRecoveryFactory(checkpointRecoveryFactory)
                                    .setCheckpointCleaner(checkpointCleaner)
                                    .build();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    executorService,
                    LOG);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testIdleSlotsAreReleasedAfterDownScalingTriggeredByLoweredResourceRequirements()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final Duration slotIdleTimeout = Duration.ofMillis(10);

        final Configuration configuration = createConfigurationWithNoTimeouts();
        configuration.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, slotIdleTimeout);

        final DeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(
                        jobGraph.getJobID(), singleThreadMainThreadExecutor, slotIdleTimeout);
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final int numInitialSlots = 4;
        final int numSlotsAfterDownscaling = 2;

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(numInitialSlots);

        taskManagerGateway.setCancelConsumer(createCancelConsumer(scheduler));

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, numInitialSlots)),
                            taskManagerGateway);
                });

        // wait for all tasks to be submitted
        taskManagerGateway.waitForSubmissions(numInitialSlots);

        // lower the resource requirements
        singleThreadMainThreadExecutor.execute(
                () ->
                        scheduler.updateJobResourceRequirements(
                                JobResourceRequirements.newBuilder()
                                        .setParallelismForJobVertex(
                                                JOB_VERTEX.getID(), 1, numSlotsAfterDownscaling)
                                        .build()));

        // job should be resubmitted with lower parallelism
        taskManagerGateway.waitForSubmissions(numSlotsAfterDownscaling);

        // and excessive slots should be freed
        taskManagerGateway.waitForFreedSlots(numInitialSlots - numSlotsAfterDownscaling);

        final CompletableFuture<JobStatus> jobStatusFuture = new CompletableFuture<>();
        singleThreadMainThreadExecutor.execute(
                () -> jobStatusFuture.complete(scheduler.getState().getJobStatus()));
        assertThatFuture(jobStatusFuture).eventuallySucceeds().isEqualTo(JobStatus.RUNNING);

        // make sure we haven't freed up any more slots
        assertThat(taskManagerGateway.freedSlots).isEmpty();
    }

    @Test
    void testUpdateResourceRequirementsInReactiveModeIsNotSupported() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .build();
        assertThatThrownBy(
                        () ->
                                scheduler.updateJobResourceRequirements(
                                        JobResourceRequirements.empty()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRequestDefaultResourceRequirements() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final Configuration configuration = new Configuration();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .build();
        assertThat(scheduler.requestJobResourceRequirements())
                .isEqualTo(
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        JOB_VERTEX.getID(), 1, JOB_VERTEX.getParallelism())
                                .build());
    }

    @Test
    void testRequestDefaultResourceRequirementsInReactiveMode() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .build();
        assertThat(scheduler.requestJobResourceRequirements())
                .isEqualTo(
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        JOB_VERTEX.getID(),
                                        1,
                                        SchedulerBase.getDefaultMaxParallelism(JOB_VERTEX))
                                .build());
    }

    @Test
    void testRequestUpdatedResourceRequirements() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final Configuration configuration = new Configuration();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .build();
        final JobResourceRequirements newJobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(JOB_VERTEX.getID(), 1, 12)
                        .build();
        assertThat(scheduler.requestJobResourceRequirements())
                .isNotEqualTo(newJobResourceRequirements);
        scheduler.updateJobResourceRequirements(newJobResourceRequirements);
        assertThat(scheduler.requestJobResourceRequirements())
                .isEqualTo(newJobResourceRequirements);

        final JobResourceRequirements newJobResourceRequirements2 =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(JOB_VERTEX.getID(), 4, 12)
                        .build();
        assertThat(scheduler.requestJobResourceRequirements())
                .isNotEqualTo(newJobResourceRequirements2);
        scheduler.updateJobResourceRequirements(newJobResourceRequirements2);
        assertThat(scheduler.requestJobResourceRequirements())
                .isEqualTo(newJobResourceRequirements2);
    }

    @Test
    void testScalingIntervalConfigurationIsRespected() throws ConfigurationException {
        final Duration executingCooldownTimeout = Duration.ofMillis(1337);
        final Duration scalingStabilizationTimeout = Duration.ofMillis(7331);
        final Configuration configuration = createConfigurationWithNoTimeouts();
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                executingCooldownTimeout);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                scalingStabilizationTimeout);

        final AdaptiveScheduler.Settings settings = AdaptiveScheduler.Settings.of(configuration);
        assertThat(settings.getExecutingCooldownTimeout()).isEqualTo(executingCooldownTimeout);
        assertThat(settings.getExecutingResourceStabilizationTimeout())
                .isEqualTo(scalingStabilizationTimeout);
    }

    @Test
    void testOnCompletedCheckpointIsHandledInMainThread() throws Exception {
        testCheckpointStatsEventBeingExecutedInTheMainThread(
                CheckpointStatsListener::onCompletedCheckpoint, 1, Integer.MAX_VALUE);
    }

    @Test
    void testOnFailedCheckpointIsHandledInMainThread() throws Exception {
        testCheckpointStatsEventBeingExecutedInTheMainThread(
                CheckpointStatsListener::onFailedCheckpoint, 2, 2);
    }

    private void testCheckpointStatsEventBeingExecutedInTheMainThread(
            Consumer<CheckpointStatsListener> eventCallback,
            int eventRepetitions,
            int triggerOnFailedCheckpointCount)
            throws Exception {

        final CompletableFuture<CheckpointStatsListener> statsListenerInstantiatedFuture =
                new CompletableFuture<>();
        final BlockingQueue<Integer> eventQueue = new ArrayBlockingQueue<>(1);

        scheduler =
                createSchedulerThatReachesExecutingState(
                        PARALLELISM,
                        triggerOnFailedCheckpointCount,
                        eventQueue,
                        statsListenerInstantiatedFuture);

        // start scheduling to reach Executing state
        singleThreadMainThreadExecutor.execute(scheduler::startScheduling);

        final CheckpointStatsListener statsListener = statsListenerInstantiatedFuture.get();
        assertThat(statsListener)
                .as("The CheckpointStatsListener should have been instantiated.")
                .isNotNull();

        // the first trigger happens in the Executing initialization - let's wait for that event
        // to pass
        assertThat(eventQueue.take())
                .as(
                        "The first event should have been appeared during Executing state initialization and should be ignored.")
                .isEqualTo(0);

        // counting the failed checkpoints only starts on a change event
        scheduler.updateJobResourceRequirements(
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(JOB_VERTEX.getID(), 1, PARALLELISM - 1)
                        .build());

        for (int i = 0; i < eventRepetitions; i++) {
            assertThatNoException()
                    .as(
                            "Triggering the event from outside the main thread should not have caused an error.")
                    .isThrownBy(() -> eventCallback.accept(statsListener));
        }

        assertThat(eventQueue.take()).as("Only one event should have been observed.").isEqualTo(1);
    }

    @Test
    void testGoToWaitingForResourcesConfiguresStateTransitionManagerFactory() throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final TestingStateTransitionManagerFactory factory =
                new TestingStateTransitionManagerFactory(
                        ctx ->
                                TestingStateTransitionManager.withOnChangeEventOnly(
                                        () -> {
                                            if (ctx instanceof WaitingForResources) {
                                                latch.trigger();
                                            }
                                        }));

        final Configuration configuration = new Configuration();
        final Duration resourceStabilizationTimeout = Duration.ofMillis(10L);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                resourceStabilizationTimeout);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setStateTransitionManagerFactory(factory)
                        .setJobMasterConfiguration(configuration)
                        .build();

        // start scheduling to reach Executing state
        singleThreadMainThreadExecutor.execute(scheduler::startScheduling);

        // let's wait for the onChange event in Executing state.
        latch.await();

        assertThat(scheduler.getState()).isInstanceOf(WaitingForResources.class);
        assertThat(factory.cooldownTimeout).isEqualTo(Duration.ZERO);
        assertThat(factory.maximumDelayForTrigger).isEqualTo(Duration.ZERO);
        assertThat(factory.resourceStabilizationTimeout).isEqualTo(resourceStabilizationTimeout);
    }

    @Test
    void testGoToExecutingConfiguresStateTransitionManagerFactory() throws Exception {
        final OneShotLatch latch = new OneShotLatch();
        final TestingStateTransitionManagerFactory factory =
                new TestingStateTransitionManagerFactory(
                        ctx ->
                                TestingStateTransitionManager.withOnChangeEventOnly(
                                        () -> {
                                            if (ctx instanceof WaitingForResources) {
                                                ctx.transitionToSubsequentState();
                                            }
                                            if (ctx instanceof Executing) {
                                                latch.trigger();
                                            }
                                        }));

        final Configuration configuration = new Configuration();
        final Duration executingCooldownTimeout = Duration.ofMillis(1L);
        final Duration executingResourceStabilizationTimeout = Duration.ofMillis(5L);
        final Duration maxDelayForTrigger = Duration.ofMillis(10L);

        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                executingCooldownTimeout);
        configuration.set(
                JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_MAX_DELAY, maxDelayForTrigger);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                executingResourceStabilizationTimeout);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                createJobGraph(),
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(configuration)
                        .setDeclarativeSlotPool(getSlotPoolWithFreeSlots(PARALLELISM))
                        .setStateTransitionManagerFactory(factory)
                        .build();

        // start scheduling to reach Executing state
        singleThreadMainThreadExecutor.execute(scheduler::startScheduling);

        // let's wait for the onChange event in Executing state.
        latch.await();

        assertThat(scheduler.getState()).isInstanceOf(Executing.class);
        assertThat(factory.cooldownTimeout).isEqualTo(executingCooldownTimeout);
        assertThat(factory.maximumDelayForTrigger).isEqualTo(maxDelayForTrigger);
        assertThat(factory.resourceStabilizationTimeout)
                .isEqualTo(executingResourceStabilizationTimeout);
    }

    // ---------------------------------------------------------------------------------------------
    // Utils
    // ---------------------------------------------------------------------------------------------

    private static class TestingStateTransitionManagerFactory
            implements AdaptiveScheduler.StateTransitionManagerFactory {

        private final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerCreator;

        private Duration cooldownTimeout;
        private Duration resourceStabilizationTimeout;
        private Duration maximumDelayForTrigger;

        public TestingStateTransitionManagerFactory(
                Function<StateTransitionManager.Context, StateTransitionManager>
                        stateTransitionManagerCreator) {
            this.stateTransitionManagerCreator = stateTransitionManagerCreator;
        }

        public StateTransitionManager create(
                StateTransitionManager.Context context,
                Supplier<Temporal> ignoredClock,
                Duration cooldownTimeout,
                Duration resourceStabilizationTimeout,
                Duration maximumDelayForTrigger) {
            this.cooldownTimeout = cooldownTimeout;
            this.resourceStabilizationTimeout = resourceStabilizationTimeout;
            this.maximumDelayForTrigger = maximumDelayForTrigger;

            return stateTransitionManagerCreator.apply(context);
        }
    }

    private AdaptiveScheduler createSchedulerThatReachesExecutingState(
            int parallelism,
            int onFailedCheckpointCount,
            BlockingQueue<Integer> eventQueue,
            CompletableFuture<CheckpointStatsListener> statsListenerInstantiatedFuture)
            throws Exception {
        final Configuration config = new Configuration();
        config.set(
                JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_MAX_CHECKPOINT_FAILURES,
                onFailedCheckpointCount);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Collections.singletonList(JOB_VERTEX))
                        .setJobCheckpointingSettings(
                                new JobCheckpointingSettings(
                                        new CheckpointCoordinatorConfiguration
                                                        .CheckpointCoordinatorConfigurationBuilder()
                                                .build(),
                                        null))
                        .build();
        SchedulerTestingUtils.enableCheckpointing(jobGraph);

        final DeclarativeSlotPool slotPool = getSlotPoolWithFreeSlots(parallelism);
        final AtomicInteger eventCounter = new AtomicInteger();
        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setJobMasterConfiguration(config)
                        .setDeclarativeSlotPool(slotPool)
                        .setStateTransitionManagerFactory(
                                (context,
                                        ignoredClock,
                                        ignoredCooldown,
                                        ignoredResourceStabilizationTimeout,
                                        ignoredMaxTriggerDelay) ->
                                        TestingStateTransitionManager.withOnTriggerEventOnly(
                                                () -> {
                                                    singleThreadMainThreadExecutor
                                                            .assertRunningInMainThread();

                                                    if (context instanceof WaitingForResources) {
                                                        context.transitionToSubsequentState();
                                                    } else if (context instanceof Executing) {
                                                        eventQueue.offer(
                                                                eventCounter.getAndIncrement());
                                                    }
                                                }))
                        .setCheckpointStatsTrackerFactory(
                                (metricGroup, listener) -> {
                                    assertThat(statsListenerInstantiatedFuture)
                                            .as(
                                                    "The CheckpointStatsListener should be only instantiated once.")
                                            .isNotCompleted();
                                    statsListenerInstantiatedFuture.complete(listener);
                                    return NoOpCheckpointStatsTracker.INSTANCE;
                                })
                        .build();
        return scheduler;
    }

    private CompletableFuture<ArchivedExecutionGraph> getArchivedExecutionGraphForRunningJob(
            SchedulerNG scheduler) {
        return CompletableFuture.supplyAsync(
                () -> {
                    ArchivedExecutionGraph graph = null;
                    while (graph == null || graph.getState() != JobStatus.RUNNING) {
                        graph = scheduler.requestJob().getArchivedExecutionGraph();
                    }
                    return graph;
                },
                singleThreadMainThreadExecutor);
    }

    private Consumer<ExecutionAttemptID> createCancelConsumer(SchedulerNG scheduler) {
        return executionAttemptId ->
                singleThreadMainThreadExecutor.execute(
                        () ->
                                scheduler.updateTaskExecutionState(
                                        new TaskExecutionState(
                                                executionAttemptId, ExecutionState.CANCELED)));
    }

    private static DefaultDeclarativeSlotPool createDeclarativeSlotPool(
            JobID jobId, ComponentMainThreadExecutor mainThreadExecutor) {
        return createDeclarativeSlotPool(jobId, mainThreadExecutor, DEFAULT_TIMEOUT);
    }

    private static DefaultDeclarativeSlotPool createDeclarativeSlotPool(
            JobID jobId, ComponentMainThreadExecutor mainThreadExecutor, Duration idleSlotTimeout) {
        return new DefaultDeclarativeSlotPool(
                jobId,
                new DefaultAllocatedSlotPool(),
                ignored -> {},
                idleSlotTimeout,
                DEFAULT_TIMEOUT,
                Duration.ZERO,
                mainThreadExecutor);
    }

    /**
     * Creates a testing SlotPool instance that would allow for the scheduler to transition to
     * Executing state.
     */
    private static DeclarativeSlotPool getSlotPoolWithFreeSlots(int freeSlots) {
        return new TestingDeclarativeSlotPoolBuilder()
                .setContainsFreeSlotFunction(allocationID -> true)
                .setReserveFreeSlotFunction(
                        (allocationId, resourceProfile) ->
                                TestingPhysicalSlot.builder()
                                        .withAllocationID(allocationId)
                                        .build())
                .setGetFreeSlotTrackerSupplier(
                        () ->
                                TestingFreeSlotTracker.newBuilder()
                                        .setGetFreeSlotsInformationSupplier(
                                                () ->
                                                        IntStream.range(0, freeSlots)
                                                                .mapToObj(v -> new TestingSlot())
                                                                .collect(Collectors.toSet()))
                                        .build())
                .build();
    }

    private static JobGraph createJobGraph() {
        return streamingJobGraph(JOB_VERTEX);
    }

    private static class LifecycleMethodCapturingState extends DummyState {
        boolean onLeaveCalled = false;
        @Nullable Class<? extends State> onLeaveNewStateArgument = null;

        public LifecycleMethodCapturingState(Context context) {
            super(context);
        }

        void reset() {
            onLeaveCalled = false;
            onLeaveNewStateArgument = null;
        }

        @Override
        public void onLeave(Class<? extends State> newState) {
            onLeaveCalled = true;
            onLeaveNewStateArgument = newState;
        }
    }

    /**
     * A {@link SimpleAckingTaskManagerGateway} that buffers all the task submissions into a
     * blocking queue, allowing one to wait for an arbitrary number of submissions.
     */
    public static class SubmissionBufferingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        final BlockingQueue<TaskDeploymentDescriptor> submittedTasks;
        final BlockingQueue<AllocationID> freedSlots;

        public SubmissionBufferingTaskManagerGateway(int capacity) {
            submittedTasks = new ArrayBlockingQueue<>(capacity);
            freedSlots = new ArrayBlockingQueue<>(capacity);
            initializeFunctions();
        }

        @Override
        public void setSubmitConsumer(Consumer<TaskDeploymentDescriptor> submitConsumer) {
            super.setSubmitConsumer(
                    taskDeploymentDescriptor -> {
                        Preconditions.checkState(submittedTasks.offer(taskDeploymentDescriptor));
                        submitConsumer.accept(taskDeploymentDescriptor);
                    });
        }

        @Override
        public void setFreeSlotFunction(
                BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>>
                        freeSlotFunction) {
            super.setFreeSlotFunction(
                    (allocationID, throwable) -> {
                        Preconditions.checkState(freedSlots.offer(allocationID));
                        return freeSlotFunction.apply(allocationID, throwable);
                    });
        }

        /**
         * Block until an arbitrary number of submissions have been received.
         *
         * @param numSubmissions The number of submissions to wait for
         * @return the list of the waited-for submissions
         * @throws InterruptedException if a timeout is exceeded waiting for a submission
         */
        public List<TaskDeploymentDescriptor> waitForSubmissions(int numSubmissions)
                throws InterruptedException {
            List<TaskDeploymentDescriptor> descriptors = new ArrayList<>();
            for (int i = 0; i < numSubmissions; i++) {
                descriptors.add(submittedTasks.take());
            }
            return descriptors;
        }

        public List<AllocationID> waitForFreedSlots(int numFreedSlots) throws InterruptedException {
            final List<AllocationID> allocationIds = new ArrayList<>();
            for (int i = 0; i < numFreedSlots; i++) {
                allocationIds.add(freedSlots.take());
            }
            return allocationIds;
        }

        private void initializeFunctions() {
            setSubmitConsumer(ignored -> {});
            setFreeSlotFunction(
                    (allocationId, throwable) ->
                            CompletableFuture.completedFuture(Acknowledge.get()));
        }
    }

    private static class StateInstanceFactory
            implements StateFactory<LifecycleMethodCapturingState> {

        private final LifecycleMethodCapturingState instance;

        public StateInstanceFactory(LifecycleMethodCapturingState instance) {
            this.instance = instance;
        }

        @Override
        public Class<LifecycleMethodCapturingState> getStateClass() {
            return LifecycleMethodCapturingState.class;
        }

        @Override
        public LifecycleMethodCapturingState getState() {
            return instance;
        }
    }

    static class DummyState extends StateWithoutExecutionGraph {

        private final JobStatus jobStatus;

        public DummyState(StateWithoutExecutionGraph.Context context) {
            this(context, JobStatus.RUNNING);
        }

        public DummyState(StateWithoutExecutionGraph.Context context, JobStatus jobStatus) {
            super(context, AdaptiveSchedulerTest.LOG);
            this.jobStatus = jobStatus;
        }

        @Override
        public JobStatus getJobStatus() {
            return jobStatus;
        }

        private static class Factory implements StateFactory<DummyState> {

            private final StateWithoutExecutionGraph.Context context;
            private final JobStatus jobStatus;

            public Factory(StateWithoutExecutionGraph.Context context) {
                this(context, JobStatus.RUNNING);
            }

            public Factory(StateWithoutExecutionGraph.Context context, JobStatus jobStatus) {
                this.context = context;
                this.jobStatus = jobStatus;
            }

            @Override
            public Class<DummyState> getStateClass() {
                return DummyState.class;
            }

            @Override
            public DummyState getState() {
                return new DummyState(context, jobStatus);
            }
        }
    }

    private static class ExceptionHistoryTester {
        private final ComponentMainThreadExecutor mainThreadExecutor;
        private BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic =
                (scheduler, attempts) -> {};
        private Consumer<AdaptiveSchedulerBuilder> schedulerModifier = ignored -> {};
        private Consumer<JobGraph> jobGraphModifier = ignored -> {};
        private Collection<FailureEnricher> failureEnrichers = Collections.emptySet();

        ExceptionHistoryTester(ComponentMainThreadExecutor mainThreadExecutor) {
            this.mainThreadExecutor = mainThreadExecutor;
        }

        ExceptionHistoryTester withTestLogic(
                BiConsumer<AdaptiveScheduler, List<ExecutionAttemptID>> testLogic) {
            this.testLogic = testLogic;
            return this;
        }

        ExceptionHistoryTester withModifiedScheduler(
                Consumer<AdaptiveSchedulerBuilder> schedulerModifier) {
            this.schedulerModifier = schedulerModifier;
            return this;
        }

        ExceptionHistoryTester withModifiedJobGraph(Consumer<JobGraph> jobGraphModifier) {
            this.jobGraphModifier = jobGraphModifier;
            return this;
        }

        ExceptionHistoryTester withFailureEnrichers(Collection<FailureEnricher> failureEnrichers) {
            this.failureEnrichers = failureEnrichers;
            return this;
        }

        Iterable<RootExceptionHistoryEntry> run() throws Exception {
            final JobGraph jobGraph = createJobGraph();
            jobGraphModifier.accept(jobGraph);

            final CompletedCheckpointStore completedCheckpointStore =
                    new StandaloneCompletedCheckpointStore(1);
            final CheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();
            final CheckpointsCleaner checkpointCleaner = new CheckpointsCleaner();
            TestingCheckpointRecoveryFactory checkpointRecoveryFactory =
                    new TestingCheckpointRecoveryFactory(
                            completedCheckpointStore, checkpointIDCounter);

            final DefaultDeclarativeSlotPool declarativeSlotPool =
                    createDeclarativeSlotPool(jobGraph.getJobID(), mainThreadExecutor);

            final Configuration configuration = new Configuration();
            configuration.set(
                    JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                    Duration.ofMillis(1L));

            AdaptiveSchedulerBuilder builder =
                    new AdaptiveSchedulerBuilder(
                                    jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                            .setJobMasterConfiguration(configuration)
                            .setDeclarativeSlotPool(declarativeSlotPool)
                            .setCheckpointRecoveryFactory(checkpointRecoveryFactory)
                            .setCheckpointCleaner(checkpointCleaner)
                            .setFailureEnrichers(failureEnrichers);
            schedulerModifier.accept(builder);
            final AdaptiveScheduler scheduler = builder.build();

            try {
                final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                        new SubmissionBufferingTaskManagerGateway(PARALLELISM);
                taskManagerGateway.setCancelConsumer(
                        attemptId ->
                                mainThreadExecutor.execute(
                                        () ->
                                                scheduler.updateTaskExecutionState(
                                                        new TaskExecutionStateTransition(
                                                                new TaskExecutionState(
                                                                        attemptId,
                                                                        ExecutionState.CANCELED,
                                                                        null)))));

                mainThreadExecutor.execute(
                        () -> {
                            scheduler.startScheduling();
                            offerSlots(
                                    declarativeSlotPool,
                                    createSlotOffersForResourceRequirements(
                                            ResourceCounter.withResource(
                                                    ResourceProfile.UNKNOWN, PARALLELISM)),
                                    taskManagerGateway);
                        });
                // wait for all tasks to be deployed this is important because some tests trigger
                // savepoints these only properly work if the deployment has been started
                taskManagerGateway.waitForSubmissions(PARALLELISM);

                CompletableFuture<Iterable<ArchivedExecutionVertex>> vertexFuture =
                        new CompletableFuture<>();
                mainThreadExecutor.execute(
                        () ->
                                vertexFuture.complete(
                                        scheduler
                                                .requestJob()
                                                .getArchivedExecutionGraph()
                                                .getAllExecutionVertices()));
                final Iterable<ArchivedExecutionVertex> executionVertices = vertexFuture.get();
                final List<ExecutionAttemptID> attemptIds =
                        IterableUtils.toStream(executionVertices)
                                .map(ArchivedExecutionVertex::getCurrentExecutionAttempt)
                                .map(ArchivedExecution::getAttemptId)
                                .collect(Collectors.toList());
                CompletableFuture<Void> runTestLogicFuture =
                        CompletableFuture.runAsync(
                                () -> testLogic.accept(scheduler, attemptIds), mainThreadExecutor);
                runTestLogicFuture.get();

                mainThreadExecutor.execute(scheduler::cancel);
                scheduler.getJobTerminationFuture().get();

                return scheduler.requestJob().getExceptionHistory();
            } finally {
                AdaptiveSchedulerTest.closeInExecutorService(scheduler, mainThreadExecutor);
            }
        }
    }

    private static CompletableFuture<Map<String, String>> createFailureLabelsFuture() {
        return CompletableFuture.completedFuture(Collections.singletonMap("failKey", "failValue"));
    }

    private static UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup
            createTestMetricGroup(List<Span> output) {
        return new UnregisteredMetricGroups.UnregisteredJobManagerJobMetricGroup() {
            @Override
            public void addSpan(SpanBuilder spanBuilder) {
                output.add(spanBuilder.build());
            }
        };
    }

    private static void checkMetrics(List<Span> results, boolean canRestart) {
        assertThat(results).isNotEmpty();
        for (Span span : results) {
            assertThat(span.getScope())
                    .isEqualTo(JobFailureMetricReporter.class.getCanonicalName());
            assertThat(span.getName()).isEqualTo("JobFailure");
            Map<String, Object> attributes = span.getAttributes();
            assertThat(attributes)
                    .containsEntry("failureLabel.failKey", "failValue")
                    .containsEntry("canRestart", String.valueOf(canRestart));
        }
    }
}
