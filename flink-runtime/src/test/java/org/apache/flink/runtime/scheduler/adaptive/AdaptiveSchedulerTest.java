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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraphTest;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.operators.coordination.TestOperatorEvent;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlotAllocator;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.jobgraph.JobGraphTestUtils.streamingJobGraph;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.offerSlots;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for the {@link AdaptiveScheduler}. */
public class AdaptiveSchedulerTest extends TestLogger {

    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX = createNoOpVertex("v1", PARALLELISM);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> TEST_EXECUTOR_RESOURCE =
            new TestExecutorResource<>(Executors::newSingleThreadScheduledExecutor);

    private final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    private final ComponentMainThreadExecutor singleThreadMainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                    TEST_EXECUTOR_RESOURCE.getExecutor());

    @Test
    public void testInitialState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(scheduler.getState(), instanceOf(Created.class));
    }

    @Test
    public void testArchivedCheckpointingSettingsNotNullIfCheckpointingIsEnabled()
            throws Exception {
        final JobGraph jobGraph = createJobGraph();
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));

        final ArchivedExecutionGraph archivedExecutionGraph =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .build()
                        .getArchivedExecutionGraph(JobStatus.INITIALIZING, null);

        ArchivedExecutionGraphTest.assertContainsCheckpointSettings(archivedExecutionGraph);
    }

    @Test
    public void testIsState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        final State state = scheduler.getState();

        assertThat(scheduler.isState(state), is(true));
        assertThat(scheduler.isState(new DummyState()), is(false));
    }

    @Test
    public void testRunIfState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(scheduler.getState(), () -> ran.set(true));
        assertThat(ran.get(), is(true));
    }

    @Test
    public void testRunIfStateWithStateMismatch() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        AtomicBoolean ran = new AtomicBoolean(false);
        scheduler.runIfState(new DummyState(), () -> ran.set(true));
        assertThat(ran.get(), is(false));
    }

    @Test
    public void testHasEnoughResourcesReturnsFalseIfUnsatisfied() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        scheduler.startScheduling();

        final ResourceCounter resourceRequirement =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

        assertThat(scheduler.hasDesiredResources(resourceRequirement), is(false));
    }

    @Test
    public void testHasEnoughResourcesReturnsTrueIfSatisfied() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        final ResourceCounter resourceRequirement =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

        offerSlots(
                declarativeSlotPool, createSlotOffersForResourceRequirements(resourceRequirement));

        assertThat(scheduler.hasDesiredResources(resourceRequirement), is(true));
    }

    @Test
    public void testHasEnoughResourcesUsesUnmatchedSlotsAsUnknown() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        final int numRequiredSlots = 1;
        final ResourceCounter requiredResources =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, numRequiredSlots);
        final ResourceCounter providedResources =
                ResourceCounter.withResource(
                        ResourceProfile.newBuilder().setCpuCores(1).build(), numRequiredSlots);

        offerSlots(declarativeSlotPool, createSlotOffersForResourceRequirements(providedResources));

        assertThat(scheduler.hasDesiredResources(requiredResources), is(true));
    }

    @Test
    public void testExecutionGraphGenerationWithAvailableResources() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(1L));

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final int numAvailableSlots = 1;

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
        taskManagerGateway.waitForSubmissions(numAvailableSlots, Duration.ofSeconds(5));

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .join();

        assertThat(
                executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism(),
                is(numAvailableSlots));
    }

    @Test
    public void testExecutionGraphGenerationSetsInitializationTimestamp() throws Exception {
        final long initializationTimestamp = 42L;
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(1L));

        final AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setInitializationTimestamp(initializationTimestamp)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(PARALLELISM);

        singleThreadMainThreadExecutor.execute(
                () -> {
                    adaptiveScheduler.startScheduling();
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        // Wait for just the first submission to indicate the execution graph is ready
        taskManagerGateway.waitForSubmissions(1, Duration.ofSeconds(5));

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> adaptiveScheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .join();

        assertThat(
                executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
                is(initializationTimestamp));
    }

    @Test
    public void testInitializationTimestampForwarding() throws Exception {
        final long expectedInitializationTimestamp = 42L;

        final AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setInitializationTimestamp(expectedInitializationTimestamp)
                        .build();

        final long initializationTimestamp =
                adaptiveScheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getStatusTimestamp(JobStatus.INITIALIZING);

        assertThat(initializationTimestamp, is(expectedInitializationTimestamp));
    }

    @Test
    public void testFatalErrorsForwardedToFatalErrorHandler() throws Exception {
        final TestingFatalErrorHandler fatalErrorHandler = new TestingFatalErrorHandler();

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setFatalErrorHandler(fatalErrorHandler)
                        .build();

        final RuntimeException exception = new RuntimeException();

        scheduler.runIfState(
                scheduler.getState(),
                () -> {
                    throw exception;
                });

        assertThat(fatalErrorHandler.getException(), is(exception));
    }

    @Test
    public void testResourceTimeout() throws Exception {
        final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
                new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());
        final Duration resourceTimeout = Duration.ofMinutes(1234);
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, resourceTimeout);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setJobMasterConfiguration(configuration)
                        .build();

        scheduler.startScheduling();

        // check whether some task was scheduled with the expected timeout
        // this is technically not really safe, but the chosen timeout value
        // is odd enough that it realistically won't cause issues.
        // With this approach we don't have to make assumption as to how many
        // tasks are being scheduled.
        final boolean b =
                mainThreadExecutor.getNonPeriodicScheduledTask().stream()
                        .anyMatch(
                                scheduledTask ->
                                        scheduledTask.getDelay(TimeUnit.MINUTES)
                                                == resourceTimeout.toMinutes());
        assertThat(b, is(true));
    }

    @Test
    public void testNumRestartsMetric() throws Exception {
        final CompletableFuture<Gauge<Integer>> numRestartsMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    if (MetricNames.NUM_RESTARTS.equals(name)) {
                                        numRestartsMetricFuture.complete((Gauge<Integer>) metric);
                                    }
                                })
                        .build();

        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        Time.minutes(10),
                        Time.minutes(10));

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.MIN_PARALLELISM_INCREASE, 1);
        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(1L));

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(
                                new JobManagerJobMetricGroup(
                                        metricRegistry,
                                        createUnregisteredJobManagerMetricGroup(),
                                        new JobID(),
                                        "jobName"))
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        final Gauge<Integer> numRestartsMetric = numRestartsMetricFuture.get();

        final SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new SubmissionBufferingTaskManagerGateway(1 + PARALLELISM);

        taskManagerGateway.setCancelConsumer(createCancelConsumer(scheduler));

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();

                    declarativeSlotPool.offerSlots(
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                            new LocalTaskManagerLocation(),
                            taskManagerGateway,
                            System.currentTimeMillis());
                });

        // wait for the first task submission
        taskManagerGateway.waitForSubmissions(1, Duration.ofSeconds(5));

        assertThat(numRestartsMetric.getValue(), is(0));

        singleThreadMainThreadExecutor.execute(
                () -> {
                    // offer more slots, which will cause a restart in order to scale up
                    offerSlots(
                            declarativeSlotPool,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(
                                            ResourceProfile.UNKNOWN, PARALLELISM)),
                            taskManagerGateway);
                });

        // wait for the second task submissions
        taskManagerGateway.waitForSubmissions(PARALLELISM, Duration.ofSeconds(5));

        assertThat(numRestartsMetric.getValue(), is(1));
    }

    // ---------------------------------------------------------------------------------------------
    // State transition tests
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testStartSchedulingTransitionsToWaitingForResources() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        scheduler.startScheduling();

        assertThat(scheduler.getState(), instanceOf(WaitingForResources.class));
    }

    @Test
    public void testStartSchedulingSetsResourceRequirementsForDefaultMode() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        assertThat(
                declarativeSlotPool.getResourceRequirements(),
                contains(ResourceRequirement.create(ResourceProfile.UNKNOWN, PARALLELISM)));
    }

    @Test
    public void testStartSchedulingSetsResourceRequirementsForReactiveMode() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
                        .build();

        scheduler.startScheduling();

        // should request the max possible resources
        final int expectedParallelism =
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(PARALLELISM);
        assertThat(
                declarativeSlotPool.getResourceRequirements(),
                contains(ResourceRequirement.create(ResourceProfile.UNKNOWN, expectedParallelism)));
    }

    /** Tests that the listener for new slots is properly set up. */
    @Test
    public void testResourceAcquisitionTriggersJobExecution() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(1L));

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
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

        assertThat(startingStateFuture.get(), instanceOf(WaitingForResources.class));

        // Wait for all tasks to be submitted
        taskManagerGateway.waitForSubmissions(PARALLELISM, Duration.ofSeconds(5));

        final ArchivedExecutionGraph executionGraph =
                CompletableFuture.supplyAsync(
                                () -> scheduler.requestJob().getArchivedExecutionGraph(),
                                singleThreadMainThreadExecutor)
                        .get();

        assertThat(
                executionGraph.getJobVertex(JOB_VERTEX.getID()).getParallelism(), is(PARALLELISM));
    }

    @Test
    public void testGoToFinished() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();

        scheduler.goToFinished(archivedExecutionGraph);

        assertThat(scheduler.getState(), instanceOf(Finished.class));
    }

    @Test
    public void testGoToFinishedNotifiesJobListener() throws Exception {
        final AtomicReference<JobStatus> jobStatusUpdate = new AtomicReference<>();
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setJobStatusListener(
                                (jobId, newJobStatus, timestamp, error) ->
                                        jobStatusUpdate.set(newJobStatus))
                        .build();

        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();

        scheduler.goToFinished(archivedExecutionGraph);

        assertThat(jobStatusUpdate.get(), is(archivedExecutionGraph.getState()));
    }

    @Test
    public void testCloseShutsDownCheckpointingComponents() throws Exception {
        final CompletableFuture<JobStatus> completedCheckpointStoreShutdownFuture =
                new CompletableFuture<>();
        final CompletedCheckpointStore completedCheckpointStore =
                new TestingCompletedCheckpointStore(completedCheckpointStoreShutdownFuture);

        final CompletableFuture<JobStatus> checkpointIdCounterShutdownFuture =
                new CompletableFuture<>();
        final CheckpointIDCounter checkpointIdCounter =
                new TestingCheckpointIDCounter(checkpointIdCounterShutdownFuture);

        final JobGraph jobGraph = createJobGraph();
        // checkpointing components are only created if checkpointing is enabled
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        CheckpointCoordinatorConfiguration.builder().build(), null));

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(
                                        completedCheckpointStore, checkpointIdCounter))
                        .build();

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();
                    // transition into the FAILED state
                    scheduler.handleGlobalFailure(new FlinkException("Test exception"));
                    scheduler.closeAsync();
                });

        assertThat(completedCheckpointStoreShutdownFuture.get(), is(JobStatus.FAILED));
        assertThat(checkpointIdCounterShutdownFuture.get(), is(JobStatus.FAILED));
    }

    @Test
    public void testTransitionToStateCallsOnLeave() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        final LifecycleMethodCapturingState firstState = new LifecycleMethodCapturingState();

        scheduler.transitionToState(new StateInstanceFactory(firstState));

        firstState.reset();

        scheduler.transitionToState(new DummyState.Factory());
        assertThat(firstState.onLeaveCalled, is(true));
        assertThat(firstState.onLeaveNewStateArgument.equals(DummyState.class), is(true));
    }

    @Test
    public void testConsistentMaxParallelism() throws Exception {
        final int parallelism = 240;
        final int expectedMaxParallelism =
                KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        final JobVertex vertex = createNoOpVertex(parallelism);
        final JobGraph jobGraph = streamingJobGraph(vertex);

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(1L));

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .setJobMasterConfiguration(configuration)
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
        taskManagerGateway.waitForSubmissions(1, Duration.ofSeconds(5));

        ArchivedExecutionGraph executionGraph =
                getArchivedExecutionGraphForRunningJob(scheduler).get();
        ArchivedExecutionJobVertex archivedVertex = executionGraph.getJobVertex(vertex.getID());

        // ensure that the parallelism was submitted based on what is available
        assertThat(archivedVertex.getParallelism(), is(1));
        // and that the max parallelism was submitted based on what was configured
        assertThat(archivedVertex.getMaxParallelism(), is(expectedMaxParallelism));

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
        taskManagerGateway.waitForSubmissions(parallelism, Duration.ofSeconds(5));

        ArchivedExecutionGraph resubmittedExecutionGraph =
                getArchivedExecutionGraphForRunningJob(scheduler).get();
        ArchivedExecutionJobVertex resubmittedArchivedVertex =
                resubmittedExecutionGraph.getJobVertex(vertex.getID());

        // ensure that the parallelism was submitted based on what is available
        assertThat(resubmittedArchivedVertex.getParallelism(), is(parallelism));
        // and that the max parallelism was submitted based on what was configured
        assertThat(resubmittedArchivedVertex.getMaxParallelism(), is(expectedMaxParallelism));
    }

    // ---------------------------------------------------------------------------------------------
    // Failure handling tests
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testHowToHandleFailureRejectedByStrategy() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setRestartBackoffTimeStrategy(NoRestartBackoffTimeStrategy.INSTANCE)
                        .build();

        assertThat(scheduler.howToHandleFailure(new Exception("test")).canRestart(), is(false));
    }

    @Test
    public void testHowToHandleFailureAllowedByStrategy() throws Exception {
        final TestRestartBackoffTimeStrategy restartBackoffTimeStrategy =
                new TestRestartBackoffTimeStrategy(true, 1234);

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setRestartBackoffTimeStrategy(restartBackoffTimeStrategy)
                        .build();

        final Executing.FailureResult failureResult =
                scheduler.howToHandleFailure(new Exception("test"));

        assertThat(failureResult.canRestart(), is(true));
        assertThat(
                failureResult.getBackoffTime().toMillis(),
                is(restartBackoffTimeStrategy.getBackoffTime()));
    }

    @Test
    public void testHowToHandleFailureUnrecoverableFailure() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(
                scheduler
                        .howToHandleFailure(new SuppressRestartsException(new Exception("test")))
                        .canRestart(),
                is(false));
    }

    @Test(expected = IllegalStateException.class)
    public void testRepeatedTransitionIntoCurrentStateFails() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        final State state = scheduler.getState();

        // safeguard for this test
        assertThat(state, instanceOf(Created.class));

        scheduler.transitionToState(new Created.Factory(scheduler, log));
    }

    // ---------------------------------------------------------------------------------------------
    // Illegal state behavior tests
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testTriggerSavepointFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(
                scheduler.triggerSavepoint("some directory", false),
                futureFailedWith(CheckpointException.class));
    }

    @Test
    public void testStopWithSavepointFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(
                scheduler.stopWithSavepoint("some directory", false),
                futureFailedWith(CheckpointException.class));
    }

    @Test(expected = TaskNotRunningException.class)
    public void testDeliverOperatorEventToCoordinatorFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        scheduler.deliverOperatorEventToCoordinator(
                new ExecutionAttemptID(), new OperatorID(), new TestOperatorEvent());
    }

    @Test
    public void testDeliverCoordinationRequestToCoordinatorFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(
                scheduler.deliverCoordinationRequestToCoordinator(
                        new OperatorID(), new CoordinationRequest() {}),
                futureFailedWith(FlinkException.class));
    }

    @Test
    public void testUpdateTaskExecutionStateReturnsFalseInIllegalState() throws Exception {
        final JobGraph jobGraph = createJobGraph();
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor).build();

        assertThat(
                scheduler.updateTaskExecutionState(
                        new TaskExecutionStateTransition(
                                new TaskExecutionState(
                                        new ExecutionAttemptID(), ExecutionState.FAILED))),
                is(false));
    }

    @Test(expected = IOException.class)
    public void testRequestNextInputSplitFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        scheduler.requestNextInputSplit(JOB_VERTEX.getID(), new ExecutionAttemptID());
    }

    @Test(expected = PartitionProducerDisposedException.class)
    public void testRequestPartitionStateFailsInIllegalState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        scheduler.requestPartitionState(new IntermediateDataSetID(), new ResultPartitionID());
    }

    @Test
    public void testTryToAssignSlotsReturnsNotPossibleIfExpectedResourcesAreNotAvailable()
            throws Exception {

        final TestingSlotAllocator slotAllocator =
                TestingSlotAllocator.newBuilder()
                        .setTryReserveResourcesFunction(ignored -> Optional.empty())
                        .build();

        final AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor)
                        .setSlotAllocator(slotAllocator)
                        .build();

        final CreatingExecutionGraph.AssignmentResult assignmentResult =
                adaptiveScheduler.tryToAssignSlots(
                        CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create(
                                new StateTrackingMockExecutionGraph(),
                                new CreatingExecutionGraphTest.TestingVertexParallelism()));

        assertFalse(assignmentResult.isSuccess());
    }

    @Test
    public void testComputeVertexParallelismStoreForExecutionInReactiveMode() {
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

            assertThat(info.getParallelism(), is(vertex.getParallelism()));
            assertThat(info.getMaxParallelism(), is(vertex.getMaxParallelism()));
        }
    }

    @Test
    public void testComputeVertexParallelismStoreForExecutionInDefaultMode() {
        JobVertex v1 = createNoOpVertex("v1", 1, 50);
        JobVertex v2 = createNoOpVertex("v2", 50, 50);
        JobGraph graph = streamingJobGraph(v1, v2);

        VertexParallelismStore parallelismStore =
                AdaptiveScheduler.computeVertexParallelismStoreForExecution(
                        graph, null, SchedulerBase::getDefaultMaxParallelism);

        for (JobVertex vertex : graph.getVertices()) {
            VertexParallelismInformation info = parallelismStore.getParallelismInfo(vertex.getID());

            assertThat(info.getParallelism(), is(vertex.getParallelism()));
            assertThat(info.getMaxParallelism(), is(vertex.getMaxParallelism()));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Utils
    // ---------------------------------------------------------------------------------------------

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

    @Nonnull
    private static DefaultDeclarativeSlotPool createDeclarativeSlotPool(JobID jobId) {
        return new DefaultDeclarativeSlotPool(
                jobId,
                new DefaultAllocatedSlotPool(),
                ignored -> {},
                Time.minutes(10),
                Time.minutes(10));
    }

    private static JobGraph createJobGraph() {
        return streamingJobGraph(JOB_VERTEX);
    }

    private static class LifecycleMethodCapturingState extends DummyState {
        boolean onLeaveCalled = false;
        @Nullable Class<? extends State> onLeaveNewStateArgument = null;

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
    private static class SubmissionBufferingTaskManagerGateway
            extends SimpleAckingTaskManagerGateway {
        final BlockingQueue<TaskDeploymentDescriptor> submittedTasks;

        public SubmissionBufferingTaskManagerGateway(int capacity) {
            submittedTasks = new ArrayBlockingQueue<>(capacity);
            super.setSubmitConsumer(submittedTasks::offer);
        }

        @Override
        public void setSubmitConsumer(Consumer<TaskDeploymentDescriptor> submitConsumer) {
            super.setSubmitConsumer(
                    ((Consumer<TaskDeploymentDescriptor>) submittedTasks::offer)
                            .andThen(submitConsumer));
        }

        /**
         * Block until an arbitrary number of submissions have been received.
         *
         * @param numSubmissions The number of submissions to wait for
         * @param perTaskTimeout The max amount of time to wait between each submission
         * @return the list of the waited-for submissions
         * @throws InterruptedException if a timeout is exceeded waiting for a submission
         */
        public List<TaskDeploymentDescriptor> waitForSubmissions(
                int numSubmissions, Duration perTaskTimeout) throws InterruptedException {
            List<TaskDeploymentDescriptor> descriptors = new ArrayList<>();
            for (int i = 0; i < numSubmissions; i++) {
                descriptors.add(
                        submittedTasks.poll(perTaskTimeout.toMillis(), TimeUnit.MILLISECONDS));
            }
            return descriptors;
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

    static class DummyState implements State {

        @Override
        public void cancel() {}

        @Override
        public void suspend(Throwable cause) {}

        @Override
        public JobStatus getJobStatus() {
            return null;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return null;
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {}

        @Override
        public Logger getLogger() {
            return null;
        }

        private static class Factory implements StateFactory<DummyState> {

            @Override
            public Class<DummyState> getStateClass() {
                return DummyState.class;
            }

            @Override
            public DummyState getState() {
                return new DummyState();
            }
        }
    }
}
