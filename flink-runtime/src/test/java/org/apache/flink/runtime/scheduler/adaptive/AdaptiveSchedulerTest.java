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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.TestUtils;
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
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory.useSameServicesForAllJobs;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.offerSlots;
import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link AdaptiveScheduler}. */
public class AdaptiveSchedulerTest extends TestLogger {

    private static final int PARALLELISM = 4;
    private static final JobVertex JOB_VERTEX;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    static {
        JOB_VERTEX = new JobVertex("v1");
        JOB_VERTEX.setParallelism(PARALLELISM);
        JOB_VERTEX.setInvokableClass(AbstractInvokable.class);
    }

    private final ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
            new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());

    @Test
    public void testInitialState() throws Exception {
        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(createJobGraph(), mainThreadExecutor).build();

        assertThat(scheduler.getState(), instanceOf(Created.class));
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

        assertThat(scheduler.hasEnoughResources(resourceRequirement), is(false));
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

        assertThat(scheduler.hasEnoughResources(resourceRequirement), is(true));
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

        assertThat(scheduler.hasEnoughResources(requiredResources), is(true));
    }

    @Test
    public void testExecutionGraphGenerationWithAvailableResources() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        final int numAvailableSlots = 1;

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, numAvailableSlots)));

        final ExecutionGraph executionGraph =
                scheduler.createExecutionGraphWithAvailableResources();

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

        final AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setInitializationTimestamp(initializationTimestamp)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        adaptiveScheduler.startScheduling();

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)));

        final ExecutionGraph executionGraph =
                adaptiveScheduler.createExecutionGraphWithAvailableResources();

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

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
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

        scheduler.startScheduling();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        taskManagerGateway.setCancelConsumer(
                executionAttemptId ->
                        mainThreadExecutor.execute(
                                () ->
                                        scheduler.updateTaskExecutionState(
                                                new TaskExecutionState(
                                                        executionAttemptId,
                                                        ExecutionState.CANCELED))));

        declarativeSlotPool.offerSlots(
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)),
                new LocalTaskManagerLocation(),
                taskManagerGateway,
                System.currentTimeMillis());

        // trigger resource timeout and the deployment of the job
        mainThreadExecutor.triggerAllNonPeriodicTasks();

        assertThat(numRestartsMetric.getValue(), is(0));

        // offer more slots, which will cause a restart in order to scale up
        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, PARALLELISM)));

        // trigger cancellation of tasks and the restart
        mainThreadExecutor.triggerAllNonPeriodicTasks();

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
    public void testStartSchedulingSetsResourceRequirements() throws Exception {
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

    /** Tests that the listener for new slots is properly set up. */
    @Test
    public void testResourceAcquisitionTriggersJobExecution() throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraph.getJobID());

        final AdaptiveScheduler scheduler =
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        scheduler.startScheduling();

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, PARALLELISM)));

        assertThat(scheduler.getState(), instanceOf(Executing.class));
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
    public void testGoToFinishedShutsDownCheckpointingComponents() throws Exception {
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
                new AdaptiveSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(
                                        completedCheckpointStore, checkpointIdCounter))
                        .build();

        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(JobStatus.FAILED).build();

        scheduler.goToFinished(archivedExecutionGraph);

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
    public void testRestoringModifiedJobFromSavepointFails() throws Exception {
        // create savepoint data
        final long savepointId = 42L;
        final OperatorID operatorID = new OperatorID();
        final File savepointFile =
                TestUtils.createSavepointWithOperatorState(
                        TEMPORARY_FOLDER.newFile(), savepointId, operatorID);

        // set savepoint settings which don't allow non restored state
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(savepointFile.getAbsolutePath(), false);

        // create a new operator
        final JobVertex jobVertex = new JobVertex("New operator");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(1);

        // this test will fail in the end due to the previously created Savepoint having a state for
        // a given OperatorID that does not match any operator of the newly created JobGraph
        final JobGraph jobGraphWithNewOperator =
                TestUtils.createJobGraphFromJobVerticesWithCheckpointing(
                        savepointRestoreSettings, jobVertex);

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraphWithNewOperator.getJobID());

        final AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(jobGraphWithNewOperator, mainThreadExecutor)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        adaptiveScheduler.startScheduling();

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)));

        final ArchivedExecutionGraph archivedExecutionGraph =
                adaptiveScheduler.requestJob().getArchivedExecutionGraph();

        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
        assertThat(
                archivedExecutionGraph.getFailureInfo().getException(),
                FlinkMatchers.containsMessage("Failed to rollback to checkpoint/savepoint"));
    }

    @Test
    public void testRestoringModifiedJobFromSavepointWithAllowNonRestoredStateSucceeds()
            throws Exception {
        // create savepoint data
        final long savepointId = 42L;
        final OperatorID operatorID = new OperatorID();
        final File savepointFile =
                TestUtils.createSavepointWithOperatorState(
                        TEMPORARY_FOLDER.newFile(), savepointId, operatorID);

        // allow for non restored state
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath(savepointFile.getAbsolutePath(), true);

        // create a new operator
        final JobVertex jobVertex = new JobVertex("New operator");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(1);

        final JobGraph jobGraphWithNewOperator =
                TestUtils.createJobGraphFromJobVerticesWithCheckpointing(
                        savepointRestoreSettings, jobVertex);

        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final CheckpointRecoveryFactory testingCheckpointRecoveryFactory =
                useSameServicesForAllJobs(
                        completedCheckpointStore, new StandaloneCheckpointIDCounter());

        final DefaultDeclarativeSlotPool declarativeSlotPool =
                createDeclarativeSlotPool(jobGraphWithNewOperator.getJobID());

        AdaptiveScheduler adaptiveScheduler =
                new AdaptiveSchedulerBuilder(jobGraphWithNewOperator, mainThreadExecutor)
                        .setCheckpointRecoveryFactory(testingCheckpointRecoveryFactory)
                        .setDeclarativeSlotPool(declarativeSlotPool)
                        .build();

        adaptiveScheduler.startScheduling();

        offerSlots(
                declarativeSlotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)));

        // starting and offering the required slots should trigger the ExecutionGraph creation
        final CompletedCheckpoint savepoint = completedCheckpointStore.getLatestCheckpoint(false);

        MatcherAssert.assertThat(savepoint, notNullValue());

        MatcherAssert.assertThat(savepoint.getCheckpointID(), Matchers.is(savepointId));
    }

    // ---------------------------------------------------------------------------------------------
    // Utils
    // ---------------------------------------------------------------------------------------------

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
        return JobGraphTestUtils.streamingJobGraph(JOB_VERTEX);
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

        private static class Factory implements StateFactory<LifecycleMethodCapturingState> {

            @Override
            public Class<LifecycleMethodCapturingState> getStateClass() {
                return LifecycleMethodCapturingState.class;
            }

            @Override
            public LifecycleMethodCapturingState getState() {
                return new LifecycleMethodCapturingState();
            }
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

    private static class DummyState implements State {

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
