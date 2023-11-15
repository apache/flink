/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TestingCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.TestingCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.hooks.TestMasterHook;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingJobStatusHook;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartAllFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.TestFailoverStrategyFactory;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerTest;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntryTestingUtils;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestSchedulingStrategy;
import org.apache.flink.runtime.shuffle.TestingShuffleMaster;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.acknowledgePendingCheckpoint;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createFailedTaskExecutionState;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.enableCheckpointing;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.getCheckpointCoordinator;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultScheduler}. */
public class DefaultSchedulerTest extends TestLogger {

    private static final int TIMEOUT_MS = 1000;

    private final ManuallyTriggeredScheduledExecutor taskRestartExecutor =
            new ManuallyTriggeredScheduledExecutor();

    private ExecutorService executor;

    private ScheduledExecutorService scheduledExecutorService;

    private Configuration configuration;

    private TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy;

    private TestExecutionOperationsDecorator testExecutionOperations;

    private ExecutionVertexVersioner executionVertexVersioner;

    private TestExecutionSlotAllocatorFactory executionSlotAllocatorFactory;

    private TestExecutionSlotAllocator testExecutionSlotAllocator;

    private TestingShuffleMaster shuffleMaster;

    private TestingJobMasterPartitionTracker partitionTracker;

    private Time timeout;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        configuration = new Configuration();

        testRestartBackoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, 0);

        testExecutionOperations =
                new TestExecutionOperationsDecorator(new DefaultExecutionOperations());

        executionVertexVersioner = new ExecutionVertexVersioner();

        executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory();
        testExecutionSlotAllocator = executionSlotAllocatorFactory.getTestExecutionSlotAllocator();

        shuffleMaster = new TestingShuffleMaster();
        partitionTracker = new TestingJobMasterPartitionTracker();

        timeout = Time.seconds(60);
    }

    @AfterEach
    void tearDown() {
        if (scheduledExecutorService != null) {
            ExecutorUtils.gracefulShutdown(
                    TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduledExecutorService);
        }

        if (executor != null) {
            ExecutorUtils.gracefulShutdown(TIMEOUT_MS, TimeUnit.MILLISECONDS, executor);
        }
    }

    @Test
    void startScheduling() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        createSchedulerAndStartScheduling(jobGraph);

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices).contains(executionVertexId);
    }

    @Test
    void testCorrectSettingOfInitializationTimestamp() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionGraphInfo executionGraphInfo = scheduler.requestJob();
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();

        // ensure all statuses are set in the ExecutionGraph
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING))
                .isGreaterThan(0L);
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED)).isGreaterThan(0L);
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.RUNNING)).isGreaterThan(0L);

        // ensure correct order
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING))
                .isLessThanOrEqualTo(archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED));
    }

    @Test
    void deployTasksOnlyWhenAllSlotRequestsAreFulfilled() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(4);
        final JobVertexID onlyJobVertexId = getOnlyJobVertex(jobGraph).getID();

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        scheduler.startScheduling();

        final List<ExecutionVertexID> verticesToSchedule =
                Arrays.asList(
                        new ExecutionVertexID(onlyJobVertexId, 0),
                        new ExecutionVertexID(onlyJobVertexId, 1),
                        new ExecutionVertexID(onlyJobVertexId, 2),
                        new ExecutionVertexID(onlyJobVertexId, 3));
        schedulingStrategy.schedule(verticesToSchedule);

        assertThat(testExecutionOperations.getDeployedVertices()).isEmpty();

        testExecutionSlotAllocator.completePendingRequest(verticesToSchedule.get(0));
        assertThat(testExecutionOperations.getDeployedVertices()).isEmpty();

        testExecutionSlotAllocator.completePendingRequests();
        assertThat(testExecutionOperations.getDeployedVertices()).hasSize(4);
    }

    @Test
    void scheduledVertexOrderFromSchedulingStrategyIsRespected() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(10);
        final JobVertexID onlyJobVertexId = getOnlyJobVertex(jobGraph).getID();

        final List<ExecutionVertexID> desiredScheduleOrder =
                Arrays.asList(
                        new ExecutionVertexID(onlyJobVertexId, 4),
                        new ExecutionVertexID(onlyJobVertexId, 0),
                        new ExecutionVertexID(onlyJobVertexId, 3),
                        new ExecutionVertexID(onlyJobVertexId, 1),
                        new ExecutionVertexID(onlyJobVertexId, 2));

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        createScheduler(
                jobGraph,
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();

        schedulingStrategy.schedule(desiredScheduleOrder);

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();

        assertThat(desiredScheduleOrder).isEqualTo(deployedExecutionVertices);
    }

    @Test
    void restartAfterDeploymentFails() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        testExecutionOperations.enableFailDeploy();

        createSchedulerAndStartScheduling(jobGraph);

        testExecutionOperations.disableFailDeploy();
        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices).contains(executionVertexId, executionVertexId);
    }

    @Test
    void restartFailedTask() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex archivedExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                archivedExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));

        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices).contains(executionVertexId, executionVertexId);
    }

    @Test
    void updateTaskExecutionStateReturnsFalseIfExecutionDoesNotExist() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final TaskExecutionState taskExecutionState =
                createFailedTaskExecutionState(createExecutionAttemptId());

        assertThat(scheduler.updateTaskExecutionState(taskExecutionState)).isFalse();
    }

    @Test
    void failJobIfCannotRestart() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        testRestartBackoffTimeStrategy.setCanRestart(false);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));

        taskRestartExecutor.triggerScheduledTasks();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void failJobIfNotEnoughResources() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        testRestartBackoffTimeStrategy.setCanRestart(false);
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        testExecutionSlotAllocator.timeoutPendingRequests();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus).isEqualTo(JobStatus.FAILED);

        Throwable failureCause =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getFailureInfo()
                        .getException()
                        .deserializeError(DefaultSchedulerTest.class.getClassLoader());
        assertThat(findThrowable(failureCause, NoResourceAvailableException.class)).isPresent();
        assertThat(
                        findThrowableWithMessage(
                                failureCause,
                                "Could not allocate the required slot within slot request timeout."))
                .isPresent();
        assertThat(jobStatus).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void restartVerticesOnSlotAllocationTimeout() throws Exception {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testRestartVerticesOnFailuresInScheduling(
                vid -> testExecutionSlotAllocator.timeoutPendingRequest(vid));
    }

    @Test
    void restartVerticesOnAssignedSlotReleased() throws Exception {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testRestartVerticesOnFailuresInScheduling(
                vid -> {
                    final LogicalSlot slot = testExecutionSlotAllocator.completePendingRequest(vid);
                    slot.releaseSlot(new Exception("Release slot for test"));
                });
    }

    private void testRestartVerticesOnFailuresInScheduling(
            Consumer<ExecutionVertexID> actionsToTriggerTaskFailure) throws Exception {
        final int parallelism = 2;
        final JobVertex v1 = createVertex("vertex1", parallelism);
        final JobVertex v2 = createVertex("vertex2", parallelism);
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(v1, v2);

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory,
                        new RestartPipelinedRegionFailoverStrategy.Factory());
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        scheduler.startScheduling();

        final ExecutionVertexID vid11 = new ExecutionVertexID(v1.getID(), 0);
        final ExecutionVertexID vid12 = new ExecutionVertexID(v1.getID(), 1);
        final ExecutionVertexID vid21 = new ExecutionVertexID(v2.getID(), 0);
        final ExecutionVertexID vid22 = new ExecutionVertexID(v2.getID(), 1);
        schedulingStrategy.schedule(Arrays.asList(vid11, vid12, vid21, vid22));

        assertThat(testExecutionSlotAllocator.getPendingRequests()).hasSize(4);

        actionsToTriggerTaskFailure.accept(vid11);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        final ArchivedExecutionVertex ev11 = vertexIterator.next();
        final ArchivedExecutionVertex ev12 = vertexIterator.next();
        final ArchivedExecutionVertex ev21 = vertexIterator.next();
        final ArchivedExecutionVertex ev22 = vertexIterator.next();

        // ev11 and ev21 needs to be restarted because it is pipelined region failover and
        // they are in the same region. ev12 and ev22 will not be affected
        assertThat(testExecutionSlotAllocator.getPendingRequests()).hasSize(2);
        assertThat(ev11.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(ev21.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
        assertThat(ev12.getExecutionState()).isEqualTo(ExecutionState.SCHEDULED);
        assertThat(ev22.getExecutionState()).isEqualTo(ExecutionState.SCHEDULED);

        taskRestartExecutor.triggerScheduledTasks();
        assertThat(schedulingStrategy.getReceivedVerticesToRestart()).contains(vid11, vid21);
    }

    @Test
    void skipDeploymentIfVertexVersionOutdated() {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
        final List<JobVertex> sortedJobVertices =
                jobGraph.getVerticesSortedTopologicallyFromSources();
        final ExecutionVertexID sourceExecutionVertexId =
                new ExecutionVertexID(sortedJobVertices.get(0).getID(), 0);
        final ExecutionVertexID sinkExecutionVertexId =
                new ExecutionVertexID(sortedJobVertices.get(1).getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        testExecutionSlotAllocator.completePendingRequest(sourceExecutionVertexId);

        final ArchivedExecutionVertex sourceExecutionVertex =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator()
                        .next();
        final ExecutionAttemptID attemptId =
                sourceExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));
        testRestartBackoffTimeStrategy.setCanRestart(false);

        testExecutionSlotAllocator.enableAutoCompletePendingRequests();
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(testExecutionOperations.getDeployedVertices())
                .contains(sourceExecutionVertexId, sinkExecutionVertexId);
        assertThat(scheduler.requestJob().getArchivedExecutionGraph().getState())
                .isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void releaseSlotIfVertexVersionOutdated() {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(getOnlyJobVertex(jobGraph).getID(), 0);

        createSchedulerAndStartScheduling(jobGraph);

        executionVertexVersioner.recordModification(onlyExecutionVertexId);
        testExecutionSlotAllocator.completePendingRequests();

        assertThat(testExecutionSlotAllocator.getReturnedSlots()).hasSize(1);
    }

    @Test
    void vertexIsResetBeforeRestarted() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

        scheduler.startScheduling();

        final SchedulingExecutionVertex onlySchedulingVertex =
                Iterables.getOnlyElement(topology.getVertices());
        schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertex.getId()));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));

        taskRestartExecutor.triggerScheduledTasks();

        assertThat(schedulingStrategy.getReceivedVerticesToRestart()).hasSize(1);
        assertThat(onlySchedulingVertex.getState()).isEqualTo(ExecutionState.CREATED);
    }

    @Test
    void scheduleOnlyIfVertexIsCreated() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

        scheduler.startScheduling();

        final ExecutionVertexID onlySchedulingVertexId =
                Iterables.getOnlyElement(topology.getVertices()).getId();

        // Schedule the vertex to get it to a non-CREATED state
        schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertexId));

        // The scheduling of a non-CREATED vertex will result in IllegalStateException
        assertThatThrownBy(
                        () ->
                                schedulingStrategy.schedule(
                                        Collections.singletonList(onlySchedulingVertexId)),
                        "IllegalStateException should happen")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void handleGlobalFailure() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        scheduler.handleGlobalFailure(new Exception("forced failure"));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attemptId, ExecutionState.CANCELED));

        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices).contains(executionVertexId, executionVertexId);
    }

    @Test
    void testRestoreVertexEndOfDataListener() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph, null, null, Long.MAX_VALUE - 1, true);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        scheduler.notifyEndOfData(attemptId);
        assertThat(scheduler.getVertexEndOfDataListener().areAllTasksEndOfData()).isTrue();

        scheduler.restoreState(Collections.singleton(attemptId.getExecutionVertexId()), true);
        assertThat(scheduler.getVertexEndOfDataListener().areAllTasksEndOfData()).isFalse();
    }

    /**
     * This test covers the use-case where a global fail-over is followed by a local task failure.
     * It verifies (besides checking the expected deployments) that the assert in the global
     * recovery handling of {@link SchedulerBase#restoreState} is not triggered due to version
     * updates.
     */
    @Test
    void handleGlobalFailureWithLocalFailure() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        enableCheckpointing(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final List<ExecutionAttemptID> attemptIds =
                StreamSupport.stream(
                                scheduler
                                        .requestJob()
                                        .getArchivedExecutionGraph()
                                        .getAllExecutionVertices()
                                        .spliterator(),
                                false)
                        .map(ArchivedExecutionVertex::getCurrentExecutionAttempt)
                        .map(ArchivedExecution::getAttemptId)
                        .collect(Collectors.toList());
        final ExecutionAttemptID localFailureAttemptId = attemptIds.get(0);
        scheduler.handleGlobalFailure(new Exception("global failure"));
        // the local failure shouldn't affect the global fail-over
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        localFailureAttemptId,
                        ExecutionState.FAILED,
                        new Exception("local failure")));

        for (ExecutionAttemptID attemptId : attemptIds) {
            scheduler.updateTaskExecutionState(
                    new TaskExecutionState(attemptId, ExecutionState.CANCELED));
        }

        taskRestartExecutor.triggerScheduledTasks();

        final ExecutionVertexID executionVertexId0 =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);
        final ExecutionVertexID executionVertexId1 =
                new ExecutionVertexID(onlyJobVertex.getID(), 1);
        assertThat(testExecutionOperations.getDeployedVertices())
                .withFailMessage(
                        "The "
                                + "execution vertices should be deployed in a specific order reflecting the "
                                + "scheduling start and the global fail-over afterwards.")
                .contains(
                        executionVertexId0,
                        executionVertexId1,
                        executionVertexId0,
                        executionVertexId1);
    }

    @Test
    void testStartingCheckpointSchedulerAfterExecutionGraphFinished() {
        assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
                SchedulerBase::startCheckpointScheduler);
    }

    @Test
    void testStoppingCheckpointSchedulerAfterExecutionGraphFinished() {
        assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
                SchedulerBase::stopCheckpointScheduler);
    }

    private void assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
            Consumer<DefaultScheduler> callSchedulingOperation) {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        assertThat(scheduler.getCheckpointCoordinator()).isNotNull();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        Iterables.getOnlyElement(
                                        scheduler.getExecutionGraph().getAllExecutionVertices())
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        ExecutionState.FINISHED));

        assertThat(scheduler.getCheckpointCoordinator()).isNull();
        callSchedulingOperation.accept(scheduler);
        assertThat(scheduler.getCheckpointCoordinator()).isNull();
    }

    @Test
    void vertexIsNotAffectedByOutdatedDeployment() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        final ArchivedExecutionVertex v1 = vertexIterator.next();
        final ArchivedExecutionVertex v2 = vertexIterator.next();

        final SchedulingExecutionVertex sv1 =
                scheduler.getSchedulingTopology().getVertices().iterator().next();

        // fail v1 and let it recover to SCHEDULED
        // the initial deployment of v1 will be outdated
        scheduler.updateTaskExecutionState(
                createFailedTaskExecutionState(v1.getCurrentExecutionAttempt().getAttemptId()));
        taskRestartExecutor.triggerScheduledTasks();

        // fail v2 to get all pending slot requests in the initial deployments to be done
        // this triggers the outdated deployment of v1
        scheduler.updateTaskExecutionState(
                createFailedTaskExecutionState(v2.getCurrentExecutionAttempt().getAttemptId()));

        // v1 should not be affected
        assertThat(sv1.getState()).isEqualTo(ExecutionState.SCHEDULED);
    }

    @Test
    void abortPendingCheckpointsWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler =
                createSchedulerAndStartScheduling(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                                new DirectScheduledExecutorService()));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        transitionToRunning(scheduler, attemptId);

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
    }

    @Test
    void restoreStateWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler =
                createSchedulerAndStartScheduling(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                                new DirectScheduledExecutorService()));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        transitionToRunning(scheduler, attemptId);

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        // register a stateful master hook to help verify state restore
        final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
        checkpointCoordinator.addMasterHook(masterHook);

        // complete one checkpoint for state restore
        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        final long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
        acknowledgePendingCheckpoint(scheduler, checkpointId);

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(masterHook.getRestoreCount()).isOne();
    }

    @Test
    void testTriggerCheckpointAndCompletedAfterStore() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        CompletedCheckpointStore store =
                TestingCompletedCheckpointStore.builder()
                        .withGetAllCheckpointsSupplier(Collections::emptyList)
                        .withAddCheckpointAndSubsumeOldestOneFunction(
                                (ignoredCompletedCheckpoint,
                                        ignoredCheckpointsCleaner,
                                        ignoredPostCleanup) -> {
                                    throw new RuntimeException(
                                            "Throw exception when add checkpoint to store.");
                                })
                        .withGetSharedStateRegistrySupplier(SharedStateRegistryImpl::new)
                        .build();

        ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        new DirectScheduledExecutorService());
        final DefaultScheduler scheduler;
        scheduler =
                createSchedulerBuilder(jobGraph, mainThreadExecutor)
                        .setCheckpointRecoveryFactory(
                                new TestingCheckpointRecoveryFactory(
                                        store, new StandaloneCheckpointIDCounter()))
                        .build();
        mainThreadExecutor.execute(scheduler::startScheduling);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        transitionToRunning(scheduler, attemptId);

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        // complete one checkpoint for state restore
        CompletableFuture<CompletedCheckpoint> checkpointCompletableFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();

        final long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
        OneShotLatch latch = new OneShotLatch();
        mainThreadExecutor.execute(
                () -> {
                    try {
                        final AcknowledgeCheckpoint acknowledgeCheckpoint =
                                new AcknowledgeCheckpoint(
                                        jobGraph.getJobID(), attemptId, checkpointId);
                        checkpointCoordinator.receiveAcknowledgeMessage(
                                acknowledgeCheckpoint, "Unknown location");
                    } catch (Exception e) {
                        latch.trigger();
                    }
                });

        latch.await();
        assertThat(checkpointCompletableFuture).isCompletedExceptionally();
    }

    @Test
    void failGlobalWhenRestoringStateFails() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler =
                createSchedulerAndStartScheduling(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                                new DirectScheduledExecutorService()));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        transitionToRunning(scheduler, attemptId);

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        // register a master hook to fail state restore
        final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
        masterHook.enableFailOnRestore();
        checkpointCoordinator.addMasterHook(masterHook);

        // complete one checkpoint for state restore
        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        final long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
        acknowledgePendingCheckpoint(scheduler, checkpointId);

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));
        taskRestartExecutor.triggerScheduledTasks();

        // the first task failover should be skipped on state restore failure
        List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices).contains(executionVertexId);

        // a global failure should be triggered on state restore failure
        masterHook.disableFailOnRestore();
        taskRestartExecutor.triggerScheduledTasks();
        deployedExecutionVertices = testExecutionOperations.getDeployedVertices();
        assertThat(deployedExecutionVertices).contains(executionVertexId, executionVertexId);
    }

    @Test
    void failJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.failJob(
                new FlinkException("Test failure."),
                System.currentTimeMillis(),
                FailureEnricherUtils.EMPTY_FAILURE_LABELS);

        assertThat(executionVertexVersioner.isModified(executionVertexVersion)).isTrue();
    }

    @Test
    void cancelJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.cancel();

        assertThat(executionVertexVersioner.isModified(executionVertexVersion)).isTrue();
    }

    @Test
    void suspendJobWillIncrementVertexVersions() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.close();

        assertThat(executionVertexVersioner.isModified(executionVertexVersion)).isTrue();
    }

    @Test
    void jobStatusIsRestartingIfOneVertexIsWaitingForRestart() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        final ExecutionAttemptID attemptId1 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionAttemptID attemptId2 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptId1, ExecutionState.FAILED, new RuntimeException("expected")));
        final JobStatus jobStatusAfterFirstFailure = scheduler.requestJobStatus();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptId2, ExecutionState.FAILED, new RuntimeException("expected")));

        taskRestartExecutor.triggerNonPeriodicScheduledTask();
        final JobStatus jobStatusWithPendingRestarts = scheduler.requestJobStatus();
        taskRestartExecutor.triggerNonPeriodicScheduledTask();
        final JobStatus jobStatusAfterRestarts = scheduler.requestJobStatus();

        assertThat(jobStatusAfterFirstFailure).isEqualTo(JobStatus.RESTARTING);
        assertThat(jobStatusWithPendingRestarts).isEqualTo(JobStatus.RESTARTING);
        assertThat(jobStatusAfterRestarts).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void cancelWhileRestartingShouldWaitForRunningTasks() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final SchedulingTopology topology = scheduler.getSchedulingTopology();

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        final ExecutionAttemptID attemptId1 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionAttemptID attemptId2 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionVertexID executionVertex2 = attemptId2.getExecutionVertexId();

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptId1, ExecutionState.FAILED, new RuntimeException("expected")));
        scheduler.cancel();
        final ExecutionState vertex2StateAfterCancel =
                topology.getVertex(executionVertex2).getState();
        final JobStatus statusAfterCancelWhileRestarting = scheduler.requestJobStatus();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptId2, ExecutionState.CANCELED, new RuntimeException("expected")));

        assertThat(vertex2StateAfterCancel).isEqualTo(ExecutionState.CANCELING);
        assertThat(statusAfterCancelWhileRestarting).isEqualTo(JobStatus.CANCELLING);
        assertThat(scheduler.requestJobStatus()).isEqualTo(JobStatus.CANCELED);
    }

    @Test
    void failureInfoIsSetAfterTaskFailure() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptId, ExecutionState.FAILED, new RuntimeException(exceptionMessage)));

        final ErrorInfo failureInfo =
                scheduler.requestJob().getArchivedExecutionGraph().getFailureInfo();
        assertThat(failureInfo).isNotNull();
        assertThat(failureInfo.getExceptionAsString()).contains(exceptionMessage);
    }

    @Test
    void allocationIsCanceledWhenVertexIsFailedOrCanceled() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new PipelinedRegionSchedulingStrategy.Factory(),
                        new RestartAllFailoverStrategy.Factory());
        scheduler.startScheduling();

        Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        ArchivedExecutionVertex v1 = vertexIterator.next();

        assertThat(testExecutionSlotAllocator.getPendingRequests()).hasSize(2);

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        v1.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException(exceptionMessage)));

        vertexIterator =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator();
        v1 = vertexIterator.next();
        ArchivedExecutionVertex v2 = vertexIterator.next();
        assertThat(v1.getExecutionState()).isEqualTo(ExecutionState.FAILED);
        assertThat(v2.getExecutionState()).isEqualTo(ExecutionState.CANCELED);
        assertThat(testExecutionSlotAllocator.getPendingRequests()).isEmpty();
    }

    @Test
    void pendingSlotRequestsOfVerticesToRestartWillNotBeFulfilledByReturnedSlots()
            throws Exception {
        final int parallelism = 10;
        final JobGraph jobGraph = sourceSinkJobGraph(parallelism);
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testExecutionSlotAllocator.enableCompletePendingRequestsWithReturnedSlots();

        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new PipelinedRegionSchedulingStrategy.Factory(),
                        new RestartAllFailoverStrategy.Factory());
        scheduler.startScheduling();

        final ExecutionVertex ev1 =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 0);

        final Set<CompletableFuture<LogicalSlot>> pendingLogicalSlotFutures =
                testExecutionSlotAllocator.getPendingRequests().values().stream()
                        .map(ExecutionSlotAssignment::getLogicalSlotFuture)
                        .collect(Collectors.toSet());
        assertThat(pendingLogicalSlotFutures).hasSize(parallelism * 2);

        testExecutionSlotAllocator.completePendingRequest(ev1.getID());
        assertThat(pendingLogicalSlotFutures.stream().filter(CompletableFuture::isDone).count())
                .isEqualTo(1L);

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        ev1.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException(exceptionMessage)));

        assertThat(testExecutionSlotAllocator.getPendingRequests()).isEmpty();

        // the failed task will return its slot before triggering failover. And the slot
        // will be returned and re-assigned to another task which is waiting for a slot.
        // failover will be triggered after that and the re-assigned slot will be returned
        // once the attached task is canceled, but the slot will not be assigned to other
        // tasks which are identified to be restarted soon.
        assertThat(testExecutionSlotAllocator.getReturnedSlots()).hasSize(2);
        assertThat(
                        pendingLogicalSlotFutures.stream()
                                .filter(CompletableFuture::isCancelled)
                                .count())
                .isEqualTo(parallelism * 2L - 2L);
    }

    @Test
    void testExceptionHistoryWithGlobalFailOver() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionAttemptID attemptId =
                Iterables.getOnlyElement(
                                scheduler
                                        .requestJob()
                                        .getArchivedExecutionGraph()
                                        .getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();

        final Exception expectedException = new Exception("Expected exception");
        scheduler.handleGlobalFailure(expectedException);

        // we have to cancel the task and trigger the restart to have the exception history
        // populated
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attemptId, ExecutionState.CANCELED, expectedException));
        taskRestartExecutor.triggerScheduledTasks();

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory();

        assertThat(actualExceptionHistory).hasSize(1);

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();

        assertThat(
                        ExceptionHistoryEntryTestingUtils.matchesGlobalFailure(
                                failure,
                                expectedException,
                                scheduler.getExecutionGraph().getFailureInfo().getTimestamp()))
                .isTrue();
        assertThat(failure.getConcurrentExceptions()).isEmpty();
    }

    @Test
    void testExceptionHistoryWithRestartableFailure() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final TestingLogicalSlotBuilder logicalSlotBuilder = new TestingLogicalSlotBuilder();
        logicalSlotBuilder.setTaskManagerLocation(taskManagerLocation);

        executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory(logicalSlotBuilder);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        // initiate restartable failure
        final ArchivedExecutionVertex taskFailureExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final RuntimeException restartableException = new RuntimeException("restartable exception");
        final long updateStateTriggeringRestartTimestamp =
                initiateFailure(
                        scheduler,
                        taskFailureExecutionVertex.getCurrentExecutionAttempt().getAttemptId(),
                        restartableException);

        taskRestartExecutor.triggerNonPeriodicScheduledTask();

        // initiate job failure
        testRestartBackoffTimeStrategy.setCanRestart(false);

        final ExecutionAttemptID failingAttemptId =
                Iterables.getOnlyElement(
                                scheduler
                                        .requestJob()
                                        .getArchivedExecutionGraph()
                                        .getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        final RuntimeException failingException = new RuntimeException("failing exception");
        final long updateStateTriggeringJobFailureTimestamp =
                initiateFailure(scheduler, failingAttemptId, failingException);

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory();

        // assert restarted attempt
        assertThat(actualExceptionHistory).hasSize(2);
        Iterator<RootExceptionHistoryEntry> iterator = actualExceptionHistory.iterator();
        RootExceptionHistoryEntry entry0 = iterator.next();
        assertThat(
                        ExceptionHistoryEntryTestingUtils.matchesFailure(
                                entry0,
                                restartableException,
                                updateStateTriggeringRestartTimestamp,
                                taskFailureExecutionVertex.getTaskNameWithSubtaskIndex(),
                                taskFailureExecutionVertex.getCurrentAssignedResourceLocation()))
                .isTrue();
        RootExceptionHistoryEntry entry1 = iterator.next();
        assertThat(
                        ExceptionHistoryEntryTestingUtils.matchesGlobalFailure(
                                entry1, failingException, updateStateTriggeringJobFailureTimestamp))
                .isTrue();
    }

    /** Verify DefaultScheduler propagates Task failure labels as generated by Failure Enrichers. */
    @Test
    void testTaskFailureWithFailureEnricherLabels() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final TestingFailureEnricher testingFailureEnricher = new TestingFailureEnricher();
        final DefaultScheduler scheduler =
                createSchedulerAndStartScheduling(
                        jobGraph, Collections.singleton(testingFailureEnricher));

        final ExecutionAttemptID firstAttempt =
                Iterables.getOnlyElement(
                                scheduler
                                        .requestJob()
                                        .getArchivedExecutionGraph()
                                        .getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        final RuntimeException firstException = new RuntimeException("First exception");
        final long firstFailTimestamp = initiateFailure(scheduler, firstAttempt, firstException);
        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        final ArchivedExecutionVertex executionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());

        // Make sure FailureEnricher is triggered
        assertThat(testingFailureEnricher.getSeenThrowables().stream().map(t -> t.getMessage()))
                .contains(firstException.getMessage());
        // And failure labels are part of ExceptionHistory
        assertThat(scheduler.getExceptionHistory())
                .map(entry -> entry.getFailureLabelsFuture().get())
                .contains(testingFailureEnricher.getFailureLabels());

        assertThat(scheduler.getExceptionHistory())
                .anySatisfy(
                        e ->
                                ExceptionHistoryEntryTestingUtils.matchesFailure(
                                        e,
                                        firstException,
                                        firstFailTimestamp,
                                        testingFailureEnricher.getFailureLabels()));

        final RuntimeException anotherException = new RuntimeException("Another exception");
        final long anotherFailTimestamp =
                initiateFailure(
                        scheduler,
                        executionVertex.getCurrentExecutionAttempt().getAttemptId(),
                        anotherException);

        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        assertThat(testingFailureEnricher.getSeenThrowables().stream().map(t -> t.getMessage()))
                .contains(anotherException.getMessage());

        assertThat(scheduler.getExceptionHistory())
                .anySatisfy(
                        e ->
                                ExceptionHistoryEntryTestingUtils.matchesFailure(
                                        e,
                                        anotherException,
                                        anotherFailTimestamp,
                                        testingFailureEnricher.getFailureLabels()));
    }

    @Test
    void testExceptionHistoryWithPreDeployFailure() {
        // disable auto-completing slot requests to simulate timeout
        executionSlotAllocatorFactory
                .getTestExecutionSlotAllocator()
                .disableAutoCompletePendingRequests();
        final DefaultScheduler scheduler =
                createSchedulerAndStartScheduling(singleNonParallelJobVertexJobGraph());

        executionSlotAllocatorFactory.getTestExecutionSlotAllocator().timeoutPendingRequests();

        final ArchivedExecutionVertex taskFailureExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());

        // pending slot request timeout triggers a task failure that needs to be processed
        taskRestartExecutor.triggerNonPeriodicScheduledTask();

        // sanity check that the TaskManagerLocation of the failed task is indeed null, as expected
        assertThat(taskFailureExecutionVertex.getCurrentAssignedResourceLocation()).isNull();

        final ErrorInfo failureInfo =
                taskFailureExecutionVertex
                        .getFailureInfo()
                        .orElseThrow(() -> new AssertionError("A failureInfo should be set."));

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory();
        assertThat(actualExceptionHistory)
                .anySatisfy(
                        e ->
                                ExceptionHistoryEntryTestingUtils.matchesFailure(
                                        e,
                                        failureInfo.getException(),
                                        failureInfo.getTimestamp(),
                                        taskFailureExecutionVertex.getTaskNameWithSubtaskIndex(),
                                        taskFailureExecutionVertex
                                                .getCurrentAssignedResourceLocation()));
    }

    @Test
    void testExceptionHistoryConcurrentRestart() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);

        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final TestingLogicalSlotBuilder logicalSlotBuilder = new TestingLogicalSlotBuilder();
        logicalSlotBuilder.setTaskManagerLocation(taskManagerLocation);

        executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory(logicalSlotBuilder);

        final ReorganizableManuallyTriggeredScheduledExecutor delayExecutor =
                new ReorganizableManuallyTriggeredScheduledExecutor();
        final TestFailoverStrategyFactory failoverStrategyFactory =
                new TestFailoverStrategyFactory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new PipelinedRegionSchedulingStrategy.Factory(),
                        failoverStrategyFactory,
                        delayExecutor);
        scheduler.startScheduling();

        final ExecutionVertex executionVertex0 =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 0);
        final ExecutionVertex executionVertex1 =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 1);

        // single-ExecutionVertex failure
        final RuntimeException exception0 = new RuntimeException("failure #0");
        failoverStrategyFactory.setTasksToRestart(executionVertex0.getID());
        final long updateStateTriggeringRestartTimestamp0 =
                initiateFailure(
                        scheduler,
                        executionVertex0.getCurrentExecutionAttempt().getAttemptId(),
                        exception0);

        // multi-ExecutionVertex failure
        final RuntimeException exception1 = new RuntimeException("failure #1");
        failoverStrategyFactory.setTasksToRestart(
                executionVertex1.getID(), executionVertex0.getID());
        final long updateStateTriggeringRestartTimestamp1 =
                initiateFailure(
                        scheduler,
                        executionVertex1.getCurrentExecutionAttempt().getAttemptId(),
                        exception1);

        // there might be a race condition with the delayExecutor if the tasks are scheduled quite
        // close to each other which we want to simulate here
        Collections.reverse(delayExecutor.getCollectedScheduledTasks());

        delayExecutor.triggerNonPeriodicScheduledTasks();

        assertThat(scheduler.getExceptionHistory()).hasSize(2);
        final Iterator<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory().iterator();

        final RootExceptionHistoryEntry entry0 = actualExceptionHistory.next();
        assertThat(
                        ExceptionHistoryEntryTestingUtils.matchesFailure(
                                entry0,
                                exception0,
                                updateStateTriggeringRestartTimestamp0,
                                executionVertex0.getTaskNameWithSubtaskIndex(),
                                executionVertex0.getCurrentAssignedResourceLocation()))
                .isTrue();
        assertThat(entry0.getConcurrentExceptions())
                .anySatisfy(
                        e ->
                                ExceptionHistoryEntryTestingUtils.matchesFailure(
                                        e,
                                        exception1,
                                        updateStateTriggeringRestartTimestamp1,
                                        executionVertex1.getTaskNameWithSubtaskIndex(),
                                        executionVertex1.getCurrentAssignedResourceLocation()));

        final RootExceptionHistoryEntry entry1 = actualExceptionHistory.next();
        assertThat(
                        ExceptionHistoryEntryTestingUtils.matchesFailure(
                                entry1,
                                exception1,
                                updateStateTriggeringRestartTimestamp1,
                                executionVertex1.getTaskNameWithSubtaskIndex(),
                                executionVertex1.getCurrentAssignedResourceLocation()))
                .isTrue();
        assertThat(entry1.getConcurrentExceptions()).isEmpty();
    }

    @Test
    void testExceptionHistoryTruncation() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        configuration.set(WebOptions.MAX_EXCEPTION_HISTORY_SIZE, 1);
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionAttemptID attemptId0 =
                Iterables.getOnlyElement(
                                scheduler
                                        .requestJob()
                                        .getArchivedExecutionGraph()
                                        .getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        initiateFailure(scheduler, attemptId0, new RuntimeException("old exception"));
        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        final ArchivedExecutionVertex executionVertex1 =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final RuntimeException exception = new RuntimeException("relevant exception");
        final long relevantTimestamp =
                initiateFailure(
                        scheduler,
                        executionVertex1.getCurrentExecutionAttempt().getAttemptId(),
                        exception);

        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        assertThat(scheduler.getExceptionHistory())
                .anySatisfy(
                        e ->
                                ExceptionHistoryEntryTestingUtils.matchesFailure(
                                        e,
                                        exception,
                                        relevantTimestamp,
                                        executionVertex1.getTaskNameWithSubtaskIndex(),
                                        executionVertex1.getCurrentAssignedResourceLocation()));
    }

    @Test
    void testStatusMetrics() throws Exception {
        // running time acts as a stand-in for generic status time metrics
        final CompletableFuture<Gauge<Long>> runningTimeMetricFuture = new CompletableFuture<>();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, name, group) -> {
                                    switch (name) {
                                        case "runningTimeTotal":
                                            runningTimeMetricFuture.complete((Gauge<Long>) metric);
                                            break;
                                    }
                                })
                        .build();

        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final Configuration configuration = new Configuration();
        configuration.set(
                MetricOptions.JOB_STATUS_METRICS,
                Arrays.asList(MetricOptions.JobStatusMetrics.TOTAL_TIME));

        final ComponentMainThreadExecutor singleThreadMainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);

        final Time slotTimeout = Time.milliseconds(5L);
        final SlotPool slotPool =
                new DeclarativeSlotPoolBridgeBuilder()
                        .setBatchSlotTimeout(slotTimeout)
                        .buildAndStart(singleThreadMainThreadExecutor);
        final PhysicalSlotProvider slotProvider =
                new PhysicalSlotProviderImpl(
                        LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);

        final DefaultScheduler scheduler =
                createSchedulerBuilder(jobGraph, singleThreadMainThreadExecutor)
                        .setJobMasterConfiguration(configuration)
                        .setJobManagerJobMetricGroup(
                                JobManagerMetricGroup.createJobManagerMetricGroup(
                                                metricRegistry, "localhost")
                                        .addJob(new JobID(), "jobName"))
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        slotProvider, slotTimeout))
                        .build();

        final AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway taskManagerGateway =
                new AdaptiveSchedulerTest.SubmissionBufferingTaskManagerGateway(1);

        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        assertThat(slotPool.registerTaskManager(taskManagerLocation.getResourceID())).isTrue();

        taskManagerGateway.setCancelConsumer(
                executionAttemptId -> {
                    singleThreadMainThreadExecutor.execute(
                            () ->
                                    scheduler.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    executionAttemptId, ExecutionState.CANCELED)));
                });

        singleThreadMainThreadExecutor.execute(
                () -> {
                    scheduler.startScheduling();

                    slotPool.offerSlots(
                            taskManagerLocation,
                            taskManagerGateway,
                            createSlotOffersForResourceRequirements(
                                    ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1)));
                });

        // wait for the first task submission
        taskManagerGateway.waitForSubmissions(1);

        // sleep a bit to ensure uptime is > 0
        Thread.sleep(10L);

        final Gauge<Long> runningTimeGauge = runningTimeMetricFuture.get();
        assertThat(runningTimeGauge.getValue()).isGreaterThan(0L);
    }

    @Test
    void testDeploymentWaitForProducedPartitionRegistration() {
        shuffleMaster.setAutoCompleteRegistration(false);

        final List<ResultPartitionID> trackedPartitions = new ArrayList<>();
        partitionTracker.setStartTrackingPartitionsConsumer(
                (resourceID, resultPartitionDeploymentDescriptor) ->
                        trackedPartitions.add(
                                resultPartitionDeploymentDescriptor
                                        .getShuffleDescriptor()
                                        .getResultPartitionID()));

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();

        createSchedulerAndStartScheduling(jobGraph);

        assertThat(trackedPartitions).isEmpty();
        assertThat(testExecutionOperations.getDeployedVertices()).isEmpty();

        shuffleMaster.completeAllPendingRegistrations();
        assertThat(trackedPartitions).hasSize(1);
        assertThat(testExecutionOperations.getDeployedVertices()).hasSize(2);
    }

    @Test
    void testFailedProducedPartitionRegistration() {
        shuffleMaster.setAutoCompleteRegistration(false);

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();

        createSchedulerAndStartScheduling(jobGraph);

        assertThat(testExecutionOperations.getCanceledVertices()).isEmpty();
        assertThat(testExecutionOperations.getFailedVertices()).isEmpty();

        shuffleMaster.failAllPendingRegistrations();
        assertThat(testExecutionOperations.getCanceledVertices()).hasSize(2);
        assertThat(testExecutionOperations.getFailedVertices()).hasSize(1);
    }

    @Test
    void testDirectExceptionOnProducedPartitionRegistration() {
        shuffleMaster.setThrowExceptionalOnRegistration(true);

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();

        createSchedulerAndStartScheduling(jobGraph);

        assertThat(testExecutionOperations.getCanceledVertices()).hasSize(2);
        assertThat(testExecutionOperations.getFailedVertices()).hasSize(1);
    }

    @Test
    void testProducedPartitionRegistrationTimeout() throws Exception {
        ScheduledExecutorService scheduledExecutorService = null;
        try {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            final ComponentMainThreadExecutor mainThreadExecutor =
                    ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                            scheduledExecutorService);

            shuffleMaster.setAutoCompleteRegistration(false);

            final JobGraph jobGraph = nonParallelSourceSinkJobGraph();

            timeout = Time.milliseconds(1);
            createSchedulerAndStartScheduling(jobGraph, mainThreadExecutor);

            testExecutionOperations.awaitCanceledExecutions(2);
            testExecutionOperations.awaitFailedExecutions(1);
        } finally {
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }
        }
    }

    @Test
    void testLateRegisteredPartitionsWillBeReleased() {
        shuffleMaster.setAutoCompleteRegistration(false);

        final List<ResultPartitionID> trackedPartitions = new ArrayList<>();
        partitionTracker.setStartTrackingPartitionsConsumer(
                (resourceID, resultPartitionDeploymentDescriptor) ->
                        trackedPartitions.add(
                                resultPartitionDeploymentDescriptor
                                        .getShuffleDescriptor()
                                        .getResultPartitionID()));

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex sourceExecutionVertex =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator()
                        .next();
        final ExecutionAttemptID attemptId =
                sourceExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));

        // late registered partitions will not be tracked and will be released
        shuffleMaster.completeAllPendingRegistrations();
        assertThat(trackedPartitions).isEmpty();
        assertThat(shuffleMaster.getExternallyReleasedPartitions()).hasSize(1);
    }

    @Test
    void testCheckpointCleanerIsClosedAfterCheckpointServices() throws Exception {
        final ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        try {
            doTestCheckpointCleanerIsClosedAfterCheckpointServices(
                    (checkpointRecoveryFactory, checkpointCleaner) -> {
                        final JobGraph jobGraph = singleJobVertexJobGraph(1);
                        enableCheckpointing(jobGraph);
                        try {
                            return new DefaultSchedulerBuilder(
                                            jobGraph,
                                            ComponentMainThreadExecutorServiceAdapter
                                                    .forSingleThreadExecutor(executorService),
                                            executorService)
                                    .setCheckpointRecoveryFactory(checkpointRecoveryFactory)
                                    .setCheckpointCleaner(checkpointCleaner)
                                    .build();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    executorService,
                    log);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void testJobStatusHookWithJobFailed() throws Exception {
        commonJobStatusHookTest(ExecutionState.FAILED, JobStatus.FAILED);
    }

    @Test
    void testJobStatusHookWithJobCanceled() throws Exception {
        commonJobStatusHookTest(ExecutionState.CANCELED, JobStatus.CANCELED);
    }

    @Test
    void testJobStatusHookWithJobFinished() throws Exception {
        commonJobStatusHookTest(ExecutionState.FINISHED, JobStatus.FINISHED);
    }

    private void commonJobStatusHookTest(
            ExecutionState expectedExecutionState, JobStatus expectedJobStatus) throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        TestingJobStatusHook jobStatusHook = new TestingJobStatusHook();

        final List<JobID> onCreatedJobList = new LinkedList<>();
        jobStatusHook.setOnCreatedConsumer((jobId) -> onCreatedJobList.add(jobId));

        final List<JobID> onJobStatusList = new LinkedList<>();
        switch (expectedJobStatus) {
            case FAILED:
                jobStatusHook.setOnFailedConsumer((jobID, throwable) -> onJobStatusList.add(jobID));
                break;
            case CANCELED:
                jobStatusHook.setOnCanceledConsumer((jobID) -> onJobStatusList.add(jobID));
                break;
            case FINISHED:
                jobStatusHook.setOnFinishedConsumer((jobID) -> onJobStatusList.add(jobID));
                break;
            default:
                throw new UnsupportedOperationException(
                        "JobStatusHook test is not supported: " + expectedJobStatus);
        }

        List<JobStatusHook> jobStatusHooks = new ArrayList<>();
        jobStatusHooks.add(jobStatusHook);
        jobGraph.setJobStatusHooks(jobStatusHooks);

        testRestartBackoffTimeStrategy.setCanRestart(false);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        if (JobStatus.CANCELED == expectedJobStatus) {
            scheduler.cancel();
        }
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attemptId, expectedExecutionState));

        taskRestartExecutor.triggerScheduledTasks();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus).isEqualTo(expectedJobStatus);
        assertThat(onCreatedJobList).singleElement().isEqualTo(jobGraph.getJobID());

        assertThat(onCreatedJobList).singleElement().isEqualTo(jobGraph.getJobID());
    }

    /**
     * Visible for re-use in {@link
     * org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerTest}.
     */
    public static void doTestCheckpointCleanerIsClosedAfterCheckpointServices(
            BiFunction<CheckpointRecoveryFactory, CheckpointsCleaner, SchedulerNG> schedulerFactory,
            ScheduledExecutorService executorService,
            Logger logger)
            throws Exception {
        final CountDownLatch checkpointServicesShutdownBlocked = new CountDownLatch(1);
        final CountDownLatch cleanerClosed = new CountDownLatch(1);
        final CompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(1) {

                    @Override
                    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
                            throws Exception {
                        checkpointServicesShutdownBlocked.await();
                        super.shutdown(jobStatus, checkpointsCleaner);
                    }
                };
        final CheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter() {

                    @Override
                    public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
                        try {
                            checkpointServicesShutdownBlocked.await();
                        } catch (InterruptedException e) {
                            logger.error(
                                    "An error occurred while executing waiting for the CheckpointServices shutdown.",
                                    e);
                            Thread.currentThread().interrupt();
                        }

                        return super.shutdown(jobStatus);
                    }
                };
        final CheckpointsCleaner checkpointsCleaner =
                new CheckpointsCleaner() {

                    @Override
                    public synchronized CompletableFuture<Void> closeAsync() {
                        cleanerClosed.countDown();
                        return super.closeAsync();
                    }
                };

        final SchedulerNG scheduler =
                schedulerFactory.apply(
                        new TestingCheckpointRecoveryFactory(
                                completedCheckpointStore, checkpointIDCounter),
                        checkpointsCleaner);
        final CompletableFuture<Void> schedulerClosed = new CompletableFuture<>();
        final CountDownLatch schedulerClosing = new CountDownLatch(1);

        executorService.submit(
                () -> {
                    scheduler.closeAsync().thenRun(() -> schedulerClosed.complete(null));
                    schedulerClosing.countDown();
                });

        // Wait for scheduler to start closing.
        schedulerClosing.await();
        assertThat(cleanerClosed.await(10, TimeUnit.MILLISECONDS))
                .withFailMessage("CheckpointCleaner should not close before checkpoint services.")
                .isFalse();
        checkpointServicesShutdownBlocked.countDown();
        cleanerClosed.await();
        schedulerClosed.get();
    }

    private static long initiateFailure(
            DefaultScheduler scheduler,
            ExecutionAttemptID executionAttemptId,
            Throwable exception) {
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(executionAttemptId, ExecutionState.FAILED, exception));
        return getFailureTimestamp(scheduler, executionAttemptId);
    }

    private static long getFailureTimestamp(
            DefaultScheduler scheduler, ExecutionAttemptID executionAttemptId) {
        final ExecutionVertex failedExecutionVertex =
                StreamSupport.stream(
                                scheduler
                                        .getExecutionGraph()
                                        .getAllExecutionVertices()
                                        .spliterator(),
                                false)
                        .filter(
                                v ->
                                        executionAttemptId.equals(
                                                v.getCurrentExecutionAttempt().getAttemptId()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No ExecutionVertex available for the passed ExecutionAttemptId "
                                                        + executionAttemptId));
        return failedExecutionVertex
                .getFailureInfo()
                .map(ErrorInfo::getTimestamp)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No failure was set for ExecutionVertex having the passed execution "
                                                + executionAttemptId));
    }

    private static JobVertex createVertex(String name, int parallelism) {
        final JobVertex v = new JobVertex(name);
        v.setParallelism(parallelism);
        v.setInvokableClass(AbstractInvokable.class);
        return v;
    }

    private void waitForTermination(final DefaultScheduler scheduler) throws Exception {
        scheduler.getJobTerminationFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    public static JobGraph singleNonParallelJobVertexJobGraph() {
        return singleJobVertexJobGraph(1);
    }

    private static JobGraph singleJobVertexJobGraph(final int parallelism) {
        final JobVertex vertex = new JobVertex("source");
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(parallelism);
        return JobGraphTestUtils.streamingJobGraph(vertex);
    }

    private static JobGraph nonParallelSourceSinkJobGraph() {
        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(source, sink);
    }

    private static JobGraph sourceSinkJobGraph(final int parallelism) {
        final JobVertex source = new JobVertex("source");
        source.setParallelism(parallelism);
        source.setInvokableClass(NoOpInvokable.class);

        final JobVertex sink = new JobVertex("sink");
        sink.setParallelism(parallelism);
        sink.setInvokableClass(NoOpInvokable.class);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(source, sink);
    }

    private static JobVertex getOnlyJobVertex(final JobGraph jobGraph) {
        final List<JobVertex> sortedVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        Preconditions.checkState(sortedVertices.size() == 1);
        return sortedVertices.get(0);
    }

    private DefaultScheduler createSchedulerAndStartScheduling(
            final JobGraph jobGraph, final Collection<FailureEnricher> failureEnrichers) {
        return createSchedulerAndStartScheduling(
                jobGraph,
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                failureEnrichers);
    }

    private DefaultScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
        return createSchedulerAndStartScheduling(
                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    private DefaultScheduler createSchedulerAndStartScheduling(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor) {
        return createSchedulerAndStartScheduling(
                jobGraph, mainThreadExecutor, Collections.emptySet());
    }

    private DefaultScheduler createSchedulerAndStartScheduling(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final Collection<FailureEnricher> failureEnrichers) {

        try {
            final DefaultScheduler scheduler =
                    createSchedulerBuilder(jobGraph, mainThreadExecutor, failureEnrichers).build();
            mainThreadExecutor.execute(scheduler::startScheduling);
            return scheduler;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory)
            throws Exception {
        return createSchedulerBuilder(jobGraph, mainThreadExecutor, Collections.emptySet())
                .setSchedulingStrategyFactory(schedulingStrategyFactory)
                .build();
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory)
            throws Exception {
        return createSchedulerBuilder(jobGraph, mainThreadExecutor, Collections.emptySet())
                .setSchedulingStrategyFactory(schedulingStrategyFactory)
                .setFailoverStrategyFactory(failoverStrategyFactory)
                .build();
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final ScheduledExecutor delayExecutor)
            throws Exception {
        return createSchedulerBuilder(jobGraph, mainThreadExecutor, Collections.emptySet())
                .setDelayExecutor(delayExecutor)
                .setSchedulingStrategyFactory(schedulingStrategyFactory)
                .setFailoverStrategyFactory(failoverStrategyFactory)
                .build();
    }

    private DefaultSchedulerBuilder createSchedulerBuilder(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor) {
        return createSchedulerBuilder(jobGraph, mainThreadExecutor, Collections.emptySet());
    }

    private DefaultSchedulerBuilder createSchedulerBuilder(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final Collection<FailureEnricher> failureEnrichers) {
        return new DefaultSchedulerBuilder(
                        jobGraph,
                        mainThreadExecutor,
                        executor,
                        scheduledExecutorService,
                        taskRestartExecutor)
                .setLogger(log)
                .setJobMasterConfiguration(configuration)
                .setSchedulingStrategyFactory(new PipelinedRegionSchedulingStrategy.Factory())
                .setFailoverStrategyFactory(new RestartPipelinedRegionFailoverStrategy.Factory())
                .setRestartBackoffTimeStrategy(testRestartBackoffTimeStrategy)
                .setExecutionOperations(testExecutionOperations)
                .setExecutionVertexVersioner(executionVertexVersioner)
                .setExecutionSlotAllocatorFactory(executionSlotAllocatorFactory)
                .setFailureEnrichers(failureEnrichers)
                .setShuffleMaster(shuffleMaster)
                .setPartitionTracker(partitionTracker)
                .setRpcTimeout(timeout);
    }

    /**
     * {@code ReorganizableManuallyTriggeredScheduledExecutor} can be used to re-organize scheduled
     * tasks before actually triggering them. This can be used to test cases with race conditions in
     * the delayed scheduler.
     */
    private static class ReorganizableManuallyTriggeredScheduledExecutor
            extends ManuallyTriggeredScheduledExecutor {

        private final List<ScheduledTask<?>> scheduledTasks = new ArrayList<>();

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return schedule(
                    () -> {
                        command.run();
                        return null;
                    },
                    delay,
                    unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            final ScheduledTask<V> scheduledTask =
                    new ScheduledTask<>(callable, unit.convert(delay, TimeUnit.MILLISECONDS));
            scheduledTasks.add(scheduledTask);
            return scheduledTask;
        }

        /**
         * Returns the collected {@link ScheduledTask ScheduledTasks}. This collection can be
         * re-organized in-place.
         *
         * @return The list of scheduled tasks.
         */
        public List<ScheduledTask<?>> getCollectedScheduledTasks() {
            return scheduledTasks;
        }

        /** Actually schedules the collected {@link ScheduledTask ScheduledTasks}. */
        void scheduleCollectedScheduledTasks() {
            for (ScheduledTask<?> scheduledTask : scheduledTasks) {
                super.schedule(
                        scheduledTask.getCallable(),
                        scheduledTask.getDelay(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS);
            }
            scheduledTasks.clear();
        }

        /**
         * Schedules all already collected tasks before actually triggering the actual scheduling of
         * the next task in the queue.
         */
        @Override
        public void triggerNonPeriodicScheduledTask() {
            scheduleCollectedScheduledTasks();
            super.triggerNonPeriodicScheduledTask();
        }

        /**
         * Schedules all already collected tasks before actually triggering the actual scheduling of
         * all tasks in the queue.
         */
        @Override
        public void triggerNonPeriodicScheduledTasks() {
            scheduleCollectedScheduledTasks();
            super.triggerNonPeriodicScheduledTasks();
        }
    }

    /**
     * Since checkpoint is triggered asynchronously, we need to figure out when checkpoint is really
     * triggered. Note that this should be invoked before scheduler initialized.
     *
     * @return the latch representing checkpoint is really triggered
     */
    private CountDownLatch getCheckpointTriggeredLatch() {
        final CountDownLatch checkpointTriggeredLatch = new CountDownLatch(1);
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        testExecutionSlotAllocator
                .getLogicalSlotBuilder()
                .setTaskManagerGateway(taskManagerGateway);
        taskManagerGateway.setCheckpointConsumer(
                (executionAttemptID, jobId, checkpointId, timestamp, checkpointOptions) -> {
                    checkpointTriggeredLatch.countDown();
                });
        return checkpointTriggeredLatch;
    }

    private void transitionToRunning(DefaultScheduler scheduler, ExecutionAttemptID attemptId) {
        Preconditions.checkState(
                scheduler.updateTaskExecutionState(
                        new TaskExecutionState(attemptId, ExecutionState.INITIALIZING)));
        Preconditions.checkState(
                scheduler.updateTaskExecutionState(
                        new TaskExecutionState(attemptId, ExecutionState.RUNNING)));
    }
}
