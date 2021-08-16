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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.hooks.TestMasterHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartAllFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.TestFailoverStrategyFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntryMatcher;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestSchedulingStrategy;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.collection.IsIterableWithSize;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.acknowledgePendingCheckpoint;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.enableCheckpointing;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.getCheckpointCoordinator;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link DefaultScheduler}. */
public class DefaultSchedulerTest extends TestLogger {

    private static final int TIMEOUT_MS = 1000;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private ManuallyTriggeredScheduledExecutor taskRestartExecutor =
            new ManuallyTriggeredScheduledExecutor();

    private ExecutorService executor;

    private ScheduledExecutorService scheduledExecutorService;

    private Configuration configuration;

    private TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy;

    private TestExecutionVertexOperationsDecorator testExecutionVertexOperations;

    private ExecutionVertexVersioner executionVertexVersioner;

    private TestExecutionSlotAllocatorFactory executionSlotAllocatorFactory;

    private TestExecutionSlotAllocator testExecutionSlotAllocator;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        scheduledExecutorService = new DirectScheduledExecutorService();

        configuration = new Configuration();

        testRestartBackoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, 0);

        testExecutionVertexOperations =
                new TestExecutionVertexOperationsDecorator(new DefaultExecutionVertexOperations());

        executionVertexVersioner = new ExecutionVertexVersioner();

        executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory();
        testExecutionSlotAllocator = executionSlotAllocatorFactory.getTestExecutionSlotAllocator();
    }

    @After
    public void tearDown() throws Exception {
        if (scheduledExecutorService != null) {
            ExecutorUtils.gracefulShutdown(
                    TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduledExecutorService);
        }

        if (executor != null) {
            ExecutorUtils.gracefulShutdown(TIMEOUT_MS, TimeUnit.MILLISECONDS, executor);
        }
    }

    @Test
    public void startScheduling() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        createSchedulerAndStartScheduling(jobGraph);

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId));
    }

    @Test
    public void testCorrectSettingOfInitializationTimestamp() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionGraphInfo executionGraphInfo = scheduler.requestJob();
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();

        // ensure all statuses are set in the ExecutionGraph
        assertThat(
                archivedExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING), greaterThan(0L));
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED), greaterThan(0L));
        assertThat(archivedExecutionGraph.getStatusTimestamp(JobStatus.RUNNING), greaterThan(0L));

        // ensure correct order
        assertThat(
                archivedExecutionGraph.getStatusTimestamp(JobStatus.INITIALIZING)
                        <= archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED),
                Is.is(true));
    }

    @Test
    public void deployTasksOnlyWhenAllSlotRequestsAreFulfilled() throws Exception {
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

        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(0));

        testExecutionSlotAllocator.completePendingRequest(verticesToSchedule.get(0));
        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(0));

        testExecutionSlotAllocator.completePendingRequests();
        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(4));
    }

    @Test
    public void scheduledVertexOrderFromSchedulingStrategyIsRespected() throws Exception {
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
                testExecutionVertexOperations.getDeployedVertices();

        assertEquals(desiredScheduleOrder, deployedExecutionVertices);
    }

    @Test
    public void restartAfterDeploymentFails() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        testExecutionVertexOperations.enableFailDeploy();

        createSchedulerAndStartScheduling(jobGraph);

        testExecutionVertexOperations.disableFailDeploy();
        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void restartFailedTask() {
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
                testExecutionVertexOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void updateTaskExecutionStateReturnsFalseIfExecutionDoesNotExist() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final TaskExecutionState taskExecutionState =
                createFailedTaskExecutionState(new ExecutionAttemptID());

        assertFalse(scheduler.updateTaskExecutionState(taskExecutionState));
    }

    @Test
    public void failJobIfCannotRestart() throws Exception {
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
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
    }

    @Test
    public void failJobIfNotEnoughResources() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        testRestartBackoffTimeStrategy.setCanRestart(false);
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        testExecutionSlotAllocator.timeoutPendingRequests();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));

        Throwable failureCause =
                scheduler
                        .requestJob()
                        .getArchivedExecutionGraph()
                        .getFailureInfo()
                        .getException()
                        .deserializeError(DefaultSchedulerTest.class.getClassLoader());
        assertTrue(findThrowable(failureCause, NoResourceAvailableException.class).isPresent());
        assertTrue(
                findThrowableWithMessage(
                                failureCause,
                                "Could not allocate the required slot within slot request timeout.")
                        .isPresent());
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
    }

    @Test
    public void restartVerticesOnSlotAllocationTimeout() throws Exception {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testRestartVerticesOnFailuresInScheduling(
                vid -> testExecutionSlotAllocator.timeoutPendingRequest(vid));
    }

    @Test
    public void restartVerticesOnAssignedSlotReleased() throws Exception {
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

        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(4));

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
        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(2));
        assertThat(ev11.getExecutionState(), is(ExecutionState.FAILED));
        assertThat(ev21.getExecutionState(), is(ExecutionState.CANCELED));
        assertThat(ev12.getExecutionState(), is(ExecutionState.SCHEDULED));
        assertThat(ev22.getExecutionState(), is(ExecutionState.SCHEDULED));

        taskRestartExecutor.triggerScheduledTasks();
        assertThat(
                schedulingStrategy.getReceivedVerticesToRestart(),
                containsInAnyOrder(vid11, vid21));
    }

    @Test
    public void skipDeploymentIfVertexVersionOutdated() {
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

        assertThat(
                testExecutionVertexOperations.getDeployedVertices(),
                containsInAnyOrder(sourceExecutionVertexId, sinkExecutionVertexId));
        assertThat(
                scheduler.requestJob().getArchivedExecutionGraph().getState(),
                is(equalTo(JobStatus.RUNNING)));
    }

    @Test
    public void releaseSlotIfVertexVersionOutdated() {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(getOnlyJobVertex(jobGraph).getID(), 0);

        createSchedulerAndStartScheduling(jobGraph);

        executionVertexVersioner.recordModification(onlyExecutionVertexId);
        testExecutionSlotAllocator.completePendingRequests();

        assertThat(testExecutionSlotAllocator.getReturnedSlots(), hasSize(1));
    }

    @Test
    public void vertexIsResetBeforeRestarted() throws Exception {
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

        assertThat(schedulingStrategy.getReceivedVerticesToRestart(), hasSize(1));
        assertThat(onlySchedulingVertex.getState(), is(equalTo(ExecutionState.CREATED)));
    }

    @Test
    public void scheduleOnlyIfVertexIsCreated() throws Exception {
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
        try {
            schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertexId));
            fail("IllegalStateException should happen");
        } catch (IllegalStateException e) {
            // expected exception
        }
    }

    @Test
    public void handleGlobalFailure() {
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
                testExecutionVertexOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    /**
     * This test covers the use-case where a global fail-over is followed by a local task failure.
     * It verifies (besides checking the expected deployments) that the assert in the global
     * recovery handling of {@link SchedulerBase#restoreState} is not triggered due to version
     * updates.
     */
    @Test
    public void handleGlobalFailureWithLocalFailure() {
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
        assertThat(
                "The execution vertices should be deployed in a specific order reflecting the scheduling start and the global fail-over afterwards.",
                testExecutionVertexOperations.getDeployedVertices(),
                contains(
                        executionVertexId0,
                        executionVertexId1,
                        executionVertexId0,
                        executionVertexId1));
    }

    @Test
    public void testStartingCheckpointSchedulerAfterExecutionGraphFinished() {
        assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
                SchedulerBase::startCheckpointScheduler);
    }

    @Test
    public void testStoppingCheckpointSchedulerAfterExecutionGraphFinished() {
        assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
                SchedulerBase::stopCheckpointScheduler);
    }

    private void assertCheckpointSchedulingOperationHavingNoEffectAfterJobFinished(
            Consumer<DefaultScheduler> callSchedulingOperation) {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        assertThat(scheduler.getCheckpointCoordinator(), is(notNullValue()));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        Iterables.getOnlyElement(
                                        scheduler.getExecutionGraph().getAllExecutionVertices())
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        ExecutionState.FINISHED));

        assertThat(scheduler.getCheckpointCoordinator(), is(nullValue()));
        callSchedulingOperation.accept(scheduler);
        assertThat(scheduler.getCheckpointCoordinator(), is(nullValue()));
    }

    @Test
    public void vertexIsNotAffectedByOutdatedDeployment() {
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
        assertThat(sv1.getState(), is(equalTo(ExecutionState.SCHEDULED)));
    }

    @Test
    public void abortPendingCheckpointsWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

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
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(1)));

        scheduler.updateTaskExecutionState(createFailedTaskExecutionState(attemptId));
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(0)));
    }

    @Test
    public void restoreStateWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

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
        assertThat(masterHook.getRestoreCount(), is(equalTo(1)));
    }

    @Test
    public void failGlobalWhenRestoringStateFails() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

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
        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        // the first task failover should be skipped on state restore failure
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId));

        // a global failure should be triggered on state restore failure
        masterHook.disableFailOnRestore();
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void failJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.failJob(new FlinkException("Test failure."), System.currentTimeMillis());

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void cancelJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.cancel();

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void suspendJobWillIncrementVertexVersions() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.close();

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void jobStatusIsRestartingIfOneVertexIsWaitingForRestart() {
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

        assertThat(jobStatusAfterFirstFailure, equalTo(JobStatus.RESTARTING));
        assertThat(jobStatusWithPendingRestarts, equalTo(JobStatus.RESTARTING));
        assertThat(jobStatusAfterRestarts, equalTo(JobStatus.RUNNING));
    }

    @Test
    public void cancelWhileRestartingShouldWaitForRunningTasks() {
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
        final ExecutionVertexID executionVertex2 =
                scheduler.getExecutionVertexIdOrThrow(attemptId2);

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

        assertThat(vertex2StateAfterCancel, is(equalTo(ExecutionState.CANCELING)));
        assertThat(statusAfterCancelWhileRestarting, is(equalTo(JobStatus.CANCELLING)));
        assertThat(scheduler.requestJobStatus(), is(equalTo(JobStatus.CANCELED)));
    }

    @Test
    public void failureInfoIsSetAfterTaskFailure() {
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
        assertThat(failureInfo, is(notNullValue()));
        assertThat(failureInfo.getExceptionAsString(), containsString(exceptionMessage));
    }

    @Test
    public void allocationIsCanceledWhenVertexIsFailedOrCanceled() throws Exception {
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

        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(2));

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
        assertThat(v1.getExecutionState(), is(ExecutionState.FAILED));
        assertThat(v2.getExecutionState(), is(ExecutionState.CANCELED));
        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(0));
    }

    @Test
    public void pendingSlotRequestsOfVerticesToRestartWillNotBeFulfilledByReturnedSlots()
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
                        .map(SlotExecutionVertexAssignment::getLogicalSlotFuture)
                        .collect(Collectors.toSet());
        assertThat(pendingLogicalSlotFutures, hasSize(parallelism * 2));

        testExecutionSlotAllocator.completePendingRequest(ev1.getID());
        assertThat(
                pendingLogicalSlotFutures.stream().filter(CompletableFuture::isDone).count(),
                is(1L));

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        ev1.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException(exceptionMessage)));

        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(0));

        // the failed task will return its slot before triggering failover. And the slot
        // will be returned and re-assigned to another task which is waiting for a slot.
        // failover will be triggered after that and the re-assigned slot will be returned
        // once the attached task is canceled, but the slot will not be assigned to other
        // tasks which are identified to be restarted soon.
        assertThat(testExecutionSlotAllocator.getReturnedSlots(), hasSize(2));
        assertThat(
                pendingLogicalSlotFutures.stream().filter(CompletableFuture::isCancelled).count(),
                is(parallelism * 2L - 2L));
    }

    @Test
    public void testExceptionHistoryWithGlobalFailOver() {
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
        final long end = System.currentTimeMillis();

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory();

        assertThat(actualExceptionHistory, IsIterableWithSize.iterableWithSize(1));

        final RootExceptionHistoryEntry failure = actualExceptionHistory.iterator().next();
        assertThat(
                failure,
                ExceptionHistoryEntryMatcher.matchesGlobalFailure(
                        expectedException,
                        scheduler.getExecutionGraph().getFailureInfo().getTimestamp()));
        assertThat(failure.getConcurrentExceptions(), IsEmptyIterable.emptyIterable());
    }

    @Test
    public void testExceptionHistoryWithRestartableFailure() {
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
        assertThat(
                actualExceptionHistory,
                IsIterableContainingInOrder.contains(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                restartableException,
                                updateStateTriggeringRestartTimestamp,
                                taskFailureExecutionVertex.getTaskNameWithSubtaskIndex(),
                                taskFailureExecutionVertex.getCurrentAssignedResourceLocation()),
                        ExceptionHistoryEntryMatcher.matchesGlobalFailure(
                                failingException, updateStateTriggeringJobFailureTimestamp)));
    }

    @Test
    public void testExceptionHistoryWithPreDeployFailure() {
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
        assertThat(
                taskFailureExecutionVertex.getCurrentAssignedResourceLocation(), is(nullValue()));

        final ErrorInfo failureInfo =
                taskFailureExecutionVertex
                        .getFailureInfo()
                        .orElseThrow(() -> new AssertionError("A failureInfo should be set."));

        final Iterable<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory();
        assertThat(
                actualExceptionHistory,
                IsIterableContainingInOrder.contains(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                failureInfo.getException(),
                                failureInfo.getTimestamp(),
                                taskFailureExecutionVertex.getTaskNameWithSubtaskIndex(),
                                taskFailureExecutionVertex.getCurrentAssignedResourceLocation())));
    }

    @Test
    public void testExceptionHistoryConcurrentRestart() throws Exception {
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

        assertThat(scheduler.getExceptionHistory(), IsIterableWithSize.iterableWithSize(2));
        final Iterator<RootExceptionHistoryEntry> actualExceptionHistory =
                scheduler.getExceptionHistory().iterator();

        final RootExceptionHistoryEntry entry0 = actualExceptionHistory.next();
        assertThat(
                entry0,
                is(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                exception0,
                                updateStateTriggeringRestartTimestamp0,
                                executionVertex0.getTaskNameWithSubtaskIndex(),
                                executionVertex0.getCurrentAssignedResourceLocation())));
        assertThat(
                entry0.getConcurrentExceptions(),
                IsIterableContainingInOrder.contains(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                exception1,
                                updateStateTriggeringRestartTimestamp1,
                                executionVertex1.getTaskNameWithSubtaskIndex(),
                                executionVertex1.getCurrentAssignedResourceLocation())));

        final RootExceptionHistoryEntry entry1 = actualExceptionHistory.next();
        assertThat(
                entry1,
                is(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                exception1,
                                updateStateTriggeringRestartTimestamp1,
                                executionVertex1.getTaskNameWithSubtaskIndex(),
                                executionVertex1.getCurrentAssignedResourceLocation())));
        assertThat(entry1.getConcurrentExceptions(), IsEmptyIterable.emptyIterable());
    }

    @Test
    public void testExceptionHistoryTruncation() {
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

        assertThat(
                scheduler.getExceptionHistory(),
                IsIterableContainingInOrder.contains(
                        ExceptionHistoryEntryMatcher.matchesFailure(
                                exception,
                                relevantTimestamp,
                                executionVertex1.getTaskNameWithSubtaskIndex(),
                                executionVertex1.getCurrentAssignedResourceLocation())));
    }

    private static TaskExecutionState createFailedTaskExecutionState(
            ExecutionAttemptID executionAttemptID) {
        return new TaskExecutionState(
                executionAttemptID, ExecutionState.FAILED, new Exception("Expected failure cause"));
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

    private static JobGraph singleNonParallelJobVertexJobGraph() {
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

    private DefaultScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
        final SchedulingStrategyFactory schedulingStrategyFactory =
                new PipelinedRegionSchedulingStrategy.Factory();

        try {
            final DefaultScheduler scheduler =
                    createScheduler(
                            jobGraph,
                            ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                            schedulingStrategyFactory);
            scheduler.startScheduling();
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
        return createScheduler(
                jobGraph,
                mainThreadExecutor,
                schedulingStrategyFactory,
                new RestartPipelinedRegionFailoverStrategy.Factory());
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory)
            throws Exception {
        return createScheduler(
                jobGraph,
                mainThreadExecutor,
                schedulingStrategyFactory,
                failoverStrategyFactory,
                taskRestartExecutor);
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final ScheduledExecutor delayExecutor)
            throws Exception {
        return SchedulerTestingUtils.newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setLogger(log)
                .setIoExecutor(executor)
                .setJobMasterConfiguration(configuration)
                .setFutureExecutor(scheduledExecutorService)
                .setDelayExecutor(delayExecutor)
                .setSchedulingStrategyFactory(schedulingStrategyFactory)
                .setFailoverStrategyFactory(failoverStrategyFactory)
                .setRestartBackoffTimeStrategy(testRestartBackoffTimeStrategy)
                .setExecutionVertexOperations(testExecutionVertexOperations)
                .setExecutionVertexVersioner(executionVertexVersioner)
                .setExecutionSlotAllocatorFactory(executionSlotAllocatorFactory)
                .build();
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
        public void scheduleCollectedScheduledTasks() {
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
