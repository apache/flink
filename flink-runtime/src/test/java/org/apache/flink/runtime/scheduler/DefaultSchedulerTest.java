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

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.hooks.TestMasterHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestSchedulingStrategy;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultScheduler}.
 */
public class DefaultSchedulerTest extends TestLogger {

	private static final int TIMEOUT_MS = 1000;

	private static final JobID TEST_JOB_ID = new JobID();

	private ManuallyTriggeredScheduledExecutor taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();

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

		testExecutionVertexOperations = new TestExecutionVertexOperationsDecorator(new DefaultExecutionVertexOperations());

		executionVertexVersioner = new ExecutionVertexVersioner();

		executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory();
		testExecutionSlotAllocator = executionSlotAllocatorFactory.getTestExecutionSlotAllocator();
	}

	@After
	public void tearDown() throws Exception {
		if (scheduledExecutorService != null) {
			ExecutorUtils.gracefulShutdown(TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduledExecutorService);
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

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId));
	}

	@Test
	public void scheduledVertexOrderFromSchedulingStrategyIsRespected() throws Exception {
		final JobGraph jobGraph = singleJobVertexJobGraph(10);
		final JobVertexID onlyJobVertexId = getOnlyJobVertex(jobGraph).getID();

		final List<ExecutionVertexID> desiredScheduleOrder = Arrays.asList(
			new ExecutionVertexID(onlyJobVertexId, 4),
			new ExecutionVertexID(onlyJobVertexId, 0),
			new ExecutionVertexID(onlyJobVertexId, 3),
			new ExecutionVertexID(onlyJobVertexId, 1),
			new ExecutionVertexID(onlyJobVertexId, 2));

		final TestSchedulingStrategy.Factory schedulingStrategyFactory = new TestSchedulingStrategy.Factory();
		createScheduler(jobGraph, schedulingStrategyFactory);
		final TestSchedulingStrategy schedulingStrategy = schedulingStrategyFactory.getLastCreatedSchedulingStrategy();

		schedulingStrategy.schedule(desiredScheduleOrder);

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();

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

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
	}

	@Test
	public void scheduleWithLazyStrategy() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

		createSchedulerAndStartScheduling(jobGraph);

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId));
	}

	@Test
	public void restartFailedTask() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex archivedExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = archivedExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		taskRestartExecutor.triggerScheduledTasks();

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
	}

	@Test
	public void updateTaskExecutionStateReturnsFalseIfExecutionDoesNotExist() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final TaskExecutionState taskExecutionState = new TaskExecutionState(
			jobGraph.getJobID(),
			new ExecutionAttemptID(),
			ExecutionState.FAILED);

		assertFalse(scheduler.updateTaskExecutionState(taskExecutionState));
	}

	@Test
	public void failJobIfCannotRestart() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		testRestartBackoffTimeStrategy.setCanRestart(false);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

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

		Throwable failureCause = scheduler.requestJob()
			.getFailureInfo()
			.getException()
			.deserializeError(DefaultSchedulerTest.class.getClassLoader());
		assertTrue(findThrowable(failureCause, NoResourceAvailableException.class).isPresent());
		assertTrue(
			findThrowableWithMessage(
				failureCause,
				"Could not allocate the required slot within slot request timeout.").isPresent());
		assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
	}

	@Test
	public void skipDeploymentIfVertexVersionOutdated() {
		testExecutionSlotAllocator.disableAutoCompletePendingRequests();

		final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
		final List<JobVertex> sortedJobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		final ExecutionVertexID sourceExecutionVertexId = new ExecutionVertexID(sortedJobVertices.get(0).getID(), 0);
		final ExecutionVertexID sinkExecutionVertexId = new ExecutionVertexID(sortedJobVertices.get(1).getID(), 0);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
		testExecutionSlotAllocator.completePendingRequest(sourceExecutionVertexId);

		final ArchivedExecutionVertex sourceExecutionVertex = scheduler.requestJob().getAllExecutionVertices().iterator().next();
		final ExecutionAttemptID attemptId = sourceExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
		testRestartBackoffTimeStrategy.setCanRestart(false);

		testExecutionSlotAllocator.enableAutoCompletePendingRequests();
		taskRestartExecutor.triggerScheduledTasks();

		assertThat(testExecutionVertexOperations.getDeployedVertices(), containsInAnyOrder(sourceExecutionVertexId, sinkExecutionVertexId));
		assertThat(scheduler.requestJob().getState(), is(equalTo(JobStatus.RUNNING)));
	}

	@Test
	public void releaseSlotIfVertexVersionOutdated() {
		testExecutionSlotAllocator.disableAutoCompletePendingRequests();

		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final ExecutionVertexID onlyExecutionVertexId = new ExecutionVertexID(getOnlyJobVertex(jobGraph).getID(), 0);

		createSchedulerAndStartScheduling(jobGraph);

		executionVertexVersioner.recordModification(onlyExecutionVertexId);
		testExecutionSlotAllocator.completePendingRequests();

		assertThat(testExecutionSlotAllocator.getReturnedSlots(), hasSize(1));
	}

	@Test
	public void vertexIsResetBeforeRestarted() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

		final TestSchedulingStrategy.Factory schedulingStrategyFactory = new TestSchedulingStrategy.Factory();
		final DefaultScheduler scheduler = createScheduler(jobGraph, schedulingStrategyFactory);
		final TestSchedulingStrategy schedulingStrategy = schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
		final SchedulingTopology<?, ?> topology = schedulingStrategy.getSchedulingTopology();

		startScheduling(scheduler);

		final SchedulingExecutionVertex<?, ?> onlySchedulingVertex = Iterables.getOnlyElement(topology.getVertices());
		schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertex.getId()));

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

		taskRestartExecutor.triggerScheduledTasks();

		assertThat(schedulingStrategy.getReceivedVerticesToRestart(), hasSize(1));
		assertThat(onlySchedulingVertex.getState(), is(equalTo(ExecutionState.CREATED)));
	}

	@Test
	public void scheduleOnlyIfVertexIsCreated() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

		final TestSchedulingStrategy.Factory schedulingStrategyFactory = new TestSchedulingStrategy.Factory();
		final DefaultScheduler scheduler = createScheduler(jobGraph, schedulingStrategyFactory);
		final TestSchedulingStrategy schedulingStrategy = schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
		final SchedulingTopology<?, ?> topology = schedulingStrategy.getSchedulingTopology();

		startScheduling(scheduler);

		final ExecutionVertexID onlySchedulingVertexId = Iterables.getOnlyElement(topology.getVertices()).getId();

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

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.CANCELED));

		taskRestartExecutor.triggerScheduledTasks();

		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
	}

	@Test
	public void vertexIsNotAffectedByOutdatedDeployment() {
		final JobGraph jobGraph = singleJobVertexJobGraph(2);

		testExecutionSlotAllocator.disableAutoCompletePendingRequests();
		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final Iterator<ArchivedExecutionVertex> vertexIterator = scheduler.requestJob().getAllExecutionVertices().iterator();
		final ArchivedExecutionVertex v1 = vertexIterator.next();
		final ArchivedExecutionVertex v2 = vertexIterator.next();

		final SchedulingExecutionVertex sv1 = scheduler.getSchedulingTopology().getVertices().iterator().next();

		// fail v1 and let it recover to SCHEDULED
		// the initial deployment of v1 will be outdated
		scheduler.updateTaskExecutionState(new TaskExecutionState(
			jobGraph.getJobID(),
			v1.getCurrentExecutionAttempt().getAttemptId(),
			ExecutionState.FAILED));
		taskRestartExecutor.triggerScheduledTasks();

		// fail v2 to get all pending slot requests in the initial deployments to be done
		// this triggers the outdated deployment of v1
		scheduler.updateTaskExecutionState(new TaskExecutionState(
			jobGraph.getJobID(),
			v2.getCurrentExecutionAttempt().getAttemptId(),
			ExecutionState.FAILED));

		// v1 should not be affected
		assertThat(sv1.getState(), is(equalTo(ExecutionState.SCHEDULED)));
	}

	@Test
	public void abortPendingCheckpointsWhenRestartingTasks() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		enableCheckpointing(jobGraph);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(),  false);
		assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(1)));

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
		taskRestartExecutor.triggerScheduledTasks();
		assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(0)));
	}

	@Test
	public void restoreStateWhenRestartingTasks() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		enableCheckpointing(jobGraph);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

		// register a stateful master hook to help verify state restore
		final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
		checkpointCoordinator.addMasterHook(masterHook);

		// complete one checkpoint for state restore
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(),  false);
		final long checkpointId = checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
		acknowledgePendingCheckpoint(scheduler, checkpointId);

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
		taskRestartExecutor.triggerScheduledTasks();
		assertThat(masterHook.getRestoreCount(), is(equalTo(1)));
	}

	@Test
	public void failGlobalWhenRestoringStateFails() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
		enableCheckpointing(jobGraph);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

		final ArchivedExecutionVertex onlyExecutionVertex = Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
		final ExecutionAttemptID attemptId = onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

		// register a master hook to fail state restore
		final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
		masterHook.enableFailOnRestore();
		checkpointCoordinator.addMasterHook(masterHook);

		// complete one checkpoint for state restore
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(),  false);
		final long checkpointId = checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
		acknowledgePendingCheckpoint(scheduler, checkpointId);

		scheduler.updateTaskExecutionState(new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
		taskRestartExecutor.triggerScheduledTasks();
		final List<ExecutionVertexID> deployedExecutionVertices = testExecutionVertexOperations.getDeployedVertices();

		// the first task failover should be skipped on state restore failure
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
		assertThat(deployedExecutionVertices, contains(executionVertexId));

		// a global failure should be triggered on state restore failure
		masterHook.disableFailOnRestore();
		taskRestartExecutor.triggerScheduledTasks();
		assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
	}

	@Test
	public void testInputConstraintALLPerf() throws Exception {
		final int parallelism = 1000;
		final JobVertex v1 = createVertexWithAllInputConstraints("vertex1", parallelism);
		final JobVertex v2 = createVertexWithAllInputConstraints("vertex2", parallelism);
		final JobVertex v3 = createVertexWithAllInputConstraints("vertex3", parallelism);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v2.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(v1, v2, v3);
		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
		final AccessExecutionJobVertex ejv1 = scheduler.requestJob().getAllVertices().get(v1.getID());

		for (int i = 0; i < parallelism - 1; i++) {
			finishSubtask(scheduler, ejv1, i);
		}

		final long startTime = System.nanoTime();
		finishSubtask(scheduler, ejv1, parallelism - 1);

		final Duration duration = Duration.ofNanos(System.nanoTime() - startTime);
		final Duration timeout = Duration.ofSeconds(5);

		assertThat(duration, lessThan(timeout));
	}

	@Test
	public void failJobWillIncrementVertexVersions() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
		final ExecutionVertexID onlyExecutionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
		final ExecutionVertexVersion executionVertexVersion = executionVertexVersioner.getExecutionVertexVersion(
			onlyExecutionVertexId);

		scheduler.failJob(new FlinkException("Test failure."));

		assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
	}

	@Test
	public void cancelJobWillIncrementVertexVersions() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
		final ExecutionVertexID onlyExecutionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
		final ExecutionVertexVersion executionVertexVersion = executionVertexVersioner.getExecutionVertexVersion(
			onlyExecutionVertexId);

		scheduler.cancel();

		assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
	}

	@Test
	public void suspendJobWillIncrementVertexVersions() {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
		final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
		final ExecutionVertexID onlyExecutionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);

		final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
		final ExecutionVertexVersion executionVertexVersion = executionVertexVersioner.getExecutionVertexVersion(
			onlyExecutionVertexId);

		scheduler.suspend(new Exception("forced suspend"));

		assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
	}

	private static JobVertex createVertexWithAllInputConstraints(String name, int parallelism) {
		final JobVertex v = new JobVertex(name);
		v.setParallelism(parallelism);
		v.setInvokableClass(AbstractInvokable.class);
		v.setInputDependencyConstraint(InputDependencyConstraint.ALL);
		return v;
	}

	private static void finishSubtask(DefaultScheduler scheduler, AccessExecutionJobVertex vertex, int subtask) {
		final ExecutionAttemptID attemptId = vertex.getTaskVertices()[subtask].getCurrentExecutionAttempt().getAttemptId();
		scheduler.updateTaskExecutionState(
			new TaskExecutionState(scheduler.getJobGraph().getJobID(), attemptId, ExecutionState.FINISHED));
	}

	private void acknowledgePendingCheckpoint(final SchedulerBase scheduler, final long checkpointId) throws Exception {
		final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

		for (ArchivedExecutionVertex executionVertex : scheduler.requestJob().getAllExecutionVertices()) {
			final ExecutionAttemptID attemptId = executionVertex.getCurrentExecutionAttempt().getAttemptId();
			final AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
				scheduler.getJobGraph().getJobID(),
				attemptId,
				checkpointId);
			checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "Unknown location");
		}
	}

	private void enableCheckpointing(final JobGraph jobGraph) {
		final List<JobVertexID> triggerVertices = new ArrayList<>();
		final List<JobVertexID> ackVertices = new ArrayList<>();
		final List<JobVertexID> commitVertices = new ArrayList<>();

		for (JobVertex vertex : jobGraph.getVertices()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		jobGraph.setSnapshotSettings(
			new JobCheckpointingSettings(
				triggerVertices,
				ackVertices,
				commitVertices,
				new CheckpointCoordinatorConfiguration(
					Long.MAX_VALUE, // disable periodical checkpointing
					10 * 60 * 1000,
					0,
					1,
					CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					false,
					false,
					0),
				null));
	}

	private CheckpointCoordinator getCheckpointCoordinator(final SchedulerBase scheduler) {
		// TODO: get CheckpointCoordinator from the scheduler directly after it is factored out from ExecutionGraph
		return scheduler.getExecutionGraph().getCheckpointCoordinator();
	}

	private void waitForTermination(final DefaultScheduler scheduler) throws Exception {
		scheduler.getTerminationFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
	}

	private static JobGraph singleNonParallelJobVertexJobGraph() {
		return singleJobVertexJobGraph(1);
	}

	private static JobGraph singleJobVertexJobGraph(final int parallelism) {
		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(parallelism);
		jobGraph.addVertex(vertex);
		return jobGraph;
	}

	private static JobGraph nonParallelSourceSinkJobGraph() {
		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		jobGraph.addVertex(source);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		jobGraph.addVertex(sink);

		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		return jobGraph;
	}

	private static JobVertex getOnlyJobVertex(final JobGraph jobGraph) {
		final List<JobVertex> sortedVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		Preconditions.checkState(sortedVertices.size() == 1);
		return sortedVertices.get(0);
	}

	private DefaultScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
		final SchedulingStrategyFactory schedulingStrategyFactory =
			jobGraph.getScheduleMode() == ScheduleMode.LAZY_FROM_SOURCES ?
				new LazyFromSourcesSchedulingStrategy.Factory() :
				new EagerSchedulingStrategy.Factory();

		try {
			final DefaultScheduler scheduler = createScheduler(jobGraph, schedulingStrategyFactory);
			startScheduling(scheduler);
			return scheduler;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private DefaultScheduler createScheduler(
			final JobGraph jobGraph,
			final SchedulingStrategyFactory schedulingStrategyFactory) throws Exception {

		return new DefaultScheduler(
			log,
			jobGraph,
			VoidBackPressureStatsTracker.INSTANCE,
			executor,
			configuration,
			new SimpleSlotProvider(TEST_JOB_ID, 0),
			scheduledExecutorService,
			taskRestartExecutor,
			ClassLoader.getSystemClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			Time.seconds(300),
			VoidBlobWriter.getInstance(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup(),
			Time.seconds(300),
			NettyShuffleMaster.INSTANCE,
			NoOpJobMasterPartitionTracker.INSTANCE,
			schedulingStrategyFactory,
			new RestartPipelinedRegionStrategy.Factory(),
			testRestartBackoffTimeStrategy,
			testExecutionVertexOperations,
			executionVertexVersioner,
			executionSlotAllocatorFactory);
	}

	private void startScheduling(final SchedulerNG scheduler) {
		scheduler.setMainThreadExecutor(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		scheduler.startScheduling();
	}
}
