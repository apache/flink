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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.NoOpPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
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
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, FailoverStrategyLoader.NO_OP_FAILOVER_STRATEGY);

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
	public void vertexIsResetBeforeRestarted() throws Exception {
		final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

		final TestSchedulingStrategy.Factory schedulingStrategyFactory = new TestSchedulingStrategy.Factory();
		final DefaultScheduler scheduler = createScheduler(jobGraph, schedulingStrategyFactory);
		final TestSchedulingStrategy schedulingStrategy = schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
		final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

		startScheduling(scheduler);

		final SchedulingExecutionVertex onlySchedulingVertex = Iterables.getOnlyElement(topology.getVertices());
		schedulingStrategy.schedule(Collections.singleton(onlySchedulingVertex.getId()));

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
		final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

		startScheduling(scheduler);

		final ExecutionVertexID onlySchedulingVertexId = Iterables.getOnlyElement(topology.getVertices()).getId();

		// Schedule the vertex to get it to a non-CREATED state
		schedulingStrategy.schedule(Collections.singleton(onlySchedulingVertexId));

		// The scheduling of a non-CREATED vertex will result in IllegalStateException
		try {
			schedulingStrategy.schedule(Collections.singleton(onlySchedulingVertexId));
			fail("IllegalStateException should happen");
		} catch (IllegalStateException e) {
			// expected exception
		}
	}

	private void waitForTermination(final DefaultScheduler scheduler) throws Exception {
		scheduler.getTerminationFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
	}

	private static JobGraph singleNonParallelJobVertexJobGraph() {
		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		final JobVertex vertex = new JobVertex("source");
		vertex.setInvokableClass(NoOpInvokable.class);
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
			NoOpPartitionTracker.INSTANCE,
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
