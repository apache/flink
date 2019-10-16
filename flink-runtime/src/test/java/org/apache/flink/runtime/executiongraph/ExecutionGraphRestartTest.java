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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.NotCancelAckingTaskGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.completeCancellingForAllVertices;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.finishAllVertices;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.switchToRunning;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests the restart behaviour of the {@link ExecutionGraph}.
 */
public class ExecutionGraphRestartTest extends TestLogger {

	private static final int NUM_TASKS = 31;

	private static final ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	private static final JobID TEST_JOB_ID = new JobID();

	// ------------------------------------------------------------------------

	@Test
	public void testNoManualRestart() throws Exception {
		NoRestartStrategy restartStrategy = new NoRestartStrategy();
		ExecutionGraph eg = createSimpleExecutionGraph(
			restartStrategy, new SimpleSlotProvider(TEST_JOB_ID, NUM_TASKS), createJobGraph());

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));

		completeCanceling(eg);

		assertEquals(JobStatus.FAILED, eg.getState());

		// This should not restart the graph.
		eg.restart(eg.getGlobalModVersion());

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	private void completeCanceling(ExecutionGraph eg) {
		executeOperationForAllExecutions(eg, Execution::completeCancelling);
	}

	private void executeOperationForAllExecutions(ExecutionGraph eg, Consumer<Execution> operation) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			operation.accept(vertex.getCurrentExecutionAttempt());
		}
	}

	@Test
	public void testRestartAutomatically() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			restartAfterFailure(TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(TestRestartStrategy.directExecuting())
				.buildAndScheduleForExecution(slotPool));
		}

	}

	@Nonnull
	private SlotPoolImpl createSlotPoolImpl() {
		return new TestingSlotPoolImpl(TEST_JOB_ID);
	}

	@Test
	public void testCancelWhileRestarting() throws Exception {
		// We want to manually control the restart and delay
		try (SlotPool slotPool = createSlotPoolImpl()) {
			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setTaskManagerLocation(taskManagerLocation)
				.buildAndScheduleForExecution(slotPool);

			// Release the TaskManager and wait for the job to restart
			slotPool.releaseTaskManager(taskManagerLocation.getResourceID(), new Exception("Test Exception"));
			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			// Canceling needs to abort the restart
			executionGraph.cancel();

			assertEquals(JobStatus.CANCELED, executionGraph.getState());

			// The restart has been aborted
			executionGraph.restart(executionGraph.getGlobalModVersion());

			assertEquals(JobStatus.CANCELED, executionGraph.getState());
		}

	}

	@Test
	public void testFailWhileRestarting() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setTaskManagerLocation(taskManagerLocation)
				.buildAndScheduleForExecution(slotPool);

			// Release the TaskManager and wait for the job to restart
			slotPool.releaseTaskManager(taskManagerLocation.getResourceID(), new Exception("Test Exception"));

			assertEquals(JobStatus.RESTARTING, executionGraph.getState());

			// If we fail when being in RESTARTING, then we should try to restart again
			final long globalModVersion = executionGraph.getGlobalModVersion();
			final Exception testException = new Exception("Test exception");
			executionGraph.failGlobal(testException);

			assertNotEquals(globalModVersion, executionGraph.getGlobalModVersion());
			assertEquals(JobStatus.RESTARTING, executionGraph.getState());
			assertEquals(testException, executionGraph.getFailureCause()); // we should have updated the failure cause

			// but it should fail when sending a SuppressRestartsException
			executionGraph.failGlobal(new SuppressRestartsException(new Exception("Suppress restart exception")));

			assertEquals(JobStatus.FAILED, executionGraph.getState());

			// The restart has been aborted
			executionGraph.restart(executionGraph.getGlobalModVersion());

			assertEquals(JobStatus.FAILED, executionGraph.getState());
		}
	}

	@Test
	public void testCancelWhileFailing() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			final ExecutionGraph graph = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.buildAndScheduleForExecution(slotPool);

			assertEquals(JobStatus.RUNNING, graph.getState());

			// switch all tasks to running
			for (ExecutionVertex vertex : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
				vertex.getCurrentExecutionAttempt().switchToRunning();
			}

			graph.failGlobal(new Exception("test"));

			assertEquals(JobStatus.FAILING, graph.getState());

			graph.cancel();

			assertEquals(JobStatus.CANCELLING, graph.getState());

			// let all tasks finish cancelling
			completeCanceling(graph);

			assertEquals(JobStatus.CANCELED, graph.getState());
		}

	}

	@Test
	public void testFailWhileCanceling() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			final ExecutionGraph graph = TestingExecutionGraphBuilder.newBuilder().buildAndScheduleForExecution(slotPool);

			assertEquals(JobStatus.RUNNING, graph.getState());
			switchAllTasksToRunning(graph);

			graph.cancel();

			assertEquals(JobStatus.CANCELLING, graph.getState());

			graph.failGlobal(new Exception("test"));

			assertEquals(JobStatus.FAILING, graph.getState());

			// let all tasks finish cancelling
			completeCanceling(graph);

			assertEquals(JobStatus.FAILED, graph.getState());
		}

	}

	@Test
	public void testTaskFailingWhileGlobalFailing() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			final ExecutionGraph graph = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setFailoverStrategyFactory(new TestFailoverStrategy.Factory())
				.buildAndScheduleForExecution(slotPool);
			final TestFailoverStrategy failoverStrategy = (TestFailoverStrategy) graph.getFailoverStrategy();

			// switch all tasks to running
			for (ExecutionVertex vertex : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
				vertex.getCurrentExecutionAttempt().switchToRunning();
			}

			graph.failGlobal(new Exception("test"));

			graph.getAllExecutionVertices().iterator().next().fail(new Exception("Test task failure"));

			// no local failover should happen when in global failover cancelling
			assertEquals(0, failoverStrategy.getLocalFailoverCount());
		}

	}

	private void switchAllTasksToRunning(ExecutionGraph graph) {
		executeOperationForAllExecutions(graph, Execution::switchToRunning);
	}

	@Test
	public void testNoRestartOnSuppressException() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new FixedDelayRestartStrategy(Integer.MAX_VALUE, 0))
				.buildAndScheduleForExecution(slotPool);

			// Fail with unrecoverable Exception
			eg.getAllExecutionVertices().iterator().next().fail(
				new SuppressRestartsException(new Exception("Test Exception")));

			assertEquals(JobStatus.FAILING, eg.getState());

			completeCanceling(eg);

			eg.waitUntilTerminal();
			assertEquals(JobStatus.FAILED, eg.getState());

			RestartStrategy restartStrategy = eg.getRestartStrategy();
			assertTrue(restartStrategy instanceof FixedDelayRestartStrategy);

			assertEquals(0, ((FixedDelayRestartStrategy) restartStrategy).getCurrentRestartAttempt());
		}

	}

	/**
	 * Tests that a failing execution does not affect a restarted job. This is important if a
	 * callback handler fails an execution after it has already reached a final state and the job
	 * has been restarted.
	 */
	@Test
	public void testFailingExecutionAfterRestart() throws Exception {
		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task1", 1, NoOpInvokable.class);
		JobVertex receiver = ExecutionGraphTestUtils.createJobVertex("Task2", 1, NoOpInvokable.class);
		JobGraph jobGraph = new JobGraph("Pointwise job", sender, receiver);

		try (SlotPool slotPool = createSlotPoolImpl()) {
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(TestRestartStrategy.directExecuting())
				.setJobGraph(jobGraph)
				.setNumberOfTasks(2)
				.buildAndScheduleForExecution(slotPool);

			Iterator<ExecutionVertex> executionVertices = eg.getAllExecutionVertices().iterator();

			Execution finishedExecution = executionVertices.next().getCurrentExecutionAttempt();
			Execution failedExecution = executionVertices.next().getCurrentExecutionAttempt();

			finishedExecution.markFinished();

			failedExecution.fail(new Exception("Test Exception"));
			failedExecution.completeCancelling();

			assertEquals(JobStatus.RUNNING, eg.getState());

			// At this point all resources have been assigned
			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				assertNotNull("No assigned resource (test instability).", vertex.getCurrentAssignedResource());
				vertex.getCurrentExecutionAttempt().switchToRunning();
			}

			// fail old finished execution, this should not affect the execution
			finishedExecution.fail(new Exception("This should have no effect"));

			for (ExecutionVertex vertex: eg.getAllExecutionVertices()) {
				vertex.getCurrentExecutionAttempt().markFinished();
			}

			// the state of the finished execution should have not changed since it is terminal
			assertEquals(ExecutionState.FINISHED, finishedExecution.getState());

			assertEquals(JobStatus.FINISHED, eg.getState());
		}
	}

	/**
	 * Tests that a graph is not restarted after cancellation via a call to
	 * {@link ExecutionGraph#failGlobal(Throwable)}. This can happen when a slot is
	 * released concurrently with cancellation.
	 */
	@Test
	public void testFailExecutionAfterCancel() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setJobGraph(createJobGraphToCancel())
				.setNumberOfTasks(2)
				.buildAndScheduleForExecution(slotPool);

			// Fail right after cancel (for example with concurrent slot release)
			eg.cancel();

			for (ExecutionVertex v : eg.getAllExecutionVertices()) {
				v.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
			}

			assertEquals(JobStatus.CANCELED, eg.getTerminationFuture().get());

			Execution execution = eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

			execution.completeCancelling();
			assertEquals(JobStatus.CANCELED, eg.getState());
		}
	}

	/**
	 * Tests that it is possible to fail a graph via a call to
	 * {@link ExecutionGraph#failGlobal(Throwable)} after cancellation.
	 */
	@Test
	public void testFailExecutionGraphAfterCancel() throws Exception {
		try (SlotPool slotPool = createSlotPoolImpl()) {
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new InfiniteDelayRestartStrategy())
				.setJobGraph(createJobGraphToCancel())
				.setNumberOfTasks(2)
				.buildAndScheduleForExecution(slotPool);

			// Fail right after cancel (for example with concurrent slot release)
			eg.cancel();
			assertEquals(JobStatus.CANCELLING, eg.getState());

			eg.failGlobal(new Exception("Test Exception"));
			assertEquals(JobStatus.FAILING, eg.getState());

			Execution execution = eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

			execution.completeCancelling();
			assertEquals(JobStatus.RESTARTING, eg.getState());
		}
	}

	/**
	 * Tests that a suspend call while restarting a job, will abort the restarting.
	 */
	@Test
	public void testSuspendWhileRestarting() throws Exception {
		TestRestartStrategy controllableRestartStrategy = TestRestartStrategy.manuallyTriggered();
		try (SlotPool slotPool = createSlotPoolImpl()) {
			TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(controllableRestartStrategy)
				.setTaskManagerLocation(taskManagerLocation)
				.buildAndScheduleForExecution(slotPool);

			// Release the TaskManager and wait for the job to restart
			slotPool.releaseTaskManager(taskManagerLocation.getResourceID(), new Exception("Test Exception"));

			assertEquals(1, controllableRestartStrategy.getNumberOfQueuedActions());

			assertEquals(JobStatus.RESTARTING, eg.getState());

			eg.suspend(new Exception("Test exception"));

			assertEquals(JobStatus.SUSPENDED, eg.getState());

			controllableRestartStrategy.triggerAll().join();

			assertEquals(JobStatus.SUSPENDED, eg.getState());
		}
	}

	@Test
	public void testLocalFailAndRestart() throws Exception {
		final int parallelism = 10;
		final TestRestartStrategy triggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();

		final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(TEST_JOB_ID, createNoOpVertex(parallelism))
			.setScheduleMode(ScheduleMode.EAGER)
			.setRestartStrategy(triggeredRestartStrategy)
			.build();

		eg.start(mainThreadExecutor);
		eg.scheduleForExecution();

		switchToRunning(eg);

		final ExecutionJobVertex vertex = eg.getVerticesTopologically().iterator().next();
		final Execution first = vertex.getTaskVertices()[0].getCurrentExecutionAttempt();
		final Execution last = vertex.getTaskVertices()[vertex.getParallelism() - 1].getCurrentExecutionAttempt();

		// Have two executions fail
		first.fail(new Exception("intended test failure 1"));
		last.fail(new Exception("intended test failure 2"));

		assertEquals(JobStatus.FAILING, eg.getState());

		completeCancellingForAllVertices(eg);

		// Now trigger the restart
		assertEquals(1, triggeredRestartStrategy.getNumberOfQueuedActions());
		triggeredRestartStrategy.triggerAll().join();

		assertEquals(JobStatus.RUNNING, eg.getState());

		switchToRunning(eg);
		finishAllVertices(eg);

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	@Test
	public void testGlobalFailAndRestarts() throws Exception {
		final int parallelism = 10;
		final JobVertex vertex = createNoOpVertex(parallelism);
		final NotCancelAckingTaskGateway taskManagerGateway = new NotCancelAckingTaskGateway();
		final SlotProvider slots = new SimpleSlotProvider(TEST_JOB_ID, parallelism, taskManagerGateway);
		final TestRestartStrategy restartStrategy = TestRestartStrategy.manuallyTriggered();

		final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(TEST_JOB_ID, vertex)
			.setSlotProvider(slots)
			.setRestartStrategy(restartStrategy)
			.setScheduleMode(ScheduleMode.EAGER)
			.build();

		eg.start(mainThreadExecutor);

		eg.scheduleForExecution();

		switchToRunning(eg);

		// fail into 'RESTARTING'
		eg.failGlobal(new Exception("intended test failure 1"));
		assertEquals(JobStatus.FAILING, eg.getState());

		completeCancellingForAllVertices(eg);

		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.failGlobal(new Exception("intended test failure 2"));
		assertEquals(JobStatus.RESTARTING, eg.getState());

		restartStrategy.triggerAll().join();

		assertEquals(JobStatus.RUNNING, eg.getState());

		switchToRunning(eg);
		finishAllVertices(eg);

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FINISHED, eg.getState());

		assertThat("Too many restarts", eg.getNumberOfRestarts(), is(lessThanOrEqualTo(2L)));
	}

	/**
	 * SlotPool#failAllocation should not fail with a {@link java.util.ConcurrentModificationException}
	 * if there is a concurrent scheduling operation. See FLINK-13421.
	 */
	@Test
	public void slotPoolExecutionGraph_ConcurrentSchedulingAndAllocationFailure_ShouldNotFailWithConcurrentModificationException() throws Exception {
		final SlotSharingGroup group = new SlotSharingGroup();
		final JobVertex vertex1 = createNoOpVertex("vertex1", 1);
		vertex1.setSlotSharingGroup(group);
		final JobVertex vertex2 = createNoOpVertex("vertex2", 3);
		vertex2.setSlotSharingGroup(group);
		final JobVertex vertex3 = createNoOpVertex("vertex3", 1);
		vertex3.setSlotSharingGroup(group);
		vertex3.connectNewDataSetAsInput(vertex2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		try (SlotPool slotPool = createSlotPoolImpl()) {
			final SlotProvider slots = createSchedulerWithSlots(2, slotPool, new LocalTaskManagerLocation());

			final AllocationID allocationId = slotPool.getAvailableSlotsInformation().iterator().next().getAllocationId();

			final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(TEST_JOB_ID, vertex1, vertex2, vertex3)
				.setSlotProvider(slots)
				.setAllocationTimeout(Time.minutes(60))
				.setScheduleMode(ScheduleMode.EAGER)
				.setAllowQueuedScheduling(true)
				.build();

			eg.start(mainThreadExecutor);

			eg.scheduleForExecution();

			slotPool.failAllocation(
				allocationId,
				new Exception("test exception"));

			eg.waitUntilTerminal();
		}
	}

	@Test
	public void testRestartWithEagerSchedulingAndSlotSharing() throws Exception {
		final int parallelism = 20;

		try (SlotPool slotPool = createSlotPoolImpl()) {
			final Scheduler scheduler = createSchedulerWithSlots(parallelism, slotPool, new LocalTaskManagerLocation());

			final SlotSharingGroup sharingGroup = new SlotSharingGroup();

			final JobVertex source = new JobVertex("source");
			source.setInvokableClass(NoOpInvokable.class);
			source.setParallelism(parallelism);
			source.setSlotSharingGroup(sharingGroup);

			final JobVertex sink = new JobVertex("sink");
			sink.setInvokableClass(NoOpInvokable.class);
			sink.setParallelism(parallelism);
			sink.setSlotSharingGroup(sharingGroup);
			sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);

			TestRestartStrategy restartStrategy = TestRestartStrategy.directExecuting();

			final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(TEST_JOB_ID, source, sink)
				.setSlotProvider(scheduler)
				.setRestartStrategy(restartStrategy)
				.setScheduleMode(ScheduleMode.EAGER)
				.build();

			eg.start(mainThreadExecutor);

			eg.scheduleForExecution();

			switchToRunning(eg);

			// fail into 'RESTARTING'
			eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt().fail(
				new Exception("intended test failure"));

			assertEquals(JobStatus.FAILING, eg.getState());

			completeCancellingForAllVertices(eg);

			assertEquals(JobStatus.RUNNING, eg.getState());

			// clean termination
			switchToRunning(eg);
			finishAllVertices(eg);

			assertEquals(JobStatus.FINISHED, eg.getState());
		}
	}

	@Test
	public void testRestartWithSlotSharingAndNotEnoughResources() throws Exception {
		final int numRestarts = 10;
		final int parallelism = 20;

		try (SlotPool slotPool = createSlotPoolImpl()) {
			final Scheduler scheduler = createSchedulerWithSlots(
				parallelism - 1, slotPool, new LocalTaskManagerLocation());

			final SlotSharingGroup sharingGroup = new SlotSharingGroup();

			final JobVertex source = new JobVertex("source");
			source.setInvokableClass(NoOpInvokable.class);
			source.setParallelism(parallelism);
			source.setSlotSharingGroup(sharingGroup);

			final JobVertex sink = new JobVertex("sink");
			sink.setInvokableClass(NoOpInvokable.class);
			sink.setParallelism(parallelism);
			sink.setSlotSharingGroup(sharingGroup);
			sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);

			TestRestartStrategy restartStrategy =
				new TestRestartStrategy(numRestarts, false);

			final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(TEST_JOB_ID, source, sink)
				.setSlotProvider(scheduler)
				.setRestartStrategy(restartStrategy)
				.setScheduleMode(ScheduleMode.EAGER)
				.build();

			eg.start(mainThreadExecutor);
			eg.scheduleForExecution();

			// the last suppressed restart is also counted
			assertEquals(numRestarts + 1, eg.getNumberOfRestarts());

			assertEquals(JobStatus.FAILED, eg.getState());

			final Throwable t = eg.getFailureCause();
			if (!(t instanceof NoResourceAvailableException)) {
				ExceptionUtils.rethrowException(t, t.getMessage());
			}
		}
	}

	/**
	 * Tests that the {@link ExecutionGraph} can handle failures while
	 * being in the RESTARTING state.
	 */
	@Test
	public void testFailureWhileRestarting() throws Exception {

		final TestRestartStrategy restartStrategy = TestRestartStrategy.manuallyTriggered();
		final ExecutionGraph executionGraph = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(createJobGraph())
			.setRestartStrategy(restartStrategy)
			.setSlotProvider(new TestingSlotProvider(ignored -> new CompletableFuture<>()))
			.allowQueuedScheduling()
			.build();

		executionGraph.start(mainThreadExecutor);
		executionGraph.scheduleForExecution();

		assertThat(executionGraph.getState(), is(JobStatus.RUNNING));

		executionGraph.failGlobal(new FlinkException("Test exception"));

		restartStrategy.triggerAll().join();

		executionGraph.failGlobal(new FlinkException("Concurrent exception"));

		restartStrategy.triggerAll().join();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());
	}

	@Test
	public void failGlobalIfExecutionIsStillRunning_failingAnExecutionTwice_ShouldTriggerOnlyOneFailover() throws Exception {
		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task1", 1, NoOpInvokable.class);
		JobVertex receiver = ExecutionGraphTestUtils.createJobVertex("Task2", 1, NoOpInvokable.class);
		JobGraph jobGraph = new JobGraph("Pointwise job", sender, receiver);

		try (SlotPool slotPool = createSlotPoolImpl()) {
			ExecutionGraph eg = TestingExecutionGraphBuilder.newBuilder()
				.setRestartStrategy(new TestRestartStrategy(1, false))
				.setJobGraph(jobGraph)
				.setNumberOfTasks(2)
				.buildAndScheduleForExecution(slotPool);

			Iterator<ExecutionVertex> executionVertices = eg.getAllExecutionVertices().iterator();

			Execution finishedExecution = executionVertices.next().getCurrentExecutionAttempt();
			Execution failedExecution = executionVertices.next().getCurrentExecutionAttempt();

			finishedExecution.markFinished();

			failedExecution.fail(new Exception("Test Exception"));
			failedExecution.completeCancelling();

			assertEquals(JobStatus.RUNNING, eg.getState());

			// At this point all resources have been assigned
			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				assertNotNull("No assigned resource (test instability).", vertex.getCurrentAssignedResource());
				vertex.getCurrentExecutionAttempt().switchToRunning();
			}

			// fail global with old finished execution, this should not affect the execution
			eg.failGlobalIfExecutionIsStillRunning(new Exception("This should have no effect"), finishedExecution.getAttemptId());

			assertThat(eg.getState(), is(JobStatus.RUNNING));

			// the state of the finished execution should have not changed since it is terminal
			assertThat(finishedExecution.getState(), is(ExecutionState.FINISHED));
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class TestingExecutionGraphBuilder {
		private RestartStrategy restartStrategy = new NoRestartStrategy();
		private FailoverStrategy.Factory failoverStrategyFactory = new RestartAllStrategy.Factory();
		private JobGraph jobGraph = createJobGraph();
		private int tasksNum = NUM_TASKS;
		private TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		private TestingExecutionGraphBuilder setRestartStrategy(RestartStrategy restartStrategy) {
			this.restartStrategy = restartStrategy;
			return this;
		}

		private TestingExecutionGraphBuilder setFailoverStrategyFactory(FailoverStrategy.Factory failoverStrategyFactory) {
			this.failoverStrategyFactory = failoverStrategyFactory;
			return this;
		}

		private TestingExecutionGraphBuilder setJobGraph(JobGraph jobGraph) {
			this.jobGraph = jobGraph;
			return this;
		}

		TestingExecutionGraphBuilder setNumberOfTasks(@SuppressWarnings("SameParameterValue") int tasksNum) {
			this.tasksNum = tasksNum;
			return this;
		}

		private TestingExecutionGraphBuilder setTaskManagerLocation(TaskManagerLocation taskManagerLocation) {
			this.taskManagerLocation = taskManagerLocation;
			return this;
		}

		private static TestingExecutionGraphBuilder newBuilder() {
			return new TestingExecutionGraphBuilder();
		}

		private ExecutionGraph buildAndScheduleForExecution(SlotPool slotPool) throws Exception {
			final Scheduler scheduler = createSchedulerWithSlots(tasksNum, slotPool, taskManagerLocation);
			final ExecutionGraph eg = createSimpleExecutionGraph(
				restartStrategy,
				failoverStrategyFactory,
				scheduler,
				jobGraph);

			assertEquals(JobStatus.CREATED, eg.getState());

			eg.scheduleForExecution();
			assertEquals(JobStatus.RUNNING, eg.getState());

			return eg;
		}
	}

	private static Scheduler createSchedulerWithSlots(
		int numSlots, SlotPool slotPool, TaskManagerLocation taskManagerLocation) throws Exception {

		final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		setupSlotPool(slotPool);
		Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);
		slotPool.registerTaskManager(taskManagerLocation.getResourceID());

		final List<SlotOffer> slotOffers = new ArrayList<>(NUM_TASKS);
		for (int i = 0; i < numSlots; i++) {
			final AllocationID allocationId = new AllocationID();
			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.ANY);
			slotOffers.add(slotOffer);
		}

		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		return scheduler;
	}

	private static void setupSlotPool(SlotPool slotPool) throws Exception {
		final String jobManagerAddress = "foobar";
		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);
	}

	private static JobGraph createJobGraph() {
		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task", NUM_TASKS, NoOpInvokable.class);
		return new JobGraph("Pointwise job", sender);
	}

	private static JobGraph createJobGraphToCancel() throws IOException {
		JobVertex vertex = ExecutionGraphTestUtils.createJobVertex("Test Vertex", 1, NoOpInvokable.class);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE, Integer.MAX_VALUE));
		JobGraph jobGraph = new JobGraph("Test Job", vertex);
		jobGraph.setExecutionConfig(executionConfig);
		return jobGraph;
	}

	private static ExecutionGraph createSimpleExecutionGraph(
		final RestartStrategy restartStrategy,
		final SlotProvider slotProvider,
		final JobGraph jobGraph) throws IOException, JobException {

		return createSimpleExecutionGraph(restartStrategy, new RestartAllStrategy.Factory(), slotProvider, jobGraph);
	}

	private static ExecutionGraph createSimpleExecutionGraph(
		final RestartStrategy restartStrategy,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final SlotProvider slotProvider,
		final JobGraph jobGraph) throws IOException, JobException {

		final ExecutionGraph executionGraph = new ExecutionGraph(
			new JobInformation(
				TEST_JOB_ID,
				"Test job",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.emptyList(),
				Collections.emptyList()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			failoverStrategyFactory,
			slotProvider);

		executionGraph.start(mainThreadExecutor);
		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		return executionGraph;
	}

	private void restartAfterFailure(ExecutionGraph eg) {
		eg.start(mainThreadExecutor);
		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));

		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().completeCancelling();
		}

		assertEquals(JobStatus.RUNNING, eg.getState());

		finishAllVertices(eg);
		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	/**
	 * Test failover strategy which records local failover count.
	 */
	static class TestFailoverStrategy extends FailoverStrategy {

		private int localFailoverCount = 0;

		@Override
		public void onTaskFailure(Execution taskExecution, Throwable cause) {
			localFailoverCount++;
		}

		@Override
		public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		}

		@Override
		public String getStrategyName() {
			return "Test Failover Strategy";
		}

		int getLocalFailoverCount() {
			return localFailoverCount;
		}

		// ------------------------------------------------------------------------
		//  factory
		// ------------------------------------------------------------------------

		/**
		 * Factory that instantiates the TestFailoverStrategy.
		 */
		public static class Factory implements FailoverStrategy.Factory {

			@Override
			public FailoverStrategy create(ExecutionGraph executionGraph) {
				return new TestFailoverStrategy();
			}
		}
	}
}
