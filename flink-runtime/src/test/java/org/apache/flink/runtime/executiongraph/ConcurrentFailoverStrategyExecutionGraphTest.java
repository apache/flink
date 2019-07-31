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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.failover.RestartIndividualStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests make sure that global failover (restart all) always takes precedence over
 * local recovery strategies.
 *
 * <p>This test must be in the package it resides in, because it uses package-private methods
 * from the ExecutionGraph classes.
 */
public class ConcurrentFailoverStrategyExecutionGraphTest extends TestLogger {

	private final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	/**
	 * Tests that a cancellation concurrent to a local failover leads to a properly
	 * cancelled state.
	 */
	@Test
	public void testCancelWhileInLocalFailover() throws Exception {

		// the logic in this test is as follows:
		//  - start a job
		//  - cause a task failure and delay the local recovery action via the manual executor
		//  - cancel the job to go into cancelling
		//  - resume in local recovery action
		//  - validate that this does in fact not start a new task, because the graph as a
		//    whole should now be cancelled already

		final JobID jid = new JobID();
		final int parallelism = 2;

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
			jid,
			TestRestartPipelinedRegionStrategy::new,
			TestRestartStrategy.directExecuting(),
			slotProvider,
			parallelism);

		graph.start(mainThreadExecutor);
		TestRestartPipelinedRegionStrategy strategy = (TestRestartPipelinedRegionStrategy) graph.getFailoverStrategy();

		// This future is used to block the failover strategy execution until we complete it
		final CompletableFuture<?> blocker = new CompletableFuture<>();
		strategy.setBlockerFuture(blocker);

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];
		final ExecutionVertex vertex2 = ejv.getTaskVertices()[1];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		// let one of the vertices fail - that triggers a local recovery action
		vertex1.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());

		// graph should still be running and the failover recovery action should be queued
		assertEquals(JobStatus.RUNNING, graph.getState());

		// now cancel the job
		graph.cancel();

		assertEquals(JobStatus.CANCELLING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue
		blocker.complete(null);

		// now report that cancelling is complete for the other vertex
		vertex2.getCurrentExecutionAttempt().completeCancelling();

		assertEquals(JobStatus.CANCELED, graph.getTerminationFuture().get());
		assertTrue(vertex1.getCurrentExecutionAttempt().getState().isTerminal());
		assertTrue(vertex2.getCurrentExecutionAttempt().getState().isTerminal());

		// make sure all slots are recycled
		assertEquals(parallelism, slotProvider.getNumberOfAvailableSlots());
	}

	/**
	 * Tests that a terminal global failure concurrent to a local failover
	 * leads to a properly failed state.
	 */
	@Test
	public void testGlobalFailureConcurrentToLocalFailover() throws Exception {

		// the logic in this test is as follows:
		//  - start a job
		//  - cause a task failure and delay the local recovery action via the manual executor
		//  - cause a global failure
		//  - resume in local recovery action
		//  - validate that this does in fact not start a new task, because the graph as a
		//    whole should now be terminally failed already

		final JobID jid = new JobID();
		final int parallelism = 2;

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
			jid,
			TestRestartPipelinedRegionStrategy::new,
			TestRestartStrategy.directExecuting(),
			slotProvider,
			parallelism);

		graph.start(mainThreadExecutor);
		TestRestartPipelinedRegionStrategy strategy = (TestRestartPipelinedRegionStrategy) graph.getFailoverStrategy();

		// This future is used to block the failover strategy execution until we complete it
		final CompletableFuture<?> blocker = new CompletableFuture<>();
		strategy.setBlockerFuture(blocker);

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];
		final ExecutionVertex vertex2 = ejv.getTaskVertices()[1];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		// let one of the vertices fail - that triggers a local recovery action
		vertex1.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());

		// graph should still be running and the failover recovery action should be queued
		assertEquals(JobStatus.RUNNING, graph.getState());

		// now cancel the job
		graph.failGlobal(new SuppressRestartsException(new Exception("test exception")));

		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue
		blocker.complete(null);

		// now report that cancelling is complete for the other vertex
		vertex2.getCurrentExecutionAttempt().completeCancelling();

		assertEquals(JobStatus.FAILED, graph.getState());
		assertTrue(vertex1.getCurrentExecutionAttempt().getState().isTerminal());
		assertTrue(vertex2.getCurrentExecutionAttempt().getState().isTerminal());

		// make sure all slots are recycled
		assertEquals(parallelism, slotProvider.getNumberOfAvailableSlots());
	}

	/**
	 * Tests that a local failover does not try to trump a global failover.
	 */
	@Test
	public void testGlobalRecoveryConcurrentToLocalRecovery() throws Exception {

		// the logic in this test is as follows:
		//  - start a job
		//  - cause a task failure and delay the local recovery action via the manual executor
		//  - cause a global failure that is recovering immediately
		//  - resume in local recovery action
		//  - validate that this does in fact not cause another task restart, because the global
		//    recovery should already have restarted the task graph

		final JobID jid = new JobID();
		final int parallelism = 2;

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
			jid,
			TestRestartPipelinedRegionStrategy::new,
			new TestRestartStrategy(2, false), // twice restart, no delay
			slotProvider,
			parallelism);

		TestRestartPipelinedRegionStrategy strategy = (TestRestartPipelinedRegionStrategy) graph.getFailoverStrategy();

		// This future is used to block the failover strategy execution until we complete it
		CompletableFuture<?> blocker = new CompletableFuture<>();
		strategy.setBlockerFuture(blocker);

		graph.start(mainThreadExecutor);

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];
		final ExecutionVertex vertex2 = ejv.getTaskVertices()[1];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex1).getState());

		// let one of the vertices fail - that triggers a local recovery action
		vertex2.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(vertex2).getState());

		// graph should still be running and the failover recovery action should be queued
		assertEquals(JobStatus.RUNNING, graph.getState());

		// now cancel the job
		graph.failGlobal(new Exception("test exception"));

		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex1.getCurrentExecutionAttempt().getState());

		// now report that cancelling is complete for the other vertex
		vertex1.getCurrentExecutionAttempt().completeCancelling();

		waitUntilJobStatus(graph, JobStatus.RUNNING, 1000);
		assertEquals(JobStatus.RUNNING, graph.getState());

		waitUntilExecutionState(vertex1.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 1000);
		waitUntilExecutionState(vertex2.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 1000);
		vertex1.getCurrentExecutionAttempt().switchToRunning();
		vertex2.getCurrentExecutionAttempt().switchToRunning();
		assertEquals(ExecutionState.RUNNING, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.RUNNING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue - this should do nothing any more
		blocker.complete(null);

		// validate that the graph is still peachy
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex1).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex2).getState());
		assertEquals(ExecutionState.RUNNING, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.RUNNING, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(1, vertex1.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, vertex2.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, vertex1.getCopyOfPriorExecutionsList().size());
		assertEquals(1, vertex2.getCopyOfPriorExecutionsList().size());

		// make sure all slots are in use
		assertEquals(0, slotProvider.getNumberOfAvailableSlots());

		blocker = new CompletableFuture<>();
		strategy.setBlockerFuture(blocker);

		// validate that a task failure then can be handled by the local recovery
		vertex2.getCurrentExecutionAttempt().fail(new Exception("test failure"));

		// let the local recovery action continue - this should recover the vertex2
		blocker.complete(null);

		waitUntilExecutionState(vertex2.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 1000);
		vertex2.getCurrentExecutionAttempt().switchToRunning();

		// validate that the local recovery result
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex1).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex2).getState());
		assertEquals(ExecutionState.RUNNING, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.RUNNING, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(1, vertex1.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(2, vertex2.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, vertex1.getCopyOfPriorExecutionsList().size());
		assertEquals(2, vertex2.getCopyOfPriorExecutionsList().size());

		// make sure all slots are in use
		assertEquals(0, slotProvider.getNumberOfAvailableSlots());
	}

	/**
	 * Tests that a local failure fails all pending checkpoints which have not been acknowledged by the failing
	 * task.
	 */
	@Test
	public void testLocalFailureFailsPendingCheckpoints() throws Exception {
		final JobID jid = new JobID();
		final int parallelism = 2;
		final long verifyTimeout = 5000L;

		final TaskManagerGateway taskManagerGateway = mock(TaskManagerGateway.class);
		when(taskManagerGateway.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));
		when(taskManagerGateway.cancelTask(any(ExecutionAttemptID.class), any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism, taskManagerGateway);

		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			10L,
			100000L,
			1L,
			3,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);

		final ExecutionGraph graph = createSampleGraph(
			jid,
			(eg) -> new RestartIndividualStrategy(eg) {
					@Override
					protected void performExecutionVertexRestart(
						ExecutionVertex vertexToRecover,
						long globalModVersion) {
					}
				}
			,
			new TestRestartStrategy(2, false), // twice restart, no delay
			slotProvider,
			parallelism);

		graph.start(mainThreadExecutor);

		final List<ExecutionJobVertex> allVertices = new ArrayList<>(graph.getAllVertices().values());

		final StandaloneCheckpointIDCounter standaloneCheckpointIDCounter = new StandaloneCheckpointIDCounter();

		graph.enableCheckpointing(
			checkpointCoordinatorConfiguration,
			allVertices,
			allVertices,
			allVertices,
			Collections.emptyList(),
			standaloneCheckpointIDCounter,
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			new CheckpointStatsTracker(
				1,
				allVertices,
				checkpointCoordinatorConfiguration,
				UnregisteredMetricGroups.createUnregisteredTaskMetricGroup()));

		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];
		final ExecutionVertex vertex2 = ejv.getTaskVertices()[1];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		verify(taskManagerGateway, timeout(verifyTimeout).times(parallelism)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		// switch all executions to running
		for (ExecutionVertex executionVertex : graph.getAllExecutionVertices()) {
			executionVertex.getCurrentExecutionAttempt().switchToRunning();
		}

		// wait for a first checkpoint to be triggered
		verify(taskManagerGateway, timeout(verifyTimeout).times(3)).triggerCheckpoint(
			eq(vertex1.getCurrentExecutionAttempt().getAttemptId()),
			any(JobID.class),
			anyLong(),
			anyLong(),
			any(CheckpointOptions.class),
			any(Boolean.class));

		verify(taskManagerGateway, timeout(verifyTimeout).times(3)).triggerCheckpoint(
			eq(vertex2.getCurrentExecutionAttempt().getAttemptId()),
			any(JobID.class),
			anyLong(),
			anyLong(),
			any(CheckpointOptions.class),
			any(Boolean.class));

		assertEquals(3, checkpointCoordinator.getNumberOfPendingCheckpoints());

		long checkpointToAcknowledge = standaloneCheckpointIDCounter.getLast();

		checkpointCoordinator.receiveAcknowledgeMessage(
			new AcknowledgeCheckpoint(
				graph.getJobID(),
				vertex1.getCurrentExecutionAttempt().getAttemptId(),
				checkpointToAcknowledge),
				"Unknown location");

		Map<Long, PendingCheckpoint> oldPendingCheckpoints = new HashMap<>(3);

		for (PendingCheckpoint pendingCheckpoint : checkpointCoordinator.getPendingCheckpoints().values()) {
			assertFalse(pendingCheckpoint.isDiscarded());
			oldPendingCheckpoints.put(pendingCheckpoint.getCheckpointId(), pendingCheckpoint);
		}

		// let one of the vertices fail - this should trigger the failing of not acknowledged pending checkpoints
		vertex1.getCurrentExecutionAttempt().fail(new Exception("test failure"));

		for (PendingCheckpoint pendingCheckpoint : oldPendingCheckpoints.values()) {
			if (pendingCheckpoint.getCheckpointId() == checkpointToAcknowledge) {
				assertFalse(pendingCheckpoint.isDiscarded());
			} else {
				assertTrue(pendingCheckpoint.isDiscarded());
			}
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createSampleGraph(
		JobID jid,
		Factory failoverStrategy,
		RestartStrategy restartStrategy,
		SlotProvider slotProvider,
		int parallelism) throws Exception {

		final JobInformation jobInformation = new DummyJobInformation(
			jid,
			"test job");

		// build a simple execution graph with on job vertex, parallelism 2
		final Time timeout = Time.seconds(10L);
		final ExecutionGraph graph = new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			timeout,
			restartStrategy,
			failoverStrategy,
			slotProvider,
			getClass().getClassLoader(),
			VoidBlobWriter.getInstance(),
			timeout);

		JobVertex jv = new JobVertex("test vertex");
		jv.setInvokableClass(NoOpInvokable.class);
		jv.setParallelism(parallelism);

		JobGraph jg = new JobGraph(jid, "testjob", jv);
		graph.attachJobGraph(jg.getVerticesSortedTopologicallyFromSources());

		return graph;
	}

	/**
	 * Test implementation of the {@link RestartPipelinedRegionStrategy} that makes it possible to control when the
	 * failover action is performed via {@link CompletableFuture}.
	 */
	static class TestRestartPipelinedRegionStrategy extends RestartPipelinedRegionStrategy {

		@Nonnull
		CompletableFuture<?> blockerFuture;

		public TestRestartPipelinedRegionStrategy(ExecutionGraph executionGraph) {
			super(executionGraph);
			this.blockerFuture = CompletableFuture.completedFuture(null);
		}

		public void setBlockerFuture(@Nonnull CompletableFuture<?> blockerFuture) {
			this.blockerFuture = blockerFuture;
		}

		@Override
		protected FailoverRegion createFailoverRegion(ExecutionGraph eg, List<ExecutionVertex> connectedExecutions) {
			Map<JobVertexID, ExecutionJobVertex> tasks = initTasks(connectedExecutions);
			return new FailoverRegion(eg, connectedExecutions, tasks) {
				@Override
				protected CompletableFuture<Void> createTerminationFutureOverAllConnectedVertexes() {
					ArrayList<CompletableFuture<?>> terminationAndBlocker = new ArrayList<>(2);
					terminationAndBlocker.add(super.createTerminationFutureOverAllConnectedVertexes());
					terminationAndBlocker.add(blockerFuture);
					return FutureUtils.waitForAll(terminationAndBlocker);
				}
			};
		}
	}
}
