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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionConnectionException;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG} failover handling.
 */
public class AdaptedRestartPipelinedRegionStrategyNGFailoverTest extends TestLogger {

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource();

	private final TestingComponentMainThreadExecutor testMainThreadUtil =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

	/**
	 * Tests for region failover for job in EAGER mode.
	 * This applies to streaming job, with no BLOCKING edge.
	 * <pre>
	 *     (v11) ---> (v21)
	 *
	 *     (v12) ---> (v22)
	 *
	 *            ^
	 *            |
	 *       (pipelined)
	 * </pre>
	 */
	@Test
	public void testRegionFailoverInEagerMode() throws Exception {
		// create a streaming job graph with EAGER schedule mode
		final JobGraph jobGraph = createStreamingJobGraph();
		final long checkpointId = 42L;
		final ExecutionGraph eg = createExecutionGraph(jobGraph, checkpointId);

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// trigger task failure of ev11
		// vertices { ev11, ev21 } should be affected
		testMainThreadUtil.execute(() -> ev11.getCurrentExecutionAttempt().fail(new Exception("Test Exception")));

		// verify vertex states and complete cancellation
		assertVertexInState(ExecutionState.FAILED, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.CANCELING, ev21);
		assertVertexInState(ExecutionState.DEPLOYING, ev22);
		testMainThreadUtil.execute(() -> ev21.getCurrentExecutionAttempt().completeCancelling());

		// verify vertex states
		// in eager mode, all affected vertices should be scheduled in failover
		assertVertexInState(ExecutionState.DEPLOYING, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.DEPLOYING, ev21);
		assertVertexInState(ExecutionState.DEPLOYING, ev22);

		// verify attempt number
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(0, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(0, ev22.getCurrentExecutionAttempt().getAttemptNumber());
	}

	/**
	 * Tests for scenes that a task fails for its own error, in which case the
	 * region containing the failed task and its consumer regions should be restarted.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *        (blocking)
	 * </pre>
	 */
	@Test
	public void testRegionFailoverForRegionInternalErrorsInLazyMode() throws Exception {
		// create a batch job graph with LAZY_FROM_SOURCES schedule mode
		final JobGraph jobGraph = createBatchJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();
		failoverStrategy.setBlockerFuture(new CompletableFuture<>());

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// trigger task failure of ev11
		// regions {ev11}, {ev21}, {ev22} should be affected
		testMainThreadUtil.execute(() -> ev11.getCurrentExecutionAttempt().fail(new Exception("Test Exception")));

		// verify vertex states and complete cancellation
		assertVertexInState(ExecutionState.FAILED, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.CANCELED, ev21);
		assertVertexInState(ExecutionState.CANCELED, ev22);
		testMainThreadUtil.execute(() -> failoverStrategy.getBlockerFuture().complete(null));

		// verify vertex states
		// only vertices with consumable inputs can be scheduled
		assertVertexInState(ExecutionState.DEPLOYING, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.CREATED, ev21);
		assertVertexInState(ExecutionState.CREATED, ev22);

		// verify attempt number
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(0, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev22.getCurrentExecutionAttempt().getAttemptNumber());
	}

	/**
	 * Tests that the failure is properly propagated to underlying strategy
	 * to calculate tasks to restart.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *        (blocking)
	 * </pre>
	 */
	@Test
	public void testFailurePropagationToUnderlyingStrategy() throws Exception {
		// create a batch job graph with LAZY_FROM_SOURCES schedule mode
		final JobGraph jobGraph = createBatchJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();

		// trigger downstream regions to schedule
		testMainThreadUtil.execute(() -> {
			// finish upstream regions to trigger scheduling of downstream regions
			ev11.getCurrentExecutionAttempt().markFinished();
			ev12.getCurrentExecutionAttempt().markFinished();
		});

		// trigger task failure of ev21 on consuming data from ev11
		Exception taskFailureCause = new PartitionConnectionException(
			new ResultPartitionID(
				ev11.getProducedPartitions().keySet().iterator().next(),
				ev11.getCurrentExecutionAttempt().getAttemptId()),
			new Exception("Test failure"));
		testMainThreadUtil.execute(() -> ev21.getCurrentExecutionAttempt().fail(taskFailureCause));

		// verify that the restarted task is the same as the underlying strategy suggested
		assertEquals(
			failoverStrategy.getRestartPipelinedRegionStrategy().getTasksNeedingRestart(
				new ExecutionVertexID(ev21.getJobvertexId(), ev21.getParallelSubtaskIndex()),
				taskFailureCause),
			failoverStrategy.getLastTasksToCancel());
	}

	/**
	 * Tests that checkpointing is working correctly in region failover.
	 */
	@Test
	public void testCheckpointingInRegionFailover() throws Exception {
		final JobGraph jobGraph = createStreamingJobGraph();
		final long checkpointId = 42L;
		final ExecutionGraph eg = createExecutionGraph(jobGraph, checkpointId);

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		testMainThreadUtil.execute(() -> acknowledgeAllCheckpoints(
			checkpointId, eg.getCheckpointCoordinator(), eg.getAllExecutionVertices().iterator()));

		// verify checkpoint has been completed successfully.
		assertEquals(1, eg.getCheckpointCoordinator().getCheckpointStore().getNumberOfRetainedCheckpoints());
		assertEquals(checkpointId, eg.getCheckpointCoordinator().getCheckpointStore().getLatestCheckpoint(false).getCheckpointID());

		testMainThreadUtil.execute(() -> ev11.getCurrentExecutionAttempt().fail(new Exception("Test Exception")));

		// ensure vertex state and complete cancellation
		assertVertexInState(ExecutionState.FAILED, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.CANCELING, ev21);
		assertVertexInState(ExecutionState.DEPLOYING, ev22);
		testMainThreadUtil.execute(() -> ev21.getCurrentExecutionAttempt().completeCancelling());

		verifyCheckpointRestoredAsExpected(checkpointId, eg);
	}

	/**
	 * Tests that the vertex partition state is reset property during failover.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *        (blocking)
	 * </pre>
	 */
	@Test
	public void testStatusResettingOnRegionFailover() throws Exception {
		// create a batch job graph with LAZY_FROM_SOURCES schedule mode
		final JobGraph jobGraph = createBatchJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph);
		final AdaptedRestartPipelinedRegionStrategyNG strategy =
			(AdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// finish upstream regions so that all the blocking result partitions should be finished and consumable
		testMainThreadUtil.execute(() -> {
			// finish upstream regions to trigger scheduling of downstream regions
			ev11.getCurrentExecutionAttempt().markFinished();
			ev12.getCurrentExecutionAttempt().markFinished();
		});

		testMainThreadUtil.execute(() -> {
			// force FINISHED ev11 to fail to reset its partition
			strategy.onTaskFailure(ev11.getCurrentExecutionAttempt(), new FlinkException("Fail for testing"));

			// complete cancelling
			ev21.getCurrentExecutionAttempt().completeCancelling();
			ev22.getCurrentExecutionAttempt().completeCancelling();
		});

		// the blocking result partitions should be no more consumable after the resetting
		IntermediateResult result = ev11.getJobVertex().getProducedDataSets()[0];
		assertFalse(result.areAllPartitionsFinished());
		assertFalse(result.getPartitions()[0].isConsumable());
		assertFalse(result.getPartitions()[1].isConsumable());
	}

	/**
	 * Tests that when a task fail, and restart strategy doesn't support restarting, the job will go to failed.
	 */
	@Test
	public void testNoRestart() throws Exception {
		final JobGraph jobGraph = createBatchJobGraph();
		final NoRestartStrategy restartStrategy = new NoRestartStrategy();
		final ExecutionGraph eg = createExecutionGraph(jobGraph, restartStrategy);

		final ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		testMainThreadUtil.execute(() -> {
			ev.fail(new Exception("Test Exception"));

			for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
				evs.getCurrentExecutionAttempt().completeCancelling();
			}
		});
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	// ------------------------------- Test Utils -----------------------------------------

	/**
	 * Creating job graph as below (execution view).
	 * It's a representative of streaming job.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *       (pipelined)
	 * </pre>
	 * 2 regions. Each has 2 pipelined connected vertices.
	 */
	private JobGraph createStreamingJobGraph() {
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");

		v1.setParallelism(2);
		v2.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(v1, v2);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		return jobGraph;
	}

	/**
	 * Creating job graph as below (execution view).
	 * It's a representative of batch job.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *        (blocking)
	 * </pre>
	 * 4 regions. Each consists of one individual vertex.
	 */
	private JobGraph createBatchJobGraph() {
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");

		v1.setParallelism(2);
		v2.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(v1, v2);
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		return jobGraph;
	}

	private ExecutionGraph createExecutionGraph(final JobGraph jobGraph) throws Exception {
		return createExecutionGraph(jobGraph, -1L);
	}

	private ExecutionGraph createExecutionGraph(final JobGraph jobGraph, final long checkpointId) throws Exception {
		final RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy(10);
		return createExecutionGraph(jobGraph, restartStrategy, checkpointId);
	}

	private ExecutionGraph createExecutionGraph(
		final JobGraph jobGraph,
		final RestartStrategy restartStrategy) throws Exception {

		return createExecutionGraph(jobGraph, restartStrategy, -1L);
	}

	private ExecutionGraph createExecutionGraph(
		final JobGraph jobGraph,
		final RestartStrategy restartStrategy,
		final long checkpointId) throws Exception {

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jobGraph.getJobID(), 14);

		final ExecutionGraph eg = new ExecutionGraph(
			new DummyJobInformation(
				jobGraph.getJobID(),
				jobGraph.getName()),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			TestAdaptedRestartPipelinedRegionStrategyNG::new,
			slotProvider);
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		if (checkpointId >= 0) {
			enableCheckpointing(eg);
		}

		eg.setScheduleMode(jobGraph.getScheduleMode());

		eg.start(testMainThreadUtil.getMainThreadExecutor());
		testMainThreadUtil.execute(() -> {
			eg.scheduleForExecution();

			if (checkpointId >= 0) {
				attachPendingCheckpoints(checkpointId, eg);
			}
		});

		return eg;
	}

	private static void assertVertexInState(final ExecutionState state, final ExecutionVertex vertex) {
		assertEquals(state, vertex.getExecutionState());
	}

	private static void enableCheckpointing(final ExecutionGraph eg) {
		final ArrayList<ExecutionJobVertex> jobVertices = new ArrayList<>(eg.getAllVertices().values());
		final CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			1000,
			100,
			0,
			1,
			CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION,
			true,
			false,
			0);
		eg.enableCheckpointing(
			chkConfig,
			jobVertices,
			jobVertices,
			jobVertices,
			Collections.emptyList(),
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			new CheckpointStatsTracker(
				0,
				jobVertices,
				mock(CheckpointCoordinatorConfiguration.class),
				new UnregisteredMetricsGroup()));
	}

	/**
	 * Attach pending checkpoints of chk-{checkpointId} and chk-{checkpointId+1} to the execution graph.
	 * If {@link #acknowledgeAllCheckpoints(long, CheckpointCoordinator, Iterator)} called then,
	 * chk-{{checkpointId}} would become the completed checkpoint.
	 */
	private void attachPendingCheckpoints(final long checkpointId, final ExecutionGraph eg) throws IOException {
		final Map<Long, PendingCheckpoint> pendingCheckpoints = new HashMap<>();
		final Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm = new HashMap<>();
		eg.getAllExecutionVertices().forEach(e -> {
			Execution ee = e.getCurrentExecutionAttempt();
			if (ee != null) {
				verticesToConfirm.put(ee.getAttemptId(), e);
			}
		});

		final CheckpointCoordinator checkpointCoordinator = eg.getCheckpointCoordinator();
		assertNotNull(checkpointCoordinator);
		final CheckpointStorageCoordinatorView checkpointStorage = checkpointCoordinator.getCheckpointStorage();
		pendingCheckpoints.put(checkpointId, new PendingCheckpoint(
			eg.getJobID(),
			checkpointId,
			0L,
			verticesToConfirm,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			checkpointStorage.initializeLocationForCheckpoint(checkpointId),
			eg.getFutureExecutor()));

		final long newCheckpointId = checkpointId + 1;
		pendingCheckpoints.put(newCheckpointId, new PendingCheckpoint(
			eg.getJobID(),
			newCheckpointId,
			0L,
			verticesToConfirm,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			checkpointStorage.initializeLocationForCheckpoint(newCheckpointId),
			eg.getFutureExecutor()));
		Whitebox.setInternalState(checkpointCoordinator, "pendingCheckpoints", pendingCheckpoints);
	}

	/**
	 * Let the checkpoint coordinator receive all acknowledges from given executionVertices
	 * to complete the expected checkpoint.
	 */
	private void acknowledgeAllCheckpoints(
		final long checkpointId,
		final CheckpointCoordinator checkpointCoordinator,
		final Iterator<ExecutionVertex> executionVertices) throws IOException, CheckpointException {

		while (executionVertices.hasNext()) {
			final ExecutionVertex executionVertex = executionVertices.next();
			for (int index = 0; index < executionVertex.getJobVertex().getParallelism(); index++) {
				final JobVertexID jobVertexID = executionVertex.getJobvertexId();
				final OperatorStateHandle opStateBackend = CheckpointCoordinatorTest.generatePartitionableStateHandle(
					jobVertexID, index, 2, 8, false);
				final OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(
					opStateBackend, null, null, null);
				final TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
				taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID), operatorSubtaskState);

				final AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					executionVertex.getJobId(),
					executionVertex.getJobVertex().getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

				checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "Unknown location");
			}
		}
	}

	private void verifyCheckpointRestoredAsExpected(final long checkpointId, final ExecutionGraph eg) throws Exception {
		// pending checkpoints have already been cancelled.
		assertNotNull(eg.getCheckpointCoordinator());
		assertTrue(eg.getCheckpointCoordinator().getPendingCheckpoints().isEmpty());

		// verify checkpoint has been restored successfully.
		assertEquals(1, eg.getCheckpointCoordinator().getCheckpointStore().getNumberOfRetainedCheckpoints());
		assertEquals(checkpointId, eg.getCheckpointCoordinator().getCheckpointStore().getLatestCheckpoint(false).getCheckpointID());
	}

	/**
	 * Test implementation of the {@link AdaptedRestartPipelinedRegionStrategyNG} that makes it possible
	 * to control when the failover action is performed via {@link CompletableFuture}.
	 * It also exposes some internal state of {@link AdaptedRestartPipelinedRegionStrategyNG}.
	 */
	static class TestAdaptedRestartPipelinedRegionStrategyNG extends AdaptedRestartPipelinedRegionStrategyNG {

		@Nonnull
		private CompletableFuture<?> blockerFuture;

		private Set<ExecutionVertexID> lastTasksToRestart;

		TestAdaptedRestartPipelinedRegionStrategyNG(ExecutionGraph executionGraph) {
			super(executionGraph);
			this.blockerFuture = CompletableFuture.completedFuture(null);
		}

		void setBlockerFuture(@Nonnull CompletableFuture<?> blockerFuture) {
			this.blockerFuture = blockerFuture;
		}

		@Override
		protected void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
			this.lastTasksToRestart = verticesToRestart;
			super.restartTasks(verticesToRestart);
		}

		@Override
		protected CompletableFuture<?> cancelTasks(final Set<ExecutionVertexID> vertices) {
			ArrayList<CompletableFuture<?>> terminationAndBlocker = new ArrayList<>(2);
			terminationAndBlocker.add(super.cancelTasks(vertices));
			terminationAndBlocker.add(blockerFuture);
			return FutureUtils.waitForAll(terminationAndBlocker);
		}

		CompletableFuture<?> getBlockerFuture() {
			return blockerFuture;
		}

		Set<ExecutionVertexID> getLastTasksToCancel() {
			return lastTasksToRestart;
		}

		RestartPipelinedRegionStrategy getRestartPipelinedRegionStrategy() {
			return restartPipelinedRegionStrategy;
		}
	}
}
