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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.restart.FailingRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.PartitionConnectionException;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG} failover handling.
 */
public class AdaptedRestartPipelinedRegionStrategyNGFailoverTest extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private ManuallyTriggeredScheduledExecutor manualMainThreadExecutor;

	@Before
	public void setUp() {
		manualMainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
		componentMainThreadExecutor = new ComponentMainThreadExecutorServiceAdapter(manualMainThreadExecutor, Thread.currentThread());
	}

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
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// trigger task failure of ev11
		// vertices { ev11, ev21 } should be affected
		ev11.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		manualMainThreadExecutor.triggerAll();
		manualMainThreadExecutor.triggerScheduledTasks();

		// verify vertex states and complete cancellation
		assertVertexInState(ExecutionState.FAILED, ev11);
		assertVertexInState(ExecutionState.DEPLOYING, ev12);
		assertVertexInState(ExecutionState.CANCELING, ev21);
		assertVertexInState(ExecutionState.DEPLOYING, ev22);
		ev21.getCurrentExecutionAttempt().completeCancelling();
		manualMainThreadExecutor.triggerAll();
		manualMainThreadExecutor.triggerScheduledTasks();

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
	 * Tests for scenario where a task fails for its own error, in which case the
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
		final JobGraph jobGraph = createBatchJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// trigger task failure of ev11
		// regions {ev11}, {ev21}, {ev22} should be affected
		ev11.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		manualMainThreadExecutor.triggerAll();
		manualMainThreadExecutor.triggerScheduledTasks();

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
		final JobGraph jobGraph = createBatchJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph);

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// finish upstream regions to trigger scheduling of downstream regions
		ev11.getCurrentExecutionAttempt().markFinished();
		ev12.getCurrentExecutionAttempt().markFinished();

		// trigger task failure of ev21 on consuming data from ev11
		Exception taskFailureCause = new PartitionConnectionException(
			new ResultPartitionID(
				ev11.getProducedPartitions().keySet().iterator().next(),
				ev11.getCurrentExecutionAttempt().getAttemptId()),
			new Exception("Test failure"));
		ev21.getCurrentExecutionAttempt().fail(taskFailureCause);
		manualMainThreadExecutor.triggerAll();

		assertThat(failoverStrategy.getLastTasksToCancel(),
			containsInAnyOrder(ev11.getID(), ev21.getID(), ev22.getID()));
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

		ev.fail(new Exception("Test Exception"));

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().completeCancelling();
		}

		manualMainThreadExecutor.triggerAll();

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that the execution of the restart logic of the failover strategy is dependent on the restart strategy
	 * calling {@link RestartCallback#triggerFullRecovery()}.
	 */
	@Test
	public void testFailoverExecutionDependentOnRestartStrategyRecoveryTrigger() throws Exception {
		final JobGraph jobGraph = createBatchJobGraph();
		final TestRestartStrategy restartStrategy = new TestRestartStrategy();

		final ExecutionGraph eg = createExecutionGraph(jobGraph, restartStrategy);

		final ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		ev.fail(new Exception("Test Exception"));

		manualMainThreadExecutor.triggerAll();

		// the entire failover-procedure is being halted by the restart strategy not doing anything
		// the only thing the failover strategy should do is cancel tasks that require it

		// sanity check to ensure we actually called into the restart strategy
		assertEquals(restartStrategy.getNumberOfQueuedActions(), 1);
		// 3 out of 4 tasks will be canceled, and removed from the set of registered executions
		assertEquals(eg.getRegisteredExecutions().size(), 1);
		// no job state change should occur; in case of a failover we never switch to RESTARTING/CANCELED
		// the important thing is that we don't switch to failed which would imply that we started a global failover
		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	@Test
	public void testFailGlobalIfErrorOnRestartTasks() throws Exception {
		final JobGraph jobGraph = createStreamingJobGraph();
		final ExecutionGraph eg = createExecutionGraph(jobGraph, new FailingRestartStrategy(1));

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		final long globalModVersionBeforeFailure = eg.getGlobalModVersion();

		ev11.fail(new Exception("Test Exception"));
		completeCancelling(ev11, ev12, ev21, ev22);

		manualMainThreadExecutor.triggerAll();
		manualMainThreadExecutor.triggerScheduledTasks();

		final long globalModVersionAfterFailure = eg.getGlobalModVersion();

		assertNotEquals(globalModVersionBeforeFailure, globalModVersionAfterFailure);
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

		final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob", v1, v2);
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
		return createExecutionGraph(jobGraph, new FixedDelayRestartStrategy(10, 0));
	}

	private ExecutionGraph createExecutionGraph(
			final JobGraph jobGraph,
			final RestartStrategy restartStrategy) throws Exception {

		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			jobGraph.getJobID(),
			NettyShuffleMaster.INSTANCE,
			ignored -> Optional.empty());

		final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(jobGraph)
			.setRestartStrategy(restartStrategy)
			.setFailoverStrategyFactory(TestAdaptedRestartPipelinedRegionStrategyNG::new)
			.setPartitionTracker(partitionTracker)
			.build();

		eg.start(componentMainThreadExecutor);
		eg.scheduleForExecution();
		manualMainThreadExecutor.triggerAll();

		return eg;
	}

	private static void assertVertexInState(final ExecutionState state, final ExecutionVertex vertex) {
		assertEquals(state, vertex.getExecutionState());
	}

	private static void completeCancelling(ExecutionVertex... executionVertices) {
		for (final ExecutionVertex executionVertex : executionVertices) {
			executionVertex.getCurrentExecutionAttempt().completeCancelling();
		}
	}

	/**
	 * Test implementation of the {@link AdaptedRestartPipelinedRegionStrategyNG} that makes it possible
	 * to control when the failover action is performed via {@link CompletableFuture}.
	 * It also exposes some internal state of {@link AdaptedRestartPipelinedRegionStrategyNG}.
	 */
	static class TestAdaptedRestartPipelinedRegionStrategyNG extends AdaptedRestartPipelinedRegionStrategyNG {

		private CompletableFuture<?> blockerFuture;

		private Set<ExecutionVertexID> lastTasksToRestart;

		TestAdaptedRestartPipelinedRegionStrategyNG(ExecutionGraph executionGraph) {
			super(executionGraph);
			this.blockerFuture = CompletableFuture.completedFuture(null);
		}

		void setBlockerFuture(CompletableFuture<?> blockerFuture) {
			this.blockerFuture = blockerFuture;
		}

		@Override
		protected void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
			this.lastTasksToRestart = verticesToRestart;
			super.restartTasks(verticesToRestart);
		}

		@Override
		protected CompletableFuture<?> cancelTasks(final Set<ExecutionVertexID> vertices) {
			final List<CompletableFuture<?>> terminationAndBlocker = Arrays.asList(
				super.cancelTasks(vertices),
				blockerFuture);
			return FutureUtils.waitForAll(terminationAndBlocker);
		}

		CompletableFuture<?> getBlockerFuture() {
			return blockerFuture;
		}

		Set<ExecutionVertexID> getLastTasksToCancel() {
			return lastTasksToRestart;
		}
	}
}
