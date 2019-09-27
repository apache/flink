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
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AdaptedRestartPipelinedRegionStrategyNGFailoverTest.TestAdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.PartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG} failover handling when concurrent failovers happen.
 * There can be local+local and local+global concurrent failovers.
 */
public class AdaptedRestartPipelinedRegionStrategyNGConcurrentFailoverTest extends TestLogger {

	private static final JobID TEST_JOB_ID = new JobID();

	private static final int DEFAULT_PARALLELISM = 2;

	private ManuallyTriggeredScheduledExecutor manualMainThreadExecutor;

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private TestRestartStrategy manuallyTriggeredRestartStrategy;

	@Before
	public void setUp() {
		manualMainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
		componentMainThreadExecutor = new ComponentMainThreadExecutorServiceAdapter(manualMainThreadExecutor, Thread.currentThread());
		manuallyTriggeredRestartStrategy = TestRestartStrategy.manuallyTriggered();
	}

	/**
	 * Tests that 2 concurrent region failovers can lead to a properly vertex state.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *       (blocking)
	 * </pre>
	 */
	@Test
	public void testConcurrentRegionFailovers() throws Exception {

		// the logic in this test is as follows:
		//  - start a job
		//  - cause {ev11} failure and delay the local recovery action via the manual executor
		//  - cause {ev12} failure and delay the local recovery action via the manual executor
		//  - resume local recovery actions
		//  - validate that each task is restarted only once

		final ExecutionGraph eg = createExecutionGraph();

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();
		failoverStrategy.setBlockerFuture(new CompletableFuture<>());

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// start job scheduling
		eg.scheduleForExecution();
		manualMainThreadExecutor.triggerAll();

		// fail ev11 to trigger region failover of {ev11}, {ev21}, {ev22}
		ev11.getCurrentExecutionAttempt().fail(new Exception("task failure 1"));
		manualMainThreadExecutor.triggerAll();
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// fail ev12 to trigger region failover of {ev12}, {ev21}, {ev22}
		ev12.getCurrentExecutionAttempt().fail(new Exception("task failure 2"));
		manualMainThreadExecutor.triggerAll();
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.FAILED, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// complete region failover blocker to trigger region failover recovery
		failoverStrategy.getBlockerFuture().complete(null);
		manualMainThreadExecutor.triggerAll();
		manuallyTriggeredRestartStrategy.triggerAll();
		manualMainThreadExecutor.triggerAll();

		// verify that all tasks are recovered and no task is restarted more than once
		assertEquals(ExecutionState.DEPLOYING, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev21.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev22.getExecutionState());
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev22.getCurrentExecutionAttempt().getAttemptNumber());
	}

	/**
	 * Tests that a global failover will take precedence over local failovers.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *       (blocking)
	 * </pre>
	 */
	@Test
	public void testRegionFailoverInterruptedByGlobalFailover() throws Exception {

		// the logic in this test is as follows:
		//  - start a job
		//  - cause a task failure and delay the local recovery action via the manual executor
		//  - cause a global failure
		//  - resume in local recovery action
		//  - validate that the local recovery does not restart tasks

		final ExecutionGraph eg = createExecutionGraph();

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();
		failoverStrategy.setBlockerFuture(new CompletableFuture<>());

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// start job scheduling
		eg.scheduleForExecution();
		manualMainThreadExecutor.triggerAll();

		// fail ev11 to trigger region failover of {ev11}, {ev21}, {ev22}
		ev11.getCurrentExecutionAttempt().fail(new Exception("task failure"));
		manualMainThreadExecutor.triggerAll();
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// trigger global failover cancelling and immediately recovery
		eg.failGlobal(new Exception("Test global failure"));
		ev12.getCurrentExecutionAttempt().completeCancelling();
		manuallyTriggeredRestartStrategy.triggerNextAction();
		manualMainThreadExecutor.triggerAll();

		// verify the job state and vertex attempt number
		assertEquals(2, eg.getGlobalModVersion());
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev22.getCurrentExecutionAttempt().getAttemptNumber());

		// complete region failover blocker to trigger region failover
		failoverStrategy.getBlockerFuture().complete(null);
		manualMainThreadExecutor.triggerAll();

		// verify that no task is restarted by region failover
		assertEquals(ExecutionState.DEPLOYING, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev21.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev22.getExecutionState());
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev22.getCurrentExecutionAttempt().getAttemptNumber());
	}

	@Test
	public void testSkipFailoverIfExecutionStateIsNotRunning() throws Exception {
		final ExecutionGraph executionGraph = createExecutionGraph();

		final Iterator<ExecutionVertex> vertexIterator = executionGraph.getAllExecutionVertices().iterator();
		final ExecutionVertex firstVertex = vertexIterator.next();

		executionGraph.cancel();

		final FailoverStrategy failoverStrategy = executionGraph.getFailoverStrategy();
		failoverStrategy.onTaskFailure(firstVertex.getCurrentExecutionAttempt(), new Exception("Test Exception"));
		manualMainThreadExecutor.triggerAll();

		assertEquals(ExecutionState.CANCELED, firstVertex.getExecutionState());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	/**
	 * Creating a sample ExecutionGraph for testing with topology as below.
	 * <pre>
	 *     (v11) -+-> (v21)
	 *            x
	 *     (v12) -+-> (v22)
	 *
	 *            ^
	 *            |
	 *       (blocking)
	 * </pre>
	 * 4 regions. Each consists of one individual execution vertex.
	 */
	private ExecutionGraph createExecutionGraph() throws Exception {

		final JobVertex v1 = new JobVertex("vertex1");
		v1.setInvokableClass(NoOpInvokable.class);
		v1.setParallelism(DEFAULT_PARALLELISM);

		final JobVertex v2 = new JobVertex("vertex2");
		v2.setInvokableClass(NoOpInvokable.class);
		v2.setParallelism(DEFAULT_PARALLELISM);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jg = new JobGraph(TEST_JOB_ID, "testjob", v1, v2);

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(TEST_JOB_ID, DEFAULT_PARALLELISM);

		final PartitionTracker partitionTracker = new PartitionTrackerImpl(
			jg.getJobID(),
			NettyShuffleMaster.INSTANCE,
			ignored -> Optional.empty());

		final ExecutionGraph graph = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(jg)
			.setRestartStrategy(manuallyTriggeredRestartStrategy)
			.setFailoverStrategyFactory(TestAdaptedRestartPipelinedRegionStrategyNG::new)
			.setSlotProvider(slotProvider)
			.setPartitionTracker(partitionTracker)
			.build();

		graph.start(componentMainThreadExecutor);

		return graph;
	}
}
