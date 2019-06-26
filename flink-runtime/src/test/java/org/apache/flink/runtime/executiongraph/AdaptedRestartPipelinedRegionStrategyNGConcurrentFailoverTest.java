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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AdaptedRestartPipelinedRegionStrategyNGFailoverTest.TestAdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AdaptedRestartPipelinedRegionStrategyNG} failover handling when concurrent failovers happen.
 * There can be local+local and local+global concurrent failovers.
 */
public class AdaptedRestartPipelinedRegionStrategyNGConcurrentFailoverTest extends TestLogger {

	private static final int DEFAULT_PARALLELISM = 2;

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource();

	private final TestingComponentMainThreadExecutor testMainThreadUtil =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

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

		final JobID jid = new JobID();
		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, DEFAULT_PARALLELISM);
		final TestRestartStrategy restartStrategy = TestRestartStrategy.manuallyTriggered();

		final ExecutionGraph eg = createExecutionGraph(
			jid,
			TestAdaptedRestartPipelinedRegionStrategyNG::new,
			restartStrategy,
			slotProvider);

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();
		failoverStrategy.setBlockerFuture(new CompletableFuture<>());

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// start job scheduling
		testMainThreadUtil.execute(eg::scheduleForExecution);

		// fail ev11 to trigger region failover of {ev11}, {ev21}, {ev22}
		testMainThreadUtil.execute(() -> ev11.getCurrentExecutionAttempt().fail(new Exception("task failure 1")));
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// fail ev12 to trigger region failover of {ev12}, {ev21}, {ev22}
		testMainThreadUtil.execute(() -> ev12.getCurrentExecutionAttempt().fail(new Exception("task failure 2")));
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.FAILED, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// complete region failover blocker to trigger region failover recovery
		testMainThreadUtil.execute(() -> failoverStrategy.getBlockerFuture().complete(null));

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

		final JobID jid = new JobID();
		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, DEFAULT_PARALLELISM);
		final TestRestartStrategy restartStrategy = TestRestartStrategy.manuallyTriggered();

		final ExecutionGraph eg = createExecutionGraph(
			jid,
			TestAdaptedRestartPipelinedRegionStrategyNG::new,
			restartStrategy,
			slotProvider);

		final TestAdaptedRestartPipelinedRegionStrategyNG failoverStrategy =
			(TestAdaptedRestartPipelinedRegionStrategyNG) eg.getFailoverStrategy();
		failoverStrategy.setBlockerFuture(new CompletableFuture<>());

		final Iterator<ExecutionVertex> vertexIterator = eg.getAllExecutionVertices().iterator();
		final ExecutionVertex ev11 = vertexIterator.next();
		final ExecutionVertex ev12 = vertexIterator.next();
		final ExecutionVertex ev21 = vertexIterator.next();
		final ExecutionVertex ev22 = vertexIterator.next();

		// start job scheduling
		testMainThreadUtil.execute(eg::scheduleForExecution);

		// fail ev11 to trigger region failover of {ev11}, {ev21}, {ev22}
		testMainThreadUtil.execute(() -> ev11.getCurrentExecutionAttempt().fail(new Exception("task failure")));
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(ExecutionState.FAILED, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev21.getExecutionState());
		assertEquals(ExecutionState.CANCELED, ev22.getExecutionState());

		// trigger global failover cancelling and immediately recovery
		testMainThreadUtil.execute(() -> {
			eg.failGlobal(new Exception("Test global failure"));
			ev12.getCurrentExecutionAttempt().completeCancelling();
			restartStrategy.triggerNextAction();
		});

		// verify the job state and vertex attempt number
		waitUntilJobStatus(eg, JobStatus.RUNNING, 2000L);
		assertEquals(2, eg.getGlobalModVersion());
		assertEquals(1, ev11.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev12.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev21.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, ev22.getCurrentExecutionAttempt().getAttemptNumber());

		// complete region failover blocker to trigger region failover
		testMainThreadUtil.execute(() -> failoverStrategy.getBlockerFuture().complete(null));

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
	private ExecutionGraph createExecutionGraph(
		JobID jid,
		Factory failoverStrategy,
		RestartStrategy restartStrategy,
		SlotProvider slotProvider) throws Exception {

		final JobInformation jobInformation = new DummyJobInformation(
			jid,
			"test job");

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

		JobVertex v1 = new JobVertex("vertex1");
		v1.setInvokableClass(NoOpInvokable.class);
		v1.setParallelism(DEFAULT_PARALLELISM);

		JobVertex v2 = new JobVertex("vertex2");
		v2.setInvokableClass(NoOpInvokable.class);
		v2.setParallelism(DEFAULT_PARALLELISM);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		JobGraph jg = new JobGraph(jid, "testjob", v1, v2);
		graph.attachJobGraph(jg.getVerticesSortedTopologicallyFromSources());

		graph.start(testMainThreadUtil.getMainThreadExecutor());

		return graph;
	}
}
