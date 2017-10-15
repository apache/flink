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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.ManuallyTriggeredDirectExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.failover.RestartIndividualStrategy;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;

import static org.junit.Assert.*;

/**
 * These tests make sure that global failover (restart all) always takes precedence over
 * local recovery strategies.
 * 
 * <p>This test must be in the package it resides in, because it uses package-private methods
 * from the ExecutionGraph classes.
 */
public class IndividualRestartsConcurrencyTest {

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

		final ManuallyTriggeredDirectExecutor executor = new ManuallyTriggeredDirectExecutor();

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
				jid,
				new IndividualFailoverWithCustomExecutor(executor),
				slotProvider,
				2);

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
		assertEquals(1, executor.numQueuedRunnables());

		// now cancel the job
		graph.cancel();

		assertEquals(JobStatus.CANCELLING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue
		executor.trigger();

		// now report that cancelling is complete for the other vertex
		vertex2.getCurrentExecutionAttempt().cancelingComplete();

		assertEquals(JobStatus.CANCELED, graph.getState());
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

		final ManuallyTriggeredDirectExecutor executor = new ManuallyTriggeredDirectExecutor();

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
				jid,
				new IndividualFailoverWithCustomExecutor(executor),
				slotProvider,
				2);

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
		assertEquals(1, executor.numQueuedRunnables());

		// now cancel the job
		graph.failGlobal(new Exception("test exception"));

		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue
		executor.trigger();

		// now report that cancelling is complete for the other vertex
		vertex2.getCurrentExecutionAttempt().cancelingComplete();

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

		final ManuallyTriggeredDirectExecutor executor = new ManuallyTriggeredDirectExecutor();

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jid, parallelism);

		final ExecutionGraph graph = createSampleGraph(
				jid,
				new IndividualFailoverWithCustomExecutor(executor),
				new FixedDelayRestartStrategy(1, 0), // one restart, no delay
				slotProvider,
				2);

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];
		final ExecutionVertex vertex2 = ejv.getTaskVertices()[1];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());

		// let one of the vertices fail - that triggers a local recovery action
		vertex2.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex2.getCurrentExecutionAttempt().getState());

		// graph should still be running and the failover recovery action should be queued
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(1, executor.numQueuedRunnables());

		// now cancel the job
		graph.failGlobal(new Exception("test exception"));

		assertEquals(JobStatus.FAILING, graph.getState());
		assertEquals(ExecutionState.FAILED, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.CANCELING, vertex1.getCurrentExecutionAttempt().getState());

		// now report that cancelling is complete for the other vertex
		vertex1.getCurrentExecutionAttempt().cancelingComplete();

		waitUntilJobStatus(graph, JobStatus.RUNNING, 1000);
		assertEquals(JobStatus.RUNNING, graph.getState());

		waitUntilExecutionState(vertex1.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 1000);
		waitUntilExecutionState(vertex2.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 1000);
		vertex1.getCurrentExecutionAttempt().switchToRunning();
		vertex2.getCurrentExecutionAttempt().switchToRunning();
		assertEquals(ExecutionState.RUNNING, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.RUNNING, vertex2.getCurrentExecutionAttempt().getState());

		// let the recovery action continue - this should do nothing any more
		executor.trigger();

		// validate that the graph is still peachy
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(ExecutionState.RUNNING, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(ExecutionState.RUNNING, vertex2.getCurrentExecutionAttempt().getState());
		assertEquals(1, vertex1.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, vertex2.getCurrentExecutionAttempt().getAttemptNumber());
		assertEquals(1, vertex1.getCopyOfPriorExecutionsList().size());
		assertEquals(1, vertex2.getCopyOfPriorExecutionsList().size());

		// make sure all slots are in use
		assertEquals(0, slotProvider.getNumberOfAvailableSlots());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createSampleGraph(
			JobID jid,
			Factory failoverStrategy,
			SlotProvider slotProvider,
			int parallelism) throws Exception {

		return createSampleGraph(jid, failoverStrategy, new NoRestartStrategy(), slotProvider, parallelism);
	}

	private ExecutionGraph createSampleGraph(
			JobID jid,
			Factory failoverStrategy,
			RestartStrategy restartStrategy,
			SlotProvider slotProvider,
			int parallelism) throws Exception {

		// build a simple execution graph with on job vertex, parallelism 2
		final ExecutionGraph graph = new ExecutionGraph(
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				jid,
				"test job",
				new Configuration(),
				new SerializedValue<>(new ExecutionConfig()),
				Time.seconds(10),
				restartStrategy,
				failoverStrategy,
				Collections.emptyList(),
				Collections.emptyList(),
				slotProvider,
				getClass().getClassLoader(),
				null);

		JobVertex jv = new JobVertex("test vertex");
		jv.setInvokableClass(NoOpInvokable.class);
		jv.setParallelism(parallelism);

		JobGraph jg = new JobGraph(jid, "testjob", jv);
		graph.attachJobGraph(jg.getVerticesSortedTopologicallyFromSources());

		return graph;
	}

	// ------------------------------------------------------------------------

	private static class IndividualFailoverWithCustomExecutor implements Factory {

		private final Executor executor;

		IndividualFailoverWithCustomExecutor(Executor executor) {
			this.executor = executor;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartIndividualStrategy(executionGraph, executor);
		}
	}

}
