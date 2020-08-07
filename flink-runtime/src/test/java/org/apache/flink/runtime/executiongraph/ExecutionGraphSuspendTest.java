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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Validates that suspending out of various states works correctly.
 */
public class ExecutionGraphSuspendTest extends TestLogger {

	/**
	 * Going into SUSPENDED out of CREATED should immediately cancel everything and
	 * not send out RPC calls.
	 */
	@Test
	public void testSuspendedOutOfCreated() throws Exception {
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway();
		final int parallelism = 10;
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		assertEquals(JobStatus.CREATED, eg.getState());

		// suspend

		eg.suspend(new Exception("suspend"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateAllVerticesInState(eg, ExecutionState.CANCELED);
		validateCancelRpcCalls(gateway, 0);

		ensureCannotLeaveSuspendedState(eg, gateway);
	}

	/**
	 * Going into SUSPENDED out of DEPLOYING vertices should cancel all vertices once with RPC calls.
	 */
	@Test
	public void testSuspendedOutOfDeploying() throws Exception {
		final int parallelism = 10;
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway(parallelism);
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, eg.getState());
		validateAllVerticesInState(eg, ExecutionState.DEPLOYING);

		// suspend
		eg.suspend(new Exception("suspend"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		ensureCannotLeaveSuspendedState(eg, gateway);
	}

	/**
	 * Going into SUSPENDED out of RUNNING vertices should cancel all vertices once with RPC calls.
	 */
	@Test
	public void testSuspendedOutOfRunning() throws Exception {
		final int parallelism = 10;
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway(parallelism);
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		assertEquals(JobStatus.RUNNING, eg.getState());
		validateAllVerticesInState(eg, ExecutionState.RUNNING);

		// suspend
		eg.suspend(new Exception("suspend"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		ensureCannotLeaveSuspendedState(eg, gateway);
	}

	/**
	 * Suspending from FAILING goes to SUSPENDED and sends no additional RPC calls.
	 */
	@Test
	public void testSuspendedOutOfFailing() throws Exception {
		final int parallelism = 10;
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway(parallelism);
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("fail global"));

		assertEquals(JobStatus.FAILING, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		// suspend
		eg.suspend(new Exception("suspend"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());
		ensureCannotLeaveSuspendedState(eg, gateway);
	}

	/**
	 * Suspending from FAILED should do nothing.
	 */
	@Test
	public void testSuspendedOutOfFailed() throws Exception {
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway();
		final int parallelism = 10;
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("fail global"));

		assertEquals(JobStatus.FAILING, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.FAILED, eg.getState());

		// suspend
		eg.suspend(new Exception("suspend"));

		// still in failed state
		assertEquals(JobStatus.FAILED, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);
	}

	/**
	 * Suspending from CANCELING goes to SUSPENDED and sends no additional RPC calls.
	 */
	@Test
	public void testSuspendedOutOfCanceling() throws Exception {
		final int parallelism = 10;
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway(parallelism);
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.cancel();

		assertEquals(JobStatus.CANCELLING, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		// suspend
		eg.suspend(new Exception("suspend"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		ensureCannotLeaveSuspendedState(eg, gateway);
	}

	/**
	 * Suspending from CANCELLED should do nothing.
	 */
	@Test
	public void testSuspendedOutOfCanceled() throws Exception {
		final InteractionsCountingTaskManagerGateway gateway = new InteractionsCountingTaskManagerGateway();
		final int parallelism = 10;
		final ExecutionGraph eg = createExecutionGraph(gateway, parallelism);

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.cancel();

		assertEquals(JobStatus.CANCELLING, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.CANCELED, eg.getTerminationFuture().get());

		// suspend
		eg.suspend(new Exception("suspend"));

		// still in failed state
		assertEquals(JobStatus.CANCELED, eg.getState());
		validateCancelRpcCalls(gateway, parallelism);
	}

	/**
	 * Tests that we can suspend a job when in state RESTARTING.
	 */
	@Test
	public void testSuspendWhileRestarting() throws Exception {
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new InfiniteDelayRestartStrategy(10));
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("test"));
		assertEquals(JobStatus.FAILING, eg.getState());

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.RESTARTING, eg.getState());

		final Exception exception = new Exception("Suspended");

		eg.suspend(exception);

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		assertEquals(exception, eg.getFailureCause());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static void ensureCannotLeaveSuspendedState(ExecutionGraph eg, InteractionsCountingTaskManagerGateway gateway) {
		gateway.waitUntilAllTasksAreSubmitted();
		assertEquals(JobStatus.SUSPENDED, eg.getState());
		gateway.resetCounts();

		eg.failGlobal(new Exception("fail"));
		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateNoInteractions(gateway);

		eg.cancel();
		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateNoInteractions(gateway);

		eg.suspend(new Exception("suspend again"));
		assertEquals(JobStatus.SUSPENDED, eg.getState());
		validateNoInteractions(gateway);

		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(0, ev.getCurrentExecutionAttempt().getAttemptNumber());
		}
	}

	private static void validateNoInteractions(InteractionsCountingTaskManagerGateway gateway) {
		assertThat(gateway.getInteractionsCount(), is(0));
	}

	private static void validateAllVerticesInState(ExecutionGraph eg, ExecutionState expected) {
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(expected, ev.getCurrentExecutionAttempt().getState());
		}
	}

	private static void validateCancelRpcCalls(InteractionsCountingTaskManagerGateway gateway, int num) {
		assertThat(gateway.getCancelTaskCount(), is(num));
	}

	private static ExecutionGraph createExecutionGraph(TaskManagerGateway gateway, int parallelism) throws Exception {
		final JobVertex vertex = new JobVertex("vertex");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(parallelism);

		final SlotProvider slotProvider = new SimpleSlotProvider(parallelism, gateway);

		ExecutionGraph simpleTestGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			slotProvider,
			new FixedDelayRestartStrategy(0, 0),
			vertex);
		simpleTestGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		return simpleTestGraph;
	}

}
