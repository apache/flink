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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * A collection of utility methods for testing the ExecutionGraph and its related classes.
 */
public class ExecutionGraphTestUtils {

	private static final Time DEFAULT_TIMEOUT = AkkaUtils.getDefaultTimeout();

	// ------------------------------------------------------------------------
	//  reaching states
	// ------------------------------------------------------------------------

	/**
	 * Waits until the Job has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilJobStatus(ExecutionGraph eg, JobStatus status, long maxWaitMillis)
			throws TimeoutException {
		checkNotNull(eg);
		checkNotNull(status);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (eg.getState() != status && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException(
				String.format("The job did not reach status %s in time. Current status is %s.",
					status, eg.getState()));
		}
	}

	/**
	 * Waits until the Execution has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilExecutionState(Execution execution, ExecutionState state, long maxWaitMillis)
			throws TimeoutException {
		checkNotNull(execution);
		checkNotNull(state);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (execution.getState() != state && System.nanoTime() < deadline) {
			try {
				Thread.sleep(2);
			} catch (InterruptedException ignored) {}
		}

		if (System.nanoTime() >= deadline) {
			throw new TimeoutException(
				String.format("The execution did not reach state %s in time. Current state is %s.",
					state, execution.getState()));
		}
	}

	/**
	 * Waits until the ExecutionVertex has reached a certain state.
	 *
	 * <p>This method is based on polling and might miss very fast state transitions!
	 */
	public static void waitUntilExecutionVertexState(ExecutionVertex executionVertex, ExecutionState state, long maxWaitMillis)
		throws TimeoutException {
		checkNotNull(executionVertex);
		checkNotNull(state);
		checkArgument(maxWaitMillis >= 0);

		// this is a poor implementation - we may want to improve it eventually
		final long deadline = maxWaitMillis == 0 ? Long.MAX_VALUE : System.nanoTime() + (maxWaitMillis * 1_000_000);

		while (true) {
			Execution execution = executionVertex.getCurrentExecutionAttempt();

			if (execution == null || (execution.getState() != state && System.nanoTime() < deadline)) {
				try {
					Thread.sleep(2);
				} catch (InterruptedException ignored) { }
			} else {
				break;
			}

			if (System.nanoTime() >= deadline) {
				if (execution != null) {
					throw new TimeoutException(
						String.format("The execution vertex did not reach state %s in time. Current state is %s.",
							state, execution.getState()));
				} else {
					throw new TimeoutException(
						"Cannot get current execution attempt of " + executionVertex + '.');
				}
			}
		}
	}

	/**
	 * Waits until all executions fulfill the given predicate.
	 *
	 * @param executionGraph for which to check the executions
	 * @param executionPredicate predicate which is to be fulfilled
	 * @param maxWaitMillis timeout for the wait operation
	 * @throws TimeoutException if the executions did not reach the target state in time
	 */
	public static void waitForAllExecutionsPredicate(
			ExecutionGraph executionGraph,
			Predicate<AccessExecution> executionPredicate,
			long maxWaitMillis) throws TimeoutException {
		final Predicate<AccessExecutionGraph> allExecutionsPredicate = allExecutionsPredicate(executionPredicate);
		final Deadline deadline = Deadline.fromNow(Duration.ofMillis(maxWaitMillis));
		boolean predicateResult;

		do {
			predicateResult = allExecutionsPredicate.test(executionGraph);

			if (!predicateResult) {
				try {
					Thread.sleep(2L);
				} catch (InterruptedException ignored) {
					Thread.currentThread().interrupt();
				}
			}
		} while (!predicateResult && deadline.hasTimeLeft());

		if (!predicateResult) {
			throw new TimeoutException("Not all executions fulfilled the predicate in time.");
		}
	}

	public static Predicate<AccessExecutionGraph> allExecutionsPredicate(final Predicate<AccessExecution> executionPredicate) {
		return accessExecutionGraph -> {
			final Iterable<? extends AccessExecutionVertex> allExecutionVertices = accessExecutionGraph.getAllExecutionVertices();

			for (AccessExecutionVertex executionVertex : allExecutionVertices) {
				final AccessExecution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

				if (currentExecutionAttempt == null || !executionPredicate.test(currentExecutionAttempt)) {
					return false;
				}
			}

			return true;
		};
	}

	public static Predicate<AccessExecution> isInExecutionState(ExecutionState executionState) {
		return (AccessExecution execution) -> execution.getState() == executionState;
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and switches their current
	 * execution to RUNNING.
	 */
	public static void switchAllVerticesToRunning(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().switchToRunning();
		}
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and attempts to move them
	 * from CANCELING to CANCELED.
	 */
	public static void completeCancellingForAllVertices(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().completeCancelling();
		}
	}

	/**
	 * Takes all vertices in the given ExecutionGraph and switches their current
	 * execution to FINISHED.
	 */
	public static void finishAllVertices(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().markFinished();
		}
	}

	/**
	 * Checks that all execution are in state DEPLOYING and then switches them
	 * to state RUNNING.
	 */
	public static void switchToRunning(ExecutionGraph eg) {
		// check that all execution are in state DEPLOYING
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			final Execution exec = ev.getCurrentExecutionAttempt();
			final ExecutionState executionState = exec.getState();
			assert executionState == ExecutionState.DEPLOYING : "Expected executionState to be DEPLOYING, was: " + executionState;
		}

		// switch executions to RUNNING
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			final Execution exec = ev.getCurrentExecutionAttempt();
			exec.switchToRunning();
		}
	}

	// ------------------------------------------------------------------------
	//  state modifications
	// ------------------------------------------------------------------------

	public static void setVertexState(ExecutionVertex vertex, ExecutionState state) {
		try {
			Execution exec = vertex.getCurrentExecutionAttempt();

			Field f = Execution.class.getDeclaredField("state");
			f.setAccessible(true);
			f.set(exec, state);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the state failed", e);
		}
	}

	public static void setVertexResource(ExecutionVertex vertex, LogicalSlot slot) {
		Execution exec = vertex.getCurrentExecutionAttempt();

		if (!exec.tryAssignResource(slot)) {
			throw new RuntimeException("Could not assign resource.");
		}
	}

	// ------------------------------------------------------------------------
	//  Mocking ExecutionGraph
	// ------------------------------------------------------------------------

	/**
	 * Creates an execution graph with on job vertex of parallelism 10 that does no restarts.
	 */
	public static ExecutionGraph createSimpleTestGraph() throws Exception {
		return createSimpleTestGraph(new NoRestartStrategy());
	}

	/**
	 * Creates an execution graph with on job vertex of parallelism 10, using the given
	 * restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(RestartStrategy restartStrategy) throws Exception {
		JobVertex vertex = createNoOpVertex(10);

		return createSimpleTestGraph(new SimpleAckingTaskManagerGateway(), restartStrategy, vertex);
	}

	/**
	 * Creates an execution graph containing the given vertices.
	 *
	 * <p>The execution graph uses {@link NoRestartStrategy} as the restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(JobVertex... vertices) throws Exception {
		return createSimpleTestGraph(new SimpleAckingTaskManagerGateway(), new NoRestartStrategy(), vertices);
	}

	/**
	 * Creates an execution graph containing the given vertices and the given restart strategy.
	 */
	public static ExecutionGraph createSimpleTestGraph(
			TaskManagerGateway taskManagerGateway,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		int numSlotsNeeded = 0;
		for (JobVertex vertex : vertices) {
			numSlotsNeeded += vertex.getParallelism();
		}

		SlotProvider slotProvider = new SimpleSlotProvider(numSlotsNeeded, taskManagerGateway);

		return createSimpleTestGraph(slotProvider, restartStrategy, vertices);
	}

	public static ExecutionGraph createSimpleTestGraph(
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			JobVertex... vertices) throws Exception {

		return createExecutionGraph(slotProvider, restartStrategy, TestingUtils.defaultExecutor(), vertices);
	}

	public static ExecutionGraph createExecutionGraph(
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			ScheduledExecutorService executor,
			JobVertex... vertices) throws Exception {

			return createExecutionGraph(slotProvider, restartStrategy, executor, Time.seconds(10L), vertices);
	}

	public static ExecutionGraph createExecutionGraph(
			SlotProvider slotProvider,
			RestartStrategy restartStrategy,
			ScheduledExecutorService executor,
			Time timeout,
			JobVertex... vertices) throws Exception {

		checkNotNull(restartStrategy);
		checkNotNull(vertices);
		checkNotNull(timeout);

		return TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(new JobGraph(vertices))
			.setFutureExecutor(executor)
			.setIoExecutor(executor)
			.setSlotProvider(slotProvider)
			.setAllocationTimeout(timeout)
			.setRpcTimeout(timeout)
			.setRestartStrategy(restartStrategy)
			.build();
	}

	public static JobVertex createNoOpVertex(int parallelism) {
		return createNoOpVertex("vertex", parallelism);
	}

	public static JobVertex createNoOpVertex(String name, int parallelism) {
		JobVertex vertex = new JobVertex(name);
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(parallelism);
		return vertex;
	}

	// ------------------------------------------------------------------------
	//  utility mocking methods
	// ------------------------------------------------------------------------

	public static ExecutionVertexID createRandomExecutionVertexId() {
		return new ExecutionVertexID(new JobVertexID(), new Random().nextInt(Integer.MAX_VALUE));
	}

	public static JobVertex createJobVertex(String task1, int numTasks, Class<NoOpInvokable> invokable) {
		JobVertex groupVertex = new JobVertex(task1);
		groupVertex.setInvokableClass(invokable);
		groupVertex.setParallelism(numTasks);
		return groupVertex;
	}

	public static ExecutionJobVertex getExecutionJobVertex(
			JobVertexID id,
			ScheduledExecutorService executor) throws Exception {
		return getExecutionJobVertex(id, executor, ScheduleMode.LAZY_FROM_SOURCES);
	}

	public static ExecutionJobVertex getExecutionJobVertex(
			JobVertexID id,
			ScheduledExecutorService executor,
			ScheduleMode scheduleMode) throws Exception {

		return getExecutionJobVertex(id, 1, null, executor, scheduleMode);
	}

	public static ExecutionJobVertex getExecutionJobVertex(
			JobVertexID id,
			int parallelism,
			@Nullable SlotSharingGroup slotSharingGroup,
			ScheduledExecutorService executor,
			ScheduleMode scheduleMode) throws Exception {

		JobVertex ajv = new JobVertex("TestVertex", id);
		ajv.setInvokableClass(AbstractInvokable.class);
		ajv.setParallelism(parallelism);
		if (slotSharingGroup != null) {
			ajv.setSlotSharingGroup(slotSharingGroup);
		}

		return getExecutionJobVertex(ajv, executor, scheduleMode);
	}

	public static ExecutionJobVertex getExecutionJobVertex(JobVertex jobVertex) throws Exception {
		return getExecutionJobVertex(jobVertex, new DirectScheduledExecutorService(), ScheduleMode.LAZY_FROM_SOURCES);
	}

	public static ExecutionJobVertex getExecutionJobVertex(
			JobVertex jobVertex,
			ScheduledExecutorService executor,
			ScheduleMode scheduleMode) throws Exception {

		JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.setScheduleMode(scheduleMode);

		ExecutionGraph graph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jobGraph)
			.setIoExecutor(executor)
			.setFutureExecutor(executor)
			.build();

		graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		return graph.getJobVertex(jobVertex.getID());
	}

	public static ExecutionJobVertex getExecutionJobVertex(JobVertexID id) throws Exception {
		return getExecutionJobVertex(id, new DirectScheduledExecutorService());
	}

	public static ExecutionVertex getExecutionVertex() throws Exception {
		return getExecutionJobVertex(new JobVertexID(), new DirectScheduledExecutorService()).getTaskVertices()[0];
	}

	public static Execution getExecution() throws Exception {
		final ExecutionJobVertex ejv = getExecutionJobVertex(new JobVertexID());
		return ejv.getTaskVertices()[0].getCurrentExecutionAttempt();
	}

	public static Execution getExecution(
			final JobVertexID jid,
			final int subtaskIndex,
			final int numTasks,
			final SlotSharingGroup slotSharingGroup) throws Exception {

		final ExecutionJobVertex ejv = getExecutionJobVertex(
			jid,
			numTasks,
			slotSharingGroup,
			new DirectScheduledExecutorService(),
			ScheduleMode.LAZY_FROM_SOURCES);

		return ejv.getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
	}

	// ------------------------------------------------------------------------
	//  graph vertex verifications
	// ------------------------------------------------------------------------

	/**
	 * Verifies the generated {@link ExecutionJobVertex} for a given {@link JobVertex} in a {@link ExecutionGraph}.
	 *
	 * @param executionGraph the generated execution graph
	 * @param originJobVertex the vertex to verify for
	 * @param inputJobVertices upstream vertices of the verified vertex, used to check inputs of generated vertex
	 * @param outputJobVertices downstream vertices of the verified vertex, used to
	 *                          check produced data sets of generated vertex
	 */
	public static void verifyGeneratedExecutionJobVertex(
			ExecutionGraph executionGraph,
			JobVertex originJobVertex,
			@Nullable List<JobVertex> inputJobVertices,
			@Nullable List<JobVertex> outputJobVertices) {

		ExecutionJobVertex ejv = executionGraph.getAllVertices().get(originJobVertex.getID());
		assertNotNull(ejv);

		// verify basic properties
		assertEquals(originJobVertex.getParallelism(), ejv.getParallelism());
		assertEquals(executionGraph.getJobID(), ejv.getJobId());
		assertEquals(originJobVertex.getID(), ejv.getJobVertexId());
		assertEquals(originJobVertex, ejv.getJobVertex());

		// verify produced data sets
		if (outputJobVertices == null) {
			assertEquals(0, ejv.getProducedDataSets().length);
		} else {
			assertEquals(outputJobVertices.size(), ejv.getProducedDataSets().length);
			for (int i = 0; i < outputJobVertices.size(); i++) {
				assertEquals(originJobVertex.getProducedDataSets().get(i).getId(), ejv.getProducedDataSets()[i].getId());
				assertEquals(originJobVertex.getParallelism(), ejv.getProducedDataSets()[0].getPartitions().length);
			}
		}

		// verify task vertices for their basic properties and their inputs
		assertEquals(originJobVertex.getParallelism(), ejv.getTaskVertices().length);

		int subtaskIndex = 0;
		for (ExecutionVertex ev : ejv.getTaskVertices()) {
			assertEquals(executionGraph.getJobID(), ev.getJobId());
			assertEquals(originJobVertex.getID(), ev.getJobvertexId());

			assertEquals(originJobVertex.getParallelism(), ev.getTotalNumberOfParallelSubtasks());
			assertEquals(subtaskIndex, ev.getParallelSubtaskIndex());

			if (inputJobVertices == null) {
				assertEquals(0, ev.getNumberOfInputs());
			} else {
				assertEquals(inputJobVertices.size(), ev.getNumberOfInputs());

				for (int i = 0; i < inputJobVertices.size(); i++) {
					ExecutionEdge[] inputEdges = ev.getInputEdges(i);
					assertEquals(inputJobVertices.get(i).getParallelism(), inputEdges.length);

					int expectedPartitionNum = 0;
					for (ExecutionEdge inEdge : inputEdges) {
						assertEquals(i, inEdge.getInputNum());
						assertEquals(expectedPartitionNum, inEdge.getSource().getPartitionNumber());

						expectedPartitionNum++;
					}
				}
			}

			subtaskIndex++;
		}
	}
}
