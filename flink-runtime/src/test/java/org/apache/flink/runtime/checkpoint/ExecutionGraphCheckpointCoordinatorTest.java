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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingSlotProvider;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the interaction between the {@link ExecutionGraph} and the {@link CheckpointCoordinator}.
 */
public class ExecutionGraphCheckpointCoordinatorTest extends TestLogger {

	/**
	 * Tests that the checkpoint coordinator is shut down if the execution graph
	 * is failed.
	 */
	@Test
	public void testShutdownCheckpointCoordinatorOnFailure() throws Exception {
		CheckpointIDCounter counter = mock(CheckpointIDCounter.class);
		CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.failGlobal(new Exception("Test Exception"));

		assertThat(checkpointCoordinator.isShutdown(), is(true));
		verify(counter, times(1)).shutdown(JobStatus.FAILED);
		verify(store, times(1)).shutdown(eq(JobStatus.FAILED));
	}

	/**
	 * Tests that the checkpoint coordinator is shut down if the execution graph
	 * is suspended.
	 */
	@Test
	public void testShutdownCheckpointCoordinatorOnSuspend() throws Exception {
		CheckpointIDCounter counter = mock(CheckpointIDCounter.class);
		CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.suspend(new Exception("Test Exception"));

		assertThat(checkpointCoordinator.isShutdown(), is(true));
		verify(counter, times(1)).shutdown(eq(JobStatus.SUSPENDED));
		verify(store, times(1)).shutdown(eq(JobStatus.SUSPENDED));
	}

	/**
	 * Tests that the checkpoint coordinator is shut down if the execution graph
	 * is finished.
	 */
	@Test
	public void testShutdownCheckpointCoordinatorOnFinished() throws Exception {
		CheckpointIDCounter counter = mock(CheckpointIDCounter.class);
		CompletedCheckpointStore store = mock(CompletedCheckpointStore.class);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.scheduleForExecution();

		for (ExecutionVertex executionVertex : graph.getAllExecutionVertices()) {
			final Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();
			graph.updateState(new TaskExecutionState(graph.getJobID(), currentExecutionAttempt.getAttemptId(), ExecutionState.FINISHED));
		}

		assertThat(graph.getTerminationFuture().get(), is(JobStatus.FINISHED));

		assertThat(checkpointCoordinator.isShutdown(), is(true));
		verify(counter, times(1)).shutdown(eq(JobStatus.FINISHED));
		verify(store, times(1)).shutdown(eq(JobStatus.FINISHED));
	}

	private ExecutionGraph createExecutionGraphAndEnableCheckpointing(
			CheckpointIDCounter counter,
			CompletedCheckpointStore store) throws Exception {
		final Time timeout = Time.days(1L);
		ExecutionGraph executionGraph = new ExecutionGraph(
			new DummyJobInformation(),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			timeout,
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			new TestingSlotProvider(slotRequestId -> CompletableFuture.completedFuture(new TestingLogicalSlot())),
			ClassLoader.getSystemClassLoader(),
			VoidBlobWriter.getInstance(),
			timeout);

		executionGraph.enableCheckpointing(
				100,
				100,
				100,
				1,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				Collections.emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				Collections.emptyList(),
				counter,
				store,
				new MemoryStateBackend(),
				CheckpointStatsTrackerTest.createTestTracker());

		JobVertex jobVertex = new JobVertex("MockVertex");
		jobVertex.setInvokableClass(AbstractInvokable.class);
		executionGraph.attachJobGraph(Collections.singletonList(jobVertex));
		executionGraph.setQueuedSchedulingAllowed(true);

		return executionGraph;
	}
}
