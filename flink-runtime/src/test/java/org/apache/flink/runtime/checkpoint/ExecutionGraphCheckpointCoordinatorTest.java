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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
		final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
		CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

		final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
		CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.failGlobal(new Exception("Test Exception"));

		assertThat(checkpointCoordinator.isShutdown(), is(true));
		assertThat(counterShutdownFuture.get(), is(JobStatus.FAILED));
		assertThat(storeShutdownFuture.get(), is(JobStatus.FAILED));
	}

	/**
	 * Tests that the checkpoint coordinator is shut down if the execution graph
	 * is suspended.
	 */
	@Test
	public void testShutdownCheckpointCoordinatorOnSuspend() throws Exception {
		final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
		CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

		final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
		CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(counter, store);
		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.suspend(new Exception("Test Exception"));

		assertThat(checkpointCoordinator.isShutdown(), is(true));
		assertThat(counterShutdownFuture.get(), is(JobStatus.SUSPENDED));
		assertThat(storeShutdownFuture.get(), is(JobStatus.SUSPENDED));
	}

	/**
	 * Tests that the checkpoint coordinator is shut down if the execution graph
	 * is finished.
	 */
	@Test
	public void testShutdownCheckpointCoordinatorOnFinished() throws Exception {
		final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
		CheckpointIDCounter counter = new TestingCheckpointIDCounter(counterShutdownFuture);

		final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
		CompletedCheckpointStore store = new TestingCompletedCheckpointStore(storeShutdownFuture);

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
		assertThat(counterShutdownFuture.get(), is(JobStatus.FINISHED));
		assertThat(storeShutdownFuture.get(), is(JobStatus.FINISHED));
	}

	/**
	 * The case is designed to check the race condition between {@link ExecutionGraph} and
	 * {@link CheckpointCoordinator}. There should be no checkpoint accepted after
	 * {@link CheckpointFailureManager} decides to fail the {@link ExecutionGraph}.
	 */
	@Test
	public void testNoCheckpointAcceptedWhileFailingExecutionGraph() throws Exception {
		CheckpointIDCounter counter = new TestingCheckpointIDCounter(new CompletableFuture<>());

		TestingCompletedCheckpointStore store = new TestingCompletedCheckpointStore(new CompletableFuture<>());

		final ManuallyTriggeredScheduledExecutor mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
		ExecutionGraph graph = createExecutionGraphAndEnableCheckpointing(
			counter,
			store,
			new ComponentMainThreadExecutorServiceAdapter(mainThreadExecutor, Thread.currentThread()));

		final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

		assertThat(checkpointCoordinator, Matchers.notNullValue());
		assertThat(checkpointCoordinator.isShutdown(), is(false));

		graph.scheduleForExecution();
		ExecutionGraphTestUtils.switchToRunning(graph);

		// trigger a normal checkpoint which we will fail/decline later
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), true);
		mainThreadExecutor.triggerAll();
		final long checkpointId = checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();

		// trigger a forced checkpoint which could avoid failing the pre-checking
		// this checkpoint would be acknowledged after the first one declined
		checkpointCoordinator.triggerCheckpoint(
			System.currentTimeMillis(),
			new CheckpointProperties(
				true,
				CheckpointType.CHECKPOINT,
				true,
				true,
				true,
				true,
				true),
			null,
			false,
			false);
		mainThreadExecutor.triggerAll();

		// now we decline the first checkpoint and acknowledge the second checkpoint
		// the first declined message should fail the execution graph and abort all pending checkpoints
		// so the second acknowledged checkpoint should be abandoned when the ack arrived
		final ExecutionAttemptID attemptId =
			graph.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt().getAttemptId();
		final DeclineCheckpoint decline = new DeclineCheckpoint(graph.getJobID(), attemptId, checkpointId);
		final AcknowledgeCheckpoint ack = new AcknowledgeCheckpoint(graph.getJobID(), attemptId, checkpointId + 1);
		checkpointCoordinator.receiveDeclineMessage(decline, "localhost");
		checkpointCoordinator.receiveAcknowledgeMessage(ack, "localhost");

		mainThreadExecutor.triggerAll();

		assertEquals(0, store.completedCheckpoints.size());
	}

	private ExecutionGraph createExecutionGraphAndEnableCheckpointing(
		CheckpointIDCounter counter,
		CompletedCheckpointStore store) throws Exception {

		return createExecutionGraphAndEnableCheckpointing(
			counter,
			store,
			ComponentMainThreadExecutorServiceAdapter.forMainThread());
	}

	private ExecutionGraph createExecutionGraphAndEnableCheckpointing(
			CheckpointIDCounter counter,
			CompletedCheckpointStore store,
			ComponentMainThreadExecutor mainThreadExecutor) throws Exception {
		final Time timeout = Time.days(1L);

		JobVertex jobVertex = new JobVertex("MockVertex");
		jobVertex.setInvokableClass(AbstractInvokable.class);

		final ExecutionGraph executionGraph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(new JobGraph(jobVertex))
			.setRpcTimeout(timeout)
			.setAllocationTimeout(timeout)
			.setIoExecutor(Executors.directExecutor())
			.build();

		final ExecutionJobVertex executionJobVertex = executionGraph.getVerticesTopologically().iterator().next();

		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			1000000,
			1000000,
			100,
			1,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);

		executionGraph.enableCheckpointing(
				chkConfig,
				Collections.singletonList(executionJobVertex),
				Collections.singletonList(executionJobVertex),
				Collections.singletonList(executionJobVertex),
				Collections.emptyList(),
				counter,
				store,
				new MemoryStateBackend(),
				CheckpointStatsTrackerTest.createTestTracker());

		executionGraph.start(mainThreadExecutor);

		return executionGraph;
	}

	private static final class TestingCheckpointIDCounter implements CheckpointIDCounter {

		private final AtomicLong counter = new AtomicLong(0);

		private final CompletableFuture<JobStatus> shutdownStatus;

		private TestingCheckpointIDCounter(CompletableFuture<JobStatus> shutdownStatus) {
			this.shutdownStatus = shutdownStatus;
		}

		@Override
		public void start() {}

		@Override
		public void shutdown(JobStatus jobStatus) {
			shutdownStatus.complete(jobStatus);
		}

		@Override
		public long getAndIncrement() {
			return counter.getAndIncrement();
		}

		@Override
		public long get() {
			return counter.get();
		}

		@Override
		public void setCount(long newId) {
			counter.set(newId);
		}
	}

	private static final class TestingCompletedCheckpointStore implements CompletedCheckpointStore {

		private final CompletableFuture<JobStatus> shutdownStatus;

		private final List<CompletedCheckpoint> completedCheckpoints = new ArrayList<>();

		private TestingCompletedCheckpointStore(CompletableFuture<JobStatus> shutdownStatus) {
			this.shutdownStatus = shutdownStatus;
		}

		@Override
		public void recover() {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public void addCheckpoint(CompletedCheckpoint checkpoint) {
			completedCheckpoints.add(checkpoint);
		}

		@Override
		public void shutdown(JobStatus jobStatus) {
			shutdownStatus.complete(jobStatus);
		}

		@Override
		public List<CompletedCheckpoint> getAllCheckpoints() {
			return completedCheckpoints;
		}

		@Override
		public int getNumberOfRetainedCheckpoints() {
			return completedCheckpoints.size();
		}

		@Override
		public int getMaxNumberOfRetainedCheckpoints() {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public boolean requiresExternalizedCheckpoints() {
			throw new UnsupportedOperationException("Not implemented.");
		}
	}
}
