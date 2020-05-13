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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestingCheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.apache.flink.core.testutils.FlinkMatchers.futureWillCompleteExceptionally;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the integration of the {@link OperatorCoordinator} with the scheduler, to ensure the relevant
 * actions are leading to the right method invocations on the coordinator.
 */
@SuppressWarnings("serial")
public class OperatorCoordinatorSchedulerTest extends TestLogger {

	private final JobVertexID testVertexId = new JobVertexID();
	private final OperatorID testOperatorId = new OperatorID();

	private final ManuallyTriggeredScheduledExecutorService executor = new ManuallyTriggeredScheduledExecutorService();

	private DefaultScheduler createdScheduler;

	@After
	public void shutdownScheduler() throws Exception{
		if (createdScheduler != null) {
			createdScheduler.suspend(new Exception("shutdown"));
		}
	}

	// ------------------------------------------------------------------------
	//  tests for scheduling
	// ------------------------------------------------------------------------

	@Test
	public void testCoordinatorStartedWhenSchedulerStarts() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		assertTrue(coordinator.isStarted());
	}

	@Test
	public void testCoordinatorDisposedWhenSchedulerStops() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		scheduler.suspend(new Exception("test suspend"));

		assertTrue(coordinator.isClosed());
	}

	@Test
	public void testFailureToStartPropagatesExceptions() throws Exception {
		final OperatorCoordinator.Provider failingCoordinatorProvider =
			new TestingOperatorCoordinator.Provider(testOperatorId, CoordinatorThatFailsInStart::new);
		final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);

		try {
			scheduler.startScheduling();
			fail("expected an exception");
		} catch (Exception ignored) {
			// expected
		}
	}

	@Test
	public void testFailureToStartClosesCoordinator() throws Exception {
		final OperatorCoordinator.Provider failingCoordinatorProvider =
				new TestingOperatorCoordinator.Provider(testOperatorId, CoordinatorThatFailsInStart::new);
		final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		try {
			scheduler.startScheduling();
		} catch (Exception ignored) {}

		assertTrue(coordinator.isClosed());
	}

	@Test
	public void deployingTaskFailureNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failTask(scheduler, 1);

		assertEquals(1, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(1));
		assertThat(coordinator.getFailedTasks(), not(contains(0)));
	}

	@Test
	public void runningTaskFailureNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failTask(scheduler, 1);

		assertEquals(1, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(1));
		assertThat(coordinator.getFailedTasks(), not(contains(0)));
	}

	@Test
	public void taskRepeatedFailureNotifyCoordinator() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failAndRestartTask(scheduler, 0);
		failAndRestartTask(scheduler, 0);

		assertEquals(2, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(0, 0));
	}

	@Test
	public void taskExceptionWhenTasksNotRunning() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final OperatorCoordinator.Context context = getCoordinator(scheduler).getContext();

		final CompletableFuture<?> result = context.sendEvent(new TestOperatorEvent(), 0);

		assertThat(result, futureFailedWith(TaskNotRunningException.class));
	}

	@Test
	public void taskTaskManagerFailuresAreReportedBack() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks(new FailingTaskExecutorOperatorEventGateway());

		final OperatorCoordinator.Context context = getCoordinator(scheduler).getContext();
		final CompletableFuture<?> result = context.sendEvent(new TestOperatorEvent(), 0);

		assertThat(result, futureFailedWith(TestException.class));
	}

	// ------------------------------------------------------------------------
	//  tests for checkpointing
	// ------------------------------------------------------------------------

	@Test
	public void testTakeCheckpoint() throws Exception {
		final byte[] checkpointData = new byte[656];
		new Random().nextBytes(checkpointData);

		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final CompletableFuture<CompletedCheckpoint> checkpointFuture = triggerCheckpoint(scheduler);
		coordinator.getLastTriggeredCheckpoint().complete(checkpointData);
		acknowledgeCurrentCheckpoint(scheduler);

		final OperatorState state = checkpointFuture.get().getOperatorStates().get(testOperatorId);
		assertArrayEquals(checkpointData, getStateHandleContents(state.getCoordinatorState()));
	}

	@Test
	public void testSnapshotSyncFailureFailsCheckpoint() throws Exception {
		final OperatorCoordinator.Provider failingCoordinatorProvider =
			new TestingOperatorCoordinator.Provider(testOperatorId, CoordinatorThatFailsCheckpointing::new);
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks(failingCoordinatorProvider);

		final CompletableFuture<?> checkpointFuture = triggerCheckpoint(scheduler);

		assertThat(checkpointFuture, futureWillCompleteWithTestException());
	}

	@Test
	public void testSnapshotAsyncFailureFailsCheckpoint() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final CompletableFuture<?> checkpointFuture = triggerCheckpoint(scheduler);
		final CompletableFuture<?> coordinatorStateFuture = coordinator.getLastTriggeredCheckpoint();

		coordinatorStateFuture.completeExceptionally(new TestException());

		assertThat(checkpointFuture, futureWillCompleteWithTestException());
	}

	@Test
	public void testSavepointRestoresCoordinator() throws Exception {
		final byte[] testCoordinatorState = new byte[123];
		new Random().nextBytes(testCoordinatorState);

		final DefaultScheduler scheduler = createSchedulerWithRestoredSavepoint(testCoordinatorState);
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final byte[] restoredState = coordinator.getLastRestoredCheckpointState();
		assertArrayEquals(testCoordinatorState, restoredState);
	}

	@Test
	public void testGlobalFailureResetsToCheckpoint() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final byte[] coordinatorState = new byte[] {7, 11, 3, 5};
		takeCompleteCheckpoint(scheduler, coordinator, coordinatorState);
		failGlobalAndRestart(scheduler, new TestException());

		assertArrayEquals("coordinator should have a restored checkpoint",
				coordinatorState, coordinator.getLastRestoredCheckpointState());
	}

	@Test
	public void testLocalFailureDoesNotResetToCheckpoint() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		takeCompleteCheckpoint(scheduler, coordinator, new byte[] {37, 11, 83, 4});
		failAndRestartTask(scheduler, 0);

		assertNull("coordinator should not have a restored checkpoint", coordinator.getLastRestoredCheckpointState());
	}

	@Test
	public void testConfirmCheckpointComplete() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final long checkpointId = takeCompleteCheckpoint(scheduler, coordinator, new byte[] {37, 11, 83, 4});

		assertEquals("coordinator should be notified of completed checkpoint",
				checkpointId, coordinator.getLastCheckpointComplete());
	}

	// ------------------------------------------------------------------------
	//  test setups
	// ------------------------------------------------------------------------

	private DefaultScheduler createScheduler(OperatorCoordinator.Provider provider) throws Exception {
		return setupTestJobAndScheduler(provider);
	}

	private DefaultScheduler createAndStartScheduler() throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId));
		scheduler.startScheduling();
		executor.triggerAll();

		// guard test assumptions: this brings tasks into DEPLOYING state
		assertEquals(ExecutionState.DEPLOYING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));

		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks() throws Exception {
		return createSchedulerAndDeployTasks(new TestingOperatorCoordinator.Provider(testOperatorId));
	}

	private DefaultScheduler createSchedulerAndDeployTasks(OperatorCoordinator.Provider provider) throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(provider);
		scheduler.startScheduling();
		executor.triggerAll();
		executor.triggerScheduledTasks();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);

		// guard test assumptions: this brings tasks into RUNNING state
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));

		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks(TaskExecutorOperatorEventGateway gateway) throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), gateway, null);
		scheduler.startScheduling();
		executor.triggerAll();
		executor.triggerScheduledTasks();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);

		// guard test assumptions: this brings tasks into RUNNING state
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));

		return scheduler;
	}

	private DefaultScheduler createSchedulerWithRestoredSavepoint(byte[] coordinatorState) throws Exception {
		final byte[] savepointMetadata = serializeAsCheckpointMetadata(testOperatorId, coordinatorState);
		final String savepointPointer = "testingSavepointPointer";

		final TestingCheckpointStorageCoordinatorView storage = new TestingCheckpointStorageCoordinatorView();
		storage.registerSavepoint(savepointPointer, savepointMetadata);

		final Consumer<JobGraph> savepointConfigurer = (jobGraph) -> {
			SchedulerTestingUtils.enableCheckpointing(jobGraph, storage.asStateBackend());
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPointer));
		};

		final DefaultScheduler scheduler = setupTestJobAndScheduler(
				new TestingOperatorCoordinator.Provider(testOperatorId),
				null,
				savepointConfigurer);

		scheduler.startScheduling();
		return scheduler;
	}

	private DefaultScheduler setupTestJobAndScheduler(OperatorCoordinator.Provider provider) throws Exception {
		return setupTestJobAndScheduler(provider, null, null);
	}

	private DefaultScheduler setupTestJobAndScheduler(
			OperatorCoordinator.Provider provider,
			@Nullable TaskExecutorOperatorEventGateway taskExecutorOperatorEventGateway,
			@Nullable Consumer<JobGraph> jobGraphPreProcessing) throws Exception {

		final OperatorIDPair opIds = OperatorIDPair.of(new OperatorID(), provider.getOperatorId());
		final JobVertex vertex = new JobVertex("Vertex with OperatorCoordinator", testVertexId, Collections.singletonList(opIds));
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.addOperatorCoordinator(new SerializedValue<>(provider));
		vertex.setParallelism(2);

		final JobGraph jobGraph = new JobGraph("test job with OperatorCoordinator", vertex);
		SchedulerTestingUtils.enableCheckpointing(jobGraph);
		if (jobGraphPreProcessing != null) {
			jobGraphPreProcessing.accept(jobGraph);
		}

		final DefaultScheduler scheduler = taskExecutorOperatorEventGateway == null
				? SchedulerTestingUtils.createScheduler(jobGraph, executor)
				: SchedulerTestingUtils.createScheduler(jobGraph, executor, taskExecutorOperatorEventGateway);
		scheduler.setMainThreadExecutor(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		this.createdScheduler = scheduler;
		return scheduler;
	}

	private TestingOperatorCoordinator getCoordinator(DefaultScheduler scheduler) {
		final ExecutionJobVertex vertexWithCoordinator = getJobVertex(scheduler, testVertexId);
		assertNotNull("vertex for coordinator not found", vertexWithCoordinator);

		final OperatorCoordinator coordinator = vertexWithCoordinator.getOperatorCoordinator(testOperatorId);
		assertNotNull("vertex does not contain coordinator", coordinator);
		assertThat(coordinator, instanceOf(TestingOperatorCoordinator.class));

		return (TestingOperatorCoordinator) coordinator;
	}

	// ------------------------------------------------------------------------
	//  test actions
	// ------------------------------------------------------------------------

	private void failTask(DefaultScheduler scheduler, int subtask) {
		SchedulerTestingUtils.failExecution(scheduler, testVertexId, subtask);
		executor.triggerAll();

		// guard the test assumptions: This must not lead to a restart, but must keep the task in FAILED state
		assertEquals(ExecutionState.FAILED, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask));
	}

	private void failAndRedeployTask(DefaultScheduler scheduler, int subtask) {
		failTask(scheduler, subtask);
		executor.triggerScheduledTasks();

		// guard the test assumptions: This must lead to a restarting and redeploying
		assertEquals(ExecutionState.DEPLOYING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask));
	}

	private void failAndRestartTask(DefaultScheduler scheduler, int subtask) {
		failAndRedeployTask(scheduler, subtask);
		SchedulerTestingUtils.setExecutionToRunning(scheduler, testVertexId, subtask);

		// guard the test assumptions: This must bring the task back to RUNNING
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask));
	}

	private void failGlobalAndRestart(DefaultScheduler scheduler, Throwable reason) {
		scheduler.handleGlobalFailure(reason);
		SchedulerTestingUtils.setAllExecutionsToCancelled(scheduler);

		executor.triggerScheduledTasks();   // this handles the restart / redeploy
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);

		// guard the test assumptions: This must bring the tasks back to RUNNING
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));
	}

	private CompletableFuture<CompletedCheckpoint> triggerCheckpoint(DefaultScheduler scheduler) throws Exception {
		final CompletableFuture<CompletedCheckpoint> future = SchedulerTestingUtils.triggerCheckpoint(scheduler);
		executor.triggerAll();
		return future;
	}

	private void acknowledgeCurrentCheckpoint(DefaultScheduler scheduler) {
		executor.triggerAll();
		SchedulerTestingUtils.acknowledgeCurrentCheckpoint(scheduler);
		executor.triggerAll();
	}

	private long takeCompleteCheckpoint(
			DefaultScheduler scheduler,
			TestingOperatorCoordinator testingOperatorCoordinator,
			byte[] coordinatorState) throws Exception {

		final CompletableFuture<CompletedCheckpoint> checkpointFuture = triggerCheckpoint(scheduler);

		testingOperatorCoordinator.getLastTriggeredCheckpoint().complete(coordinatorState);
		acknowledgeCurrentCheckpoint(scheduler);

		// wait until checkpoint has completed
		return checkpointFuture.get().getCheckpointID();
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private static ExecutionJobVertex getJobVertex(DefaultScheduler scheduler, JobVertexID jobVertexId) {
		final ExecutionVertexID id = new ExecutionVertexID(jobVertexId, 0);
		return scheduler.getExecutionVertex(id).getJobVertex();
	}

	private static OperatorState createOperatorState(OperatorID id, byte[] coordinatorState) {
		final OperatorState state = new OperatorState(id, 10, 16384);
		state.setCoordinatorState(new ByteStreamStateHandle("name", coordinatorState));
		return state;
	}

	private static byte[] serializeAsCheckpointMetadata(OperatorID id, byte[] coordinatorState) throws IOException {
		final OperatorState state = createOperatorState(id, coordinatorState);
		final CheckpointMetadata metadata = new CheckpointMetadata(
			1337L, Collections.singletonList(state), Collections.emptyList());

		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		Checkpoints.storeCheckpointMetadata(metadata, out);
		return out.toByteArray();
	}

	private static <T> Matcher<CompletableFuture<T>> futureWillCompleteWithTestException() {
		return futureWillCompleteExceptionally(
				(e) -> ExceptionUtils.findThrowableSerializedAware(
						e, TestException.class, OperatorCoordinatorSchedulerTest.class.getClassLoader()).isPresent(),
				Duration.ofSeconds(10),
				"A TestException in the cause chain");
	}

	private static byte[] getStateHandleContents(StreamStateHandle stateHandle) {
		if (stateHandle instanceof ByteStreamStateHandle) {
			return ((ByteStreamStateHandle) stateHandle).getData();
		}
		fail("other state handles not implemented");
		return null;
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static final class TestOperatorEvent implements OperatorEvent {}

	private static final class TestException extends Exception {}

	private static final class CoordinatorThatFailsInStart extends TestingOperatorCoordinator {

		public CoordinatorThatFailsInStart(Context context) {
			super(context);
		}

		@Override
		public void start() throws Exception {
			throw new Exception("test failure");
		}
	}

	private static final class CoordinatorThatFailsCheckpointing extends TestingOperatorCoordinator {

		public CoordinatorThatFailsCheckpointing(Context context) {
			super(context);
		}

		@Override
		public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) {
			throw new Error(new TestException());
		}
	}

	private static final class FailingTaskExecutorOperatorEventGateway implements TaskExecutorOperatorEventGateway {

		@Override
		public CompletableFuture<Acknowledge> sendOperatorEventToTask(
				ExecutionAttemptID task,
				OperatorID operator,
				SerializedValue<OperatorEvent> evt) {
			return FutureUtils.completedExceptionally(new TestException());
		}
	}
}
