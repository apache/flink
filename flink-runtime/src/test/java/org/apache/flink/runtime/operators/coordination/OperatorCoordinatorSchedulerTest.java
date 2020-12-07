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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartAllFailoverStrategy;
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
import org.apache.flink.runtime.state.TestingCheckpointStorageAccessCoordinatorView;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.apache.flink.core.testutils.FlinkMatchers.futureWillCompleteExceptionally;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
	public void cancellationAsPartOfFailoverNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createSchedulerWithAllRestartOnFailureAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failTask(scheduler, 1);

		assertEquals(2, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), containsInAnyOrder(0, 1));
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
		executor.triggerAll(); // process event sending

		assertThat(result, futureFailedWith(TaskNotRunningException.class));
	}

	@Test
	public void taskTaskManagerFailuresAreReportedBack() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks(new FailingTaskExecutorOperatorEventGateway());

		final OperatorCoordinator.Context context = getCoordinator(scheduler).getContext();
		final CompletableFuture<?> result = context.sendEvent(new TestOperatorEvent(), 0);
		executor.triggerAll();  // process event sending

		assertThat(result, futureFailedWith(TestException.class));
	}

	// THESE TESTS BELOW SHOULD LEGITIMATELY WORK, BUT THE SCHEDULER ITSELF SEEMS TO NOT HANDLE
	// THIS SITUATION AT THE MOMENT
	// WE KEEP THESE TESTS HERE TO ENABLE THEM ONCE THE SCHEDULER'S CONTRACT SUPPORTS THEM

	@Ignore
	@Test
	public void deployingTaskCancellationNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		cancelTask(scheduler, 1);

		assertEquals(1, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(1));
		assertThat(coordinator.getFailedTasks(), not(contains(0)));
	}

	@Ignore
	@Test
	public void runningTaskCancellationNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		cancelTask(scheduler, 0);

		assertEquals(1, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(0));
		assertThat(coordinator.getFailedTasks(), not(contains(1)));
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
	public void testGlobalFailureBeforeCheckpointResetsToEmptyState() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failGlobalAndRestart(scheduler, new TestException());

		assertSame("coordinator should have null restored state",
			TestingOperatorCoordinator.NULL_RESTORE_VALUE, coordinator.getLastRestoredCheckpointState());
		assertEquals(OperatorCoordinator.NO_CHECKPOINT, coordinator.getLastRestoredCheckpointId());
	}

	@Test
	public void testGlobalFailoverDoesNotNotifyLocalRestore() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		takeCompleteCheckpoint(scheduler, coordinator, new byte[0]);
		failGlobalAndRestart(scheduler, new TestException());

		assertThat(coordinator.getRestoredTasks(), empty());
	}

	@Test
	public void testLocalFailoverResetsTask() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		final long checkpointId = takeCompleteCheckpoint(scheduler, coordinator, new byte[0]);
		failAndRestartTask(scheduler, 1);

		assertEquals(1, coordinator.getRestoredTasks().size());
		final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask = coordinator.getRestoredTasks().get(0);
		assertEquals(1, restoredTask.subtaskIndex);
		assertEquals(checkpointId, restoredTask.checkpointId);
	}

	@Test
	public void testLocalFailoverBeforeCheckpointResetsTask() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failAndRestartTask(scheduler, 1);

		assertEquals(1, coordinator.getRestoredTasks().size());
		final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask = coordinator.getRestoredTasks().get(0);
		assertEquals(1, restoredTask.subtaskIndex);
		assertEquals(OperatorCoordinator.NO_CHECKPOINT, restoredTask.checkpointId);
	}

	@Test
	public void testLocalFailoverDoesNotResetToCheckpoint() throws Exception {
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
	//  tests for failover notifications in a batch setup (no checkpoints)
	// ------------------------------------------------------------------------

	@Test
	public void testBatchGlobalFailureResetsToEmptyState() throws Exception {
		final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failGlobalAndRestart(scheduler, new TestException());

		assertSame("coordinator should have null restored state",
			TestingOperatorCoordinator.NULL_RESTORE_VALUE, coordinator.getLastRestoredCheckpointState());
		assertEquals(OperatorCoordinator.NO_CHECKPOINT, coordinator.getLastRestoredCheckpointId());
	}

	@Test
	public void testBatchGlobalFailoverDoesNotNotifyLocalRestore() throws Exception {
		final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failGlobalAndRestart(scheduler, new TestException());

		assertThat(coordinator.getRestoredTasks(), empty());
	}

	@Test
	public void testBatchLocalFailoverResetsTask() throws Exception {
		final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failAndRestartTask(scheduler, 1);

		assertEquals(1, coordinator.getRestoredTasks().size());
		final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask = coordinator.getRestoredTasks().get(0);
		assertEquals(1, restoredTask.subtaskIndex);
		assertEquals(OperatorCoordinator.NO_CHECKPOINT, restoredTask.checkpointId);
	}

	@Test
	public void testBatchLocalFailoverDoesNotResetToCheckpoint() throws Exception {
		final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failAndRestartTask(scheduler, 0);

		assertNull("coordinator should not have a restored checkpoint", coordinator.getLastRestoredCheckpointState());
	}

	// ------------------------------------------------------------------------
	//  tests for REST request delivery
	// ------------------------------------------------------------------------

	@Test
	@SuppressWarnings("unchecked")
	public void testDeliveringClientRequestToRequestHandler() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingCoordinationRequestHandler.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);
		final TestingCoordinationRequestHandler.Response<String> response =
			(TestingCoordinationRequestHandler.Response<String>)
				scheduler.deliverCoordinationRequestToCoordinator(testOperatorId, request).get();

		assertEquals(payload, response.getPayload());
	}

	@Test
	public void testDeliveringClientRequestToNonRequestHandler() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingOperatorCoordinator.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);

		CommonTestUtils.assertThrows(
			"cannot handle client event",
			FlinkException.class,
			() -> scheduler.deliverCoordinationRequestToCoordinator(testOperatorId, request));
	}

	@Test
	public void testDeliveringClientRequestToNonExistingCoordinator() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingOperatorCoordinator.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);

		CommonTestUtils.assertThrows(
			"does not exist",
			FlinkException.class,
			() -> scheduler.deliverCoordinationRequestToCoordinator(new OperatorID(), request));
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

	private DefaultScheduler createSchedulerWithAllRestartOnFailureAndDeployTasks() throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), null, null, true);
		scheduleAllTasksToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerWithoutCheckpointingAndDeployTasks() throws Exception {
		final Consumer<JobGraph> noCheckpoints = (jobGraph) -> jobGraph.setSnapshotSettings(null);
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), null, noCheckpoints, false);

		// guard test assumptions: this must set up a scheduler without checkpoints
		assertNull(scheduler.getExecutionGraph().getCheckpointCoordinator());

		scheduleAllTasksToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks(OperatorCoordinator.Provider provider) throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(provider);
		scheduleAllTasksToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks(TaskExecutorOperatorEventGateway gateway) throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), gateway, null, false);
		scheduleAllTasksToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerWithRestoredSavepoint(byte[] coordinatorState) throws Exception {
		final byte[] savepointMetadata = serializeAsCheckpointMetadata(testOperatorId, coordinatorState);
		final String savepointPointer = "testingSavepointPointer";

		final TestingCheckpointStorageAccessCoordinatorView storage = new TestingCheckpointStorageAccessCoordinatorView();
		storage.registerSavepoint(savepointPointer, savepointMetadata);

		final Consumer<JobGraph> savepointConfigurer = (jobGraph) -> {
			SchedulerTestingUtils.enableCheckpointing(jobGraph, storage.asStateBackend());
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPointer));
		};

		final DefaultScheduler scheduler = setupTestJobAndScheduler(
				new TestingOperatorCoordinator.Provider(testOperatorId),
				null,
				savepointConfigurer,
				false);

		scheduler.startScheduling();
		return scheduler;
	}

	private DefaultScheduler setupTestJobAndScheduler(OperatorCoordinator.Provider provider) throws Exception {
		return setupTestJobAndScheduler(provider, null, null, false);
	}

	private DefaultScheduler setupTestJobAndScheduler(
			OperatorCoordinator.Provider provider,
			@Nullable TaskExecutorOperatorEventGateway taskExecutorOperatorEventGateway,
			@Nullable Consumer<JobGraph> jobGraphPreProcessing,
			boolean restartAllOnFailover) throws Exception {

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

		final SchedulerTestingUtils.DefaultSchedulerBuilder schedulerBuilder = taskExecutorOperatorEventGateway == null
				? SchedulerTestingUtils.createSchedulerBuilder(jobGraph, executor)
				: SchedulerTestingUtils.createSchedulerBuilder(jobGraph, executor, taskExecutorOperatorEventGateway);
		if (restartAllOnFailover) {
			schedulerBuilder.setFailoverStrategyFactory(new RestartAllFailoverStrategy.Factory());
		}

		final DefaultScheduler scheduler = schedulerBuilder.build();

		final ComponentMainThreadExecutor mainThreadExecutor = new ComponentMainThreadExecutorServiceAdapter(
			(ScheduledExecutorService) executor, Thread.currentThread());
		scheduler.setMainThreadExecutor(mainThreadExecutor);

		this.createdScheduler = scheduler;
		return scheduler;
	}

	private void scheduleAllTasksToRunning(DefaultScheduler scheduler) {
		scheduler.startScheduling();
		executor.triggerAll();
		executor.triggerScheduledTasks();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);

		// guard test assumptions: this brings tasks into RUNNING state
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));
	}

	private TestingOperatorCoordinator getCoordinator(DefaultScheduler scheduler) {
		final ExecutionJobVertex vertexWithCoordinator = getJobVertex(scheduler, testVertexId);
		assertNotNull("vertex for coordinator not found", vertexWithCoordinator);

		final Optional<OperatorCoordinatorHolder> coordinatorOptional = vertexWithCoordinator
				.getOperatorCoordinators()
				.stream()
				.filter((holder) -> holder.operatorId().equals(testOperatorId))
				.findFirst();
		assertTrue("vertex does not contain coordinator", coordinatorOptional.isPresent());

		final OperatorCoordinator coordinator = coordinatorOptional.get().coordinator();
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

		executor.triggerAll();
		executor.triggerScheduledTasks();
		executor.triggerAll();

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

		// make sure we propagate all asynchronous and delayed actions
		executor.triggerAll();
		executor.triggerScheduledTasks();
		executor.triggerAll();

		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);
		executor.triggerAll();

		// guard the test assumptions: This must bring the tasks back to RUNNING
		assertEquals(ExecutionState.RUNNING, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0));
	}

	private void cancelTask(DefaultScheduler scheduler, int subtask) {
		SchedulerTestingUtils.canceledExecution(scheduler, testVertexId, subtask);
		executor.triggerAll();

		// guard the test assumptions: This must not lead to a restart, but must keep the task in FAILED state
		assertEquals(ExecutionState.CANCELED, SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask));
	}

	private CompletableFuture<CompletedCheckpoint> triggerCheckpoint(DefaultScheduler scheduler) throws Exception {
		final CompletableFuture<CompletedCheckpoint> future = SchedulerTestingUtils.triggerCheckpoint(scheduler);
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		// the Checkpoint Coordinator executes parts of the logic in its timer thread, and delegates some calls
		// to the scheduler executor. so we need to do a mix of waiting for the timer thread and working off
		// tasks in the scheduler executor.
		// we can drop this here once the CheckpointCoordinator also runs in a 'main thread executor'.
		while (!(coordinator.hasTriggeredCheckpoint() || future.isDone())) {
			executor.triggerAll();
			Thread.sleep(1);
		}

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
		final long checkpointId = checkpointFuture.get().getCheckpointID();

		// now wait until it has been acknowledged
		while (!testingOperatorCoordinator.hasCompleteCheckpoint()) {
			executor.triggerAll();
			Thread.sleep(1);
		}

		return checkpointId;
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
		public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
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
