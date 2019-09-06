/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.TestingUncaughtExceptionHandler;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskLocalStateStoreImpl;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorStateContext;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.streaming.util.StreamTaskUtil.waitTaskIsRunning;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link StreamTask}.
 */
@SuppressWarnings("deprecation")
public class StreamTaskTest extends TestLogger {

	private static OneShotLatch syncLatch;

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(30);

	/**
	 * This test checks the async exceptions handling wraps the message and cause as an AsynchronousException
	 * and propagates this to the environment.
	 */
	@Test
	public void streamTaskAsyncExceptionHandler_handleException_forwardsMessageProperly() {
		MockEnvironment mockEnvironment = MockEnvironment.builder().build();
		RuntimeException expectedException = new RuntimeException("RUNTIME EXCEPTION");

		final StreamTask.StreamTaskAsyncExceptionHandler asyncExceptionHandler = new StreamTask.StreamTaskAsyncExceptionHandler(mockEnvironment);

		mockEnvironment.setExpectedExternalFailureCause(AsynchronousException.class);
		final String expectedErrorMessage = "EXPECTED_ERROR MESSAGE";

		asyncExceptionHandler.handleAsyncException(expectedErrorMessage, expectedException);

		// expect an AsynchronousException containing the supplied error details
		Optional<? extends Throwable> actualExternalFailureCause = mockEnvironment.getActualExternalFailureCause();
		final Throwable actualException = actualExternalFailureCause
			.orElseThrow(() -> new AssertionError("Expected exceptional completion"));

		assertThat(actualException, instanceOf(AsynchronousException.class));
		assertThat(actualException.getMessage(), is("EXPECTED_ERROR MESSAGE"));
		assertThat(actualException.getCause(), is(expectedException));
	}

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() throws Exception {
		final StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		cfg.setStreamOperator(new SlowlyDeserializingOperator());
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		final TaskManagerActions taskManagerActions = spy(new NoOpTaskManagerActions());
		final Task task = createTask(SourceStreamTask.class, cfg, new Configuration(), taskManagerActions);

		final TaskExecutionState state = new TaskExecutionState(
			task.getJobID(), task.getExecutionId(), ExecutionState.RUNNING);

		task.startTaskThread();

		verify(taskManagerActions, timeout(2000L)).updateTaskExecutionState(eq(state));

		// send a cancel. because the operator takes a long time to deserialize, this should
		// hit the task before the operator is deserialized
		task.cancelExecution();

		task.getExecutingThread().join();

		assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testStateBackendLoadingAndClosing() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CheckpointingOptions.STATE_BACKEND, TestMemoryStateBackendFactory.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateKeySerializer(mock(TypeSerializer.class));
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		TestStreamSource<Long, MockSourceFunction> streamSource = new TestStreamSource<>(new MockSourceFunction());
		cfg.setStreamOperator(streamSource);
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = false;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends and stream iterables are closed ...
		verify(TestStreamSource.operatorStateBackend).close();
		verify(TestStreamSource.keyedStateBackend).close();
		verify(TestStreamSource.rawOperatorStateInputs).close();
		verify(TestStreamSource.rawKeyedStateInputs).close();
		// ... and disposed
		verify(TestStreamSource.operatorStateBackend).dispose();
		verify(TestStreamSource.keyedStateBackend).dispose();

		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	@Test
	public void testStateBackendClosingOnFailure() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CheckpointingOptions.STATE_BACKEND, TestMemoryStateBackendFactory.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateKeySerializer(mock(TypeSerializer.class));
		cfg.setOperatorID(new OperatorID(4711L, 42L));
		TestStreamSource<Long, MockSourceFunction> streamSource = new TestStreamSource<>(new MockSourceFunction());
		cfg.setStreamOperator(streamSource);
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = true;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends and stream iterables are closed ...
		verify(TestStreamSource.operatorStateBackend).close();
		verify(TestStreamSource.keyedStateBackend).close();
		verify(TestStreamSource.rawOperatorStateInputs).close();
		verify(TestStreamSource.rawKeyedStateInputs).close();
		// ... and disposed
		verify(TestStreamSource.operatorStateBackend).dispose();
		verify(TestStreamSource.keyedStateBackend).dispose();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
	}

	@Test
	public void testCancellationNotBlockedOnLock() throws Exception {
		syncLatch = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		Task task = createTask(CancelLockingTask.class, cfg, new Configuration());

		// start the task and wait until it runs
		// execution state RUNNING is not enough, we need to wait until the stream task's run() method
		// is entered
		task.startTaskThread();
		syncLatch.await();

		// cancel the execution - this should lead to smooth shutdown
		task.cancelExecution();
		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testCancellationFailsWithBlockingLock() throws Exception {
		syncLatch = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		Task task = createTask(CancelFailingTask.class, cfg, new Configuration());

		// start the task and wait until it runs
		// execution state RUNNING is not enough, we need to wait until the stream task's run() method
		// is entered
		task.startTaskThread();
		syncLatch.await();

		// cancel the execution - this should lead to smooth shutdown
		task.cancelExecution();
		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testDecliningCheckpointStreamOperator() throws Exception {
		CheckpointExceptionHandlerTest.DeclineDummyEnvironment declineDummyEnvironment = new CheckpointExceptionHandlerTest.DeclineDummyEnvironment();

		// mock the returned snapshots
		OperatorSnapshotFutures operatorSnapshotResult1 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult2 = mock(OperatorSnapshotFutures.class);

		final Exception testException = new Exception("Test exception");

		RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
			declineDummyEnvironment,
			operatorChain(
				streamOperatorWithSnapshot(operatorSnapshotResult1),
				streamOperatorWithSnapshot(operatorSnapshotResult2),
				streamOperatorWithSnapshotException(testException))));
		MockStreamTask streamTask = task.streamTask;

		waitTaskIsRunning(streamTask, task.invocationFuture);

		streamTask.triggerCheckpoint(
			new CheckpointMetaData(42L, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		assertEquals(testException, declineDummyEnvironment.getLastDeclinedCheckpointCause());

		verify(operatorSnapshotResult1).cancel();
		verify(operatorSnapshotResult2).cancel();

		task.streamTask.finishInput();
		task.waitForTaskCompletion(false);
	}

	/**
	 * Tests that uncaught exceptions in the async part of a checkpoint operation are forwarded
	 * to the uncaught exception handler. See <a href="https://issues.apache.org/jira/browse/FLINK-12889">FLINK-12889</a>.
	 */
	@Test
	public void testUncaughtExceptionInAsynchronousCheckpointingOperation() throws Exception {
		final RuntimeException failingCause = new RuntimeException("Test exception");
		FailingDummyEnvironment failingDummyEnvironment = new FailingDummyEnvironment(failingCause);

		// mock the returned snapshots
		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			ExceptionallyDoneFuture.of(failingCause),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()));

		final TestingUncaughtExceptionHandler uncaughtExceptionHandler = new TestingUncaughtExceptionHandler();

		RunningTask<MockStreamTask> task = runTask(() -> new MockStreamTask(
			failingDummyEnvironment,
			operatorChain(streamOperatorWithSnapshot(operatorSnapshotResult)),
			uncaughtExceptionHandler));
		MockStreamTask streamTask = task.streamTask;

		waitTaskIsRunning(streamTask, task.invocationFuture);

		streamTask.triggerCheckpoint(
			new CheckpointMetaData(42L, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		final Throwable uncaughtException = uncaughtExceptionHandler.waitForUncaughtException();
		assertThat(uncaughtException, is(failingCause));

		streamTask.finishInput();
		task.waitForTaskCompletion(false);
	}

	/**
	 * Tests that in case of a failing AsyncCheckpointRunnable all operator snapshot results are
	 * cancelled and all non partitioned state handles are discarded.
	 */
	@Test
	public void testFailingAsyncCheckpointRunnable() throws Exception {

		// mock the new state operator snapshots
		OperatorSnapshotFutures operatorSnapshotResult1 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult2 = mock(OperatorSnapshotFutures.class);
		OperatorSnapshotFutures operatorSnapshotResult3 = mock(OperatorSnapshotFutures.class);

		RunnableFuture<SnapshotResult<OperatorStateHandle>> failingFuture = mock(RunnableFuture.class);
		when(failingFuture.get()).thenThrow(new ExecutionException(new Exception("Test exception")));

		when(operatorSnapshotResult3.getOperatorStateRawFuture()).thenReturn(failingFuture);

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder().build()) {
			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(
					streamOperatorWithSnapshot(operatorSnapshotResult1),
					streamOperatorWithSnapshot(operatorSnapshotResult2),
					streamOperatorWithSnapshot(operatorSnapshotResult3))));

			MockStreamTask streamTask = task.streamTask;

			waitTaskIsRunning(streamTask, task.invocationFuture);

			mockEnvironment.setExpectedExternalFailureCause(Throwable.class);
			streamTask.triggerCheckpoint(
				new CheckpointMetaData(42L, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

			// wait for the completion of the async task
			ExecutorService executor = streamTask.getAsyncOperationsThreadPool();
			executor.shutdown();
			if (!executor.awaitTermination(10000L, TimeUnit.MILLISECONDS)) {
				fail("Executor did not shut down within the given timeout. This indicates that the " +
					"checkpointing did not resume.");
			}

			assertTrue(mockEnvironment.getActualExternalFailureCause().isPresent());

			verify(operatorSnapshotResult1).cancel();
			verify(operatorSnapshotResult2).cancel();
			verify(operatorSnapshotResult3).cancel();

			streamTask.finishInput();
			task.waitForTaskCompletion(false);
		}
	}

	/**
	 * FLINK-5667
	 *
	 * <p>Tests that a concurrent cancel operation does not discard the state handles of an
	 * acknowledged checkpoint. The situation can only happen if the cancel call is executed
	 * after Environment.acknowledgeCheckpoint() and before the
	 * CloseableRegistry.unregisterClosable() call.
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseAfterAcknowledge() throws Exception {

		final OneShotLatch acknowledgeCheckpointLatch = new OneShotLatch();
		final OneShotLatch completeAcknowledge = new OneShotLatch();

		CheckpointResponder checkpointResponder = mock(CheckpointResponder.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) {
				acknowledgeCheckpointLatch.trigger();

				// block here so that we can issue the concurrent cancel call
				while (true) {
					try {
						// wait until we successfully await (no pun intended)
						completeAcknowledge.await();

						// when await() returns normally, we break out of the loop
						break;
					} catch (InterruptedException e) {
						// survive interruptions that arise from thread pool shutdown
						// production code cannot actually throw InterruptedException from
						// checkpoint acknowledgement
					}
				}

				return null;
			}
		}).when(checkpointResponder).acknowledgeCheckpoint(
			any(JobID.class),
			any(ExecutionAttemptID.class),
			anyLong(),
			any(CheckpointMetrics.class),
			any(TaskStateSnapshot.class));

		TaskStateManager taskStateManager = new TaskStateManagerImpl(
			new JobID(1L, 2L),
			new ExecutionAttemptID(1L, 2L),
			mock(TaskLocalStateStoreImpl.class),
			null,
			checkpointResponder);

		KeyedStateHandle managedKeyedStateHandle = mock(KeyedStateHandle.class);
		KeyedStateHandle rawKeyedStateHandle = mock(KeyedStateHandle.class);
		OperatorStateHandle managedOperatorStateHandle = mock(OperatorStreamStateHandle.class);
		OperatorStateHandle rawOperatorStateHandle = mock(OperatorStreamStateHandle.class);

		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			DoneFuture.of(SnapshotResult.of(managedKeyedStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawKeyedStateHandle)),
			DoneFuture.of(SnapshotResult.of(managedOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawOperatorStateHandle)));

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder()
			.setTaskName("mock-task")
			.setTaskStateManager(taskStateManager)
			.build()) {

			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(streamOperatorWithSnapshot(operatorSnapshotResult))));

			MockStreamTask streamTask = task.streamTask;
			waitTaskIsRunning(streamTask, task.invocationFuture);

			final long checkpointId = 42L;
			streamTask.triggerCheckpoint(
				new CheckpointMetaData(checkpointId, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

			acknowledgeCheckpointLatch.await();

			ArgumentCaptor<TaskStateSnapshot> subtaskStateCaptor = ArgumentCaptor.forClass(TaskStateSnapshot.class);

			// check that the checkpoint has been completed
			verify(checkpointResponder).acknowledgeCheckpoint(
				any(JobID.class),
				any(ExecutionAttemptID.class),
				eq(checkpointId),
				any(CheckpointMetrics.class),
				subtaskStateCaptor.capture());

			TaskStateSnapshot subtaskStates = subtaskStateCaptor.getValue();
			OperatorSubtaskState subtaskState = subtaskStates.getSubtaskStateMappings().iterator().next().getValue();

			// check that the subtask state contains the expected state handles
			assertEquals(StateObjectCollection.singleton(managedKeyedStateHandle), subtaskState.getManagedKeyedState());
			assertEquals(StateObjectCollection.singleton(rawKeyedStateHandle), subtaskState.getRawKeyedState());
			assertEquals(StateObjectCollection.singleton(managedOperatorStateHandle), subtaskState.getManagedOperatorState());
			assertEquals(StateObjectCollection.singleton(rawOperatorStateHandle), subtaskState.getRawOperatorState());

			// check that the state handles have not been discarded
			verify(managedKeyedStateHandle, never()).discardState();
			verify(rawKeyedStateHandle, never()).discardState();
			verify(managedOperatorStateHandle, never()).discardState();
			verify(rawOperatorStateHandle, never()).discardState();

			streamTask.cancel();

			completeAcknowledge.trigger();

			// canceling the stream task after it has acknowledged the checkpoint should not discard
			// the state handles
			verify(managedKeyedStateHandle, never()).discardState();
			verify(rawKeyedStateHandle, never()).discardState();
			verify(managedOperatorStateHandle, never()).discardState();
			verify(rawOperatorStateHandle, never()).discardState();

			task.waitForTaskCompletion(true);
		}
	}

	/**
	 * FLINK-5667
	 *
	 * <p>Tests that a concurrent cancel operation discards the state handles of a not yet
	 * acknowledged checkpoint and prevents sending an acknowledge message to the
	 * CheckpointCoordinator. The situation can only happen if the cancel call is executed
	 * before Environment.acknowledgeCheckpoint().
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseBeforeAcknowledge() throws Exception {

		final TestingKeyedStateHandle managedKeyedStateHandle = new TestingKeyedStateHandle();
		final TestingKeyedStateHandle rawKeyedStateHandle = new TestingKeyedStateHandle();
		final TestingOperatorStateHandle managedOperatorStateHandle = new TestingOperatorStateHandle();
		final TestingOperatorStateHandle rawOperatorStateHandle = new TestingOperatorStateHandle();

		final BlockingRunnableFuture<SnapshotResult<KeyedStateHandle>> rawKeyedStateHandleFuture = new BlockingRunnableFuture<>(2, SnapshotResult.of(rawKeyedStateHandle));
		OperatorSnapshotFutures operatorSnapshotResult = new OperatorSnapshotFutures(
			DoneFuture.of(SnapshotResult.of(managedKeyedStateHandle)),
			rawKeyedStateHandleFuture,
			DoneFuture.of(SnapshotResult.of(managedOperatorStateHandle)),
			DoneFuture.of(SnapshotResult.of(rawOperatorStateHandle)));

		final StreamOperator<?> streamOperator = streamOperatorWithSnapshot(operatorSnapshotResult);

		final AcknowledgeDummyEnvironment mockEnvironment = new AcknowledgeDummyEnvironment();

		RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
			mockEnvironment,
			operatorChain(streamOperator)));

		waitTaskIsRunning(task.streamTask, task.invocationFuture);

		final long checkpointId = 42L;
		task.streamTask.triggerCheckpoint(
			new CheckpointMetaData(checkpointId, 1L),
			CheckpointOptions.forCheckpointWithDefaultLocation(),
			false);

		rawKeyedStateHandleFuture.awaitRun();

		task.streamTask.cancel();

		final FutureUtils.ConjunctFuture<Void> discardFuture = FutureUtils.waitForAll(
			Arrays.asList(
				managedKeyedStateHandle.getDiscardFuture(),
				rawKeyedStateHandle.getDiscardFuture(),
				managedOperatorStateHandle.getDiscardFuture(),
				rawOperatorStateHandle.getDiscardFuture()));

		// make sure that all state handles have been discarded
		discardFuture.get();

		try {
			mockEnvironment.getAcknowledgeCheckpointFuture().get(10L, TimeUnit.MILLISECONDS);
			fail("The checkpoint should not get acknowledged.");
		} catch (TimeoutException expected) {
			// future should not be completed
		}

		task.waitForTaskCompletion(true);
	}

	/**
	 * FLINK-5985
	 *
	 * <p>This test ensures that empty snapshots (no op/keyed stated whatsoever) will be reported as stateless tasks. This
	 * happens by translating an empty {@link SubtaskState} into reporting 'null' to #acknowledgeCheckpoint.
	 */
	@Test
	public void testEmptySubtaskStateLeadsToStatelessAcknowledgment() throws Exception {

		// latch blocks until the async checkpoint thread acknowledges
		final OneShotLatch checkpointCompletedLatch = new OneShotLatch();
		final List<SubtaskState> checkpointResult = new ArrayList<>(1);

		CheckpointResponder checkpointResponder = mock(CheckpointResponder.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				SubtaskState subtaskState = invocation.getArgument(4);
				checkpointResult.add(subtaskState);
				checkpointCompletedLatch.trigger();
				return null;
			}
		}).when(checkpointResponder).acknowledgeCheckpoint(
			any(JobID.class),
			any(ExecutionAttemptID.class),
			anyLong(),
			any(CheckpointMetrics.class),
			nullable(TaskStateSnapshot.class));

		TaskStateManager taskStateManager = new TaskStateManagerImpl(
			new JobID(1L, 2L),
			new ExecutionAttemptID(1L, 2L),
			mock(TaskLocalStateStoreImpl.class),
			null,
			checkpointResponder);

		// mock the operator with empty snapshot result (all state handles are null)
		StreamOperator<?> statelessOperator = streamOperatorWithSnapshot(new OperatorSnapshotFutures());

		try (MockEnvironment mockEnvironment = new MockEnvironmentBuilder()
			.setTaskStateManager(taskStateManager)
			.build()) {

			RunningTask<MockStreamTask> task = runTask(() -> createMockStreamTask(
				mockEnvironment,
				operatorChain(statelessOperator)));

			waitTaskIsRunning(task.streamTask, task.invocationFuture);

			task.streamTask.triggerCheckpoint(
				new CheckpointMetaData(42L, 1L),
				CheckpointOptions.forCheckpointWithDefaultLocation(),
				false);

			checkpointCompletedLatch.await(30, TimeUnit.SECONDS);

			// ensure that 'null' was acknowledged as subtask state
			Assert.assertNull(checkpointResult.get(0));

			task.streamTask.cancel();
			task.waitForTaskCompletion(true);
		}
	}

	/**
	 * Tests that the StreamTask first closes all of its operators before setting its
	 * state to not running (isRunning == false)
	 *
	 * <p>See FLINK-7430.
	 */
	@Test
	public void testOperatorClosingBeforeStopRunning() throws Throwable {
		BlockingCloseStreamOperator.resetLatches();
		Configuration taskConfiguration = new Configuration();
		StreamConfig streamConfig = new StreamConfig(taskConfiguration);
		streamConfig.setStreamOperator(new BlockingCloseStreamOperator());
		streamConfig.setOperatorID(new OperatorID());

		try (MockEnvironment mockEnvironment =
				new MockEnvironmentBuilder()
					.setTaskName("Test Task")
					.setMemorySize(32L * 1024L)
					.setInputSplitProvider(new MockInputSplitProvider())
					.setBufferSize(1)
					.setTaskConfiguration(taskConfiguration)
					.build()) {

			RunningTask<StreamTask<Void, BlockingCloseStreamOperator>> task = runTask(() -> new NoOpStreamTask<>(mockEnvironment));

			BlockingCloseStreamOperator.inClose.await();

			// check that the StreamTask is not yet in isRunning == false
			assertTrue(task.streamTask.isRunning());

			// let the operator finish its close operation
			BlockingCloseStreamOperator.finishClose.trigger();

			task.waitForTaskCompletion(false);

			// now the StreamTask should no longer be running
			assertFalse(task.streamTask.isRunning());
		}
	}

	/**
	 * Test set user code ClassLoader before calling ProcessingTimeCallback.
	 */
	@Test
	public void testSetsUserCodeClassLoaderForTimerThreadFactory() throws Throwable {
		syncLatch = new OneShotLatch();

		try (MockEnvironment mockEnvironment =
			new MockEnvironmentBuilder()
				.setUserCodeClassLoader(new TestUserCodeClassLoader())
				.build()) {
			RunningTask<TimeServiceTask> task = runTask(() -> new TimeServiceTask(mockEnvironment));
			task.waitForTaskCompletion(false);

			assertThat(task.streamTask.getClassLoaders(), hasSize(greaterThanOrEqualTo(1)));
			assertThat(task.streamTask.getClassLoaders(), everyItem(instanceOf(TestUserCodeClassLoader.class)));
		}
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static StreamOperator<?> streamOperatorWithSnapshot(OperatorSnapshotFutures operatorSnapshotResult) throws Exception {
		StreamOperator<?> operator = mock(StreamOperator.class);
		when(operator.getOperatorID()).thenReturn(new OperatorID());

		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class)))
			.thenReturn(operatorSnapshotResult);

		return operator;
	}

	private static StreamOperator<?> streamOperatorWithSnapshotException(Exception exception) throws Exception {
		StreamOperator<?> operator = mock(StreamOperator.class);
		when(operator.getOperatorID()).thenReturn(new OperatorID());

		when(operator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class), any(CheckpointStreamFactory.class)))
			.thenThrow(exception);

		return operator;
	}

	private static <T> OperatorChain<T, AbstractStreamOperator<T>> operatorChain(StreamOperator<?>... streamOperators) {
		OperatorChain<T, AbstractStreamOperator<T>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);
		return operatorChain;
	}

	private static class RunningTask<T extends StreamTask<?, ?>> {
		final T streamTask;
		final CompletableFuture<Void> invocationFuture;

		RunningTask(T streamTask, CompletableFuture<Void> invocationFuture) {
			this.streamTask = streamTask;
			this.invocationFuture = invocationFuture;
		}

		void waitForTaskCompletion(boolean cancelled) throws Exception {
			try {
				invocationFuture.get();
			} catch (Exception e) {
				if (cancelled) {
					assertThat(e.getCause(), is(instanceOf(CancelTaskException.class)));
				} else {
					throw e;
				}
			}
			assertThat(streamTask.isCanceled(), is(cancelled));
		}
	}

	private static <T extends StreamTask<?, ?>> RunningTask<T> runTask(SupplierWithException<T, Exception> taskFactory) throws Exception {
		CompletableFuture<T> taskCreationFuture = new CompletableFuture<>();
		CompletableFuture<Void> invocationFuture = CompletableFuture.runAsync(
			() -> {
				T task;
				try {
					task = taskFactory.get();
					taskCreationFuture.complete(task);
				} catch (Exception e) {
					taskCreationFuture.completeExceptionally(e);
					return;
				}
				try {
					task.invoke();
				} catch (RuntimeException e) {
					throw e;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}, Executors.newSingleThreadExecutor());

		// Wait until task is created.
		return new RunningTask<>(taskCreationFuture.get(), invocationFuture);
	}

	/**
	 * Operator that does nothing.
	 *
	 * @param <T>
	 * @param <OP>
	 */
	public static class NoOpStreamTask<T, OP extends StreamOperator<T>> extends StreamTask<T, OP> {

		public NoOpStreamTask(Environment environment) {
			super(environment, null);
		}

		@Override
		protected void init() throws Exception {}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			context.allActionsCompleted();
		}

		@Override
		protected void cleanup() throws Exception {}
	}

	private static class BlockingCloseStreamOperator extends AbstractStreamOperator<Void> {
		private static final long serialVersionUID = -9042150529568008847L;

		private static volatile OneShotLatch inClose;
		private static volatile OneShotLatch finishClose;

		@Override
		public void close() throws Exception {
			checkLatches();
			inClose.trigger();
			finishClose.await();
			super.close();
		}

		private void checkLatches() {
			Preconditions.checkNotNull(inClose);
			Preconditions.checkNotNull(finishClose);
		}

		private static void resetLatches() {
			inClose = new OneShotLatch();
			finishClose = new OneShotLatch();
		}
	}

	public static Task createTask(
		Class<? extends AbstractInvokable> invokable,
		StreamConfig taskConfig,
		Configuration taskManagerConfig) throws Exception {
		return createTask(invokable, taskConfig, taskManagerConfig, new TestTaskStateManager(), mock(TaskManagerActions.class));
	}

	public static Task createTask(
		Class<? extends AbstractInvokable> invokable,
		StreamConfig taskConfig,
		Configuration taskManagerConfig,
		TaskManagerActions taskManagerActions) throws Exception {
		return createTask(invokable, taskConfig, taskManagerConfig, new TestTaskStateManager(), taskManagerActions);
	}

	public static Task createTask(
			Class<? extends AbstractInvokable> invokable,
			StreamConfig taskConfig,
			Configuration taskManagerConfig,
			TestTaskStateManager taskStateManager,
			TaskManagerActions taskManagerActions) throws Exception {

		BlobCacheService blobService =
			new BlobCacheService(mock(PermanentBlobCache.class), mock(TransientBlobCache.class));

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(StreamTaskTest.class.getClassLoader());

		ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);

		ShuffleEnvironment<?, ?> shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

		JobInformation jobInformation = new JobInformation(
			new JobID(),
			"Job Name",
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());

		TaskInformation taskInformation = new TaskInformation(
			new JobVertexID(),
			"Test Task",
			1,
			1,
			invokable.getName(),
			taskConfig.getConfiguration());

		return new Task(
			jobInformation,
			taskInformation,
			new ExecutionAttemptID(),
			new AllocationID(),
			0,
			0,
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			0,
			mock(MemoryManager.class),
			mock(IOManager.class),
			shuffleEnvironment,
			new KvStateService(new KvStateRegistry(), null, null),
			mock(BroadcastVariableManager.class),
			new TaskEventDispatcher(),
			taskStateManager,
			taskManagerActions,
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			new TestGlobalAggregateManager(),
			blobService,
			libCache,
			mock(FileCache.class),
			new TestingTaskManagerRuntimeInfo(taskManagerConfig, new String[] {System.getProperty("java.io.tmpdir")}),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			consumableNotifier,
			partitionProducerStateChecker,
			executor);
	}

	// ------------------------------------------------------------------------
	//  Test operators
	// ------------------------------------------------------------------------

	private static class SlowlyDeserializingOperator extends StreamSource<Long, SourceFunction<Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean canceled = false;

		public SlowlyDeserializingOperator() {
			super(new MockSourceFunction());
		}

		@Override
		public void run(Object lockingObject,
						StreamStatusMaintainer streamStatusMaintainer,
						Output<StreamRecord<Long>> collector,
						OperatorChain<?, ?> operatorChain) throws Exception {
			while (!canceled) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) {}
			}
		}

		@Override
		public void cancel() {
			canceled = true;
		}

		// slow deserialization
		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();

			long delay = 500;
			long deadline = System.currentTimeMillis() + delay;
			do {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException ignored) {}
			} while ((delay = deadline - System.currentTimeMillis()) > 0);
		}
	}

	private static class MockSourceFunction implements SourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Long> ctx) {}

		@Override
		public void cancel() {}
	}

	/**
	 * Mocked state backend factory which returns mocks for the operator and keyed state backends.
	 */
	public static final class TestMemoryStateBackendFactory implements StateBackendFactory<AbstractStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public AbstractStateBackend createFromConfig(Configuration config, ClassLoader classLoader) {
			return new TestSpyWrapperStateBackend(createInnerBackend(config));
		}

		protected AbstractStateBackend createInnerBackend(Configuration config) {
			return new MemoryStateBackend();
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	private static class MockStreamTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final OperatorChain<String, AbstractStreamOperator<String>> overrideOperatorChain;
		private volatile boolean inputFinished;

		MockStreamTask(Environment env, OperatorChain<String, AbstractStreamOperator<String>> operatorChain, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
			super(env, null, uncaughtExceptionHandler);
			this.overrideOperatorChain = operatorChain;
		}

		@Override
		protected void init() {
			// The StreamTask initializes operatorChain first on it's own in `invoke()` method.
			// Later it calls the `init()` method before actual `run()`, so we are overriding the operatorChain
			// here for test purposes.
			super.operatorChain = this.overrideOperatorChain;
			super.headOperator = super.operatorChain.getHeadOperator();
		}

		@Override
		protected void processInput(ActionContext context) {
			if (isCanceled() || inputFinished) {
				context.allActionsCompleted();
			}
		}

		@Override
		protected void cleanup() throws Exception {}

		void finishInput() {
			this.inputFinished = true;
		}
	}

	private static MockStreamTask createMockStreamTask(Environment env, OperatorChain<String, AbstractStreamOperator<String>> operatorChain) {
		return new MockStreamTask(env, operatorChain, FatalExitExceptionHandler.INSTANCE);
	}

	/**
	 * Source that instantiates the operator state backend and the keyed state backend.
	 * The created state backends can be retrieved from the static fields to check if the
	 * CloseableRegistry closed them correctly.
	 */
	public static class StateBackendTestSource extends StreamTask<Long, StreamSource<Long, SourceFunction<Long>>> {

		private static volatile boolean fail;

		public StateBackendTestSource(Environment env) {
			super(env);
		}

		@Override
		protected void init() throws Exception {

		}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			if (fail) {
				throw new RuntimeException();
			}
			context.allActionsCompleted();
		}

		@Override
		protected void cleanup() throws Exception {}

		@Override
		public StreamTaskStateInitializer createStreamTaskStateInitializer() {
			final StreamTaskStateInitializer streamTaskStateManager = super.createStreamTaskStateInitializer();
			return (operatorID, operatorClassName, keyContext, keySerializer, closeableRegistry, metricGroup) -> {

				final StreamOperatorStateContext context = streamTaskStateManager.streamOperatorStateContext(
					operatorID,
					operatorClassName,
					keyContext,
					keySerializer,
					closeableRegistry,
					metricGroup);

				return new StreamOperatorStateContext() {
					@Override
					public boolean isRestored() {
						return context.isRestored();
					}

					@Override
					public OperatorStateBackend operatorStateBackend() {
						return context.operatorStateBackend();
					}

					@Override
					public AbstractKeyedStateBackend<?> keyedStateBackend() {
						return context.keyedStateBackend();
					}

					@Override
					public InternalTimeServiceManager<?> internalTimerServiceManager() {
						InternalTimeServiceManager<?> timeServiceManager = context.internalTimerServiceManager();
						return timeServiceManager != null ? spy(timeServiceManager) : null;
					}

					@Override
					public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
						return replaceWithSpy(context.rawOperatorStateInputs());
					}

					@Override
					public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
						return replaceWithSpy(context.rawKeyedStateInputs());
					}

					public <T extends Closeable> T replaceWithSpy(T closeable) {
						T spyCloseable = spy(closeable);
						if (closeableRegistry.unregisterCloseable(closeable)) {
							try {
								closeableRegistry.registerCloseable(spyCloseable);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
						return spyCloseable;
					}
				};
			};
		}
	}

	/**
	 * A task that locks if cancellation attempts to cleanly shut down.
	 */
	public static class CancelLockingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final OneShotLatch latch = new OneShotLatch();

		private LockHolder holder;

		public CancelLockingTask(Environment env) {
			super(env);
		}

		@Override
		protected void init() {}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			holder = new LockHolder(getCheckpointLock(), latch);
			holder.start();
			latch.await();

			// we are at the point where cancelling can happen
			syncLatch.trigger();

			// just put this to sleep until it is interrupted
			try {
				Thread.sleep(100000000);
			} catch (InterruptedException ignored) {
				// restore interruption state
				Thread.currentThread().interrupt();
			}
			context.allActionsCompleted();
		}

		@Override
		protected void cleanup() {
			holder.close();
		}

		@Override
		protected void cancelTask() {
			holder.cancel();
			// do not interrupt the lock holder here, to simulate spawned threads that
			// we cannot properly interrupt on cancellation
		}

	}

	/**
	 * A task that locks if cancellation attempts to cleanly shut down.
	 */
	public static class CancelFailingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		public CancelFailingTask(Environment env) {
			super(env);
		}

		@Override
		protected void init() {}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			final OneShotLatch latch = new OneShotLatch();
			final Object lock = new Object();

			LockHolder holder = new LockHolder(lock, latch);
			holder.start();
			try {
				// cancellation should try and cancel this
				getCancelables().registerCloseable(holder);

				// wait till the lock holder has the lock
				latch.await();

				// we are at the point where cancelling can happen
				syncLatch.trigger();

				// try to acquire the lock - this is not possible as long as the lock holder
				// thread lives
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (lock) {
					// nothing
				}
			}
			finally {
				holder.close();
			}
			context.allActionsCompleted();
		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() throws Exception {
			throw new Exception("test exception");
		}

	}

	/**
	 * A task that register a processing time service callback.
	 */
	public static class TimeServiceTask extends NoOpStreamTask<String, AbstractStreamOperator<String>> {

		private final List<ClassLoader> classLoaders = Collections.synchronizedList(new ArrayList<>());

		public TimeServiceTask(Environment env) {
			super(env);
		}

		public List<ClassLoader> getClassLoaders() {
			return classLoaders;
		}

		@Override
		protected void init() throws Exception {
			getProcessingTimeService().registerTimer(0, new ProcessingTimeCallback() {
				@Override
				public void onProcessingTime(long timestamp) throws Exception {
					classLoaders.add(Thread.currentThread().getContextClassLoader());
					syncLatch.trigger();
				}
			});
		}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			syncLatch.await();
			super.processInput(context);
		}
	}

	/**
	 * A {@link ClassLoader} that delegates everything to {@link ClassLoader#getSystemClassLoader()}.
	 */
	private static class TestUserCodeClassLoader extends ClassLoader {
		public TestUserCodeClassLoader() {
			super(ClassLoader.getSystemClassLoader());
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * A thread that holds a lock as long as it lives.
	 */
	private static final class LockHolder extends Thread implements Closeable {

		private final OneShotLatch trigger;
		private final Object lock;
		private volatile boolean canceled;

		private LockHolder(Object lock, OneShotLatch trigger) {
			this.lock = lock;
			this.trigger = trigger;
		}

		@Override
		public void run() {
			synchronized (lock) {
				while (!canceled) {
					// signal that we grabbed the lock
					trigger.trigger();

					// basically freeze this thread
					try {
						//noinspection SleepWhileHoldingLock
						Thread.sleep(1000000000);
					} catch (InterruptedException ignored) {}
				}
			}
		}

		public void cancel() {
			canceled = true;
		}

		@Override
		public void close() {
			canceled = true;
			interrupt();
		}
	}

	static class TestStreamSource<OUT, SRC extends SourceFunction<OUT>> extends StreamSource<OUT, SRC> {

		static AbstractKeyedStateBackend<?> keyedStateBackend;
		static OperatorStateBackend operatorStateBackend;
		static CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
		static CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

		public TestStreamSource(SRC sourceFunction) {
			super(sourceFunction);
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			keyedStateBackend = (AbstractKeyedStateBackend<?>) getKeyedStateBackend();
			operatorStateBackend = getOperatorStateBackend();
			rawOperatorStateInputs =
				(CloseableIterable<StatePartitionStreamProvider>) context.getRawOperatorStateInputs();
			rawKeyedStateInputs =
				(CloseableIterable<KeyGroupStatePartitionStreamProvider>) context.getRawKeyedStateInputs();
			super.initializeState(context);
		}
	}

	private static class TestingKeyedStateHandle implements KeyedStateHandle {

		private static final long serialVersionUID = -2473861305282291582L;

		private final transient CompletableFuture<Void> discardFuture = new CompletableFuture<>();

		public CompletableFuture<Void> getDiscardFuture() {
			return discardFuture;
		}

		@Override
		public KeyGroupRange getKeyGroupRange() {
			return KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
		}

		@Override
		public TestingKeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
			return this;
		}

		@Override
		public void registerSharedStates(SharedStateRegistry stateRegistry) {

		}

		@Override
		public void discardState() {
			discardFuture.complete(null);
		}

		@Override
		public long getStateSize() {
			return 0L;
		}
	}

	private static class TestingOperatorStateHandle implements OperatorStateHandle {

		private static final long serialVersionUID = 923794934187614088L;

		private final transient CompletableFuture<Void> discardFuture = new CompletableFuture<>();

		public CompletableFuture<Void> getDiscardFuture() {
			return discardFuture;
		}

		@Override
		public Map<String, StateMetaInfo> getStateNameToPartitionOffsets() {
			return Collections.emptyMap();
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			throw new IOException("Cannot open input streams in testing implementation.");
		}

		@Override
		public StreamStateHandle getDelegateStateHandle() {
			throw new UnsupportedOperationException("Not implemented.");
		}

		@Override
		public void discardState() throws Exception {
			discardFuture.complete(null);
		}

		@Override
		public long getStateSize() {
			return 0L;
		}
	}

	private static class AcknowledgeDummyEnvironment extends DummyEnvironment {

		private final CompletableFuture<Long> acknowledgeCheckpointFuture = new CompletableFuture<>();

		public CompletableFuture<Long> getAcknowledgeCheckpointFuture() {
			return acknowledgeCheckpointFuture;
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
			acknowledgeCheckpointFuture.complete(checkpointId);
		}

		@Override
		public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
			acknowledgeCheckpointFuture.complete(checkpointId);
		}
	}

	private static final class BlockingRunnableFuture<V> implements RunnableFuture<V> {

		private final CompletableFuture<V> future = new CompletableFuture<>();

		private final OneShotLatch signalRunLatch = new OneShotLatch();

		private final CountDownLatch continueRunLatch;

		private final V value;

		private BlockingRunnableFuture(int parties, V value) {
			this.continueRunLatch = new CountDownLatch(parties);
			this.value = value;
		}

		@Override
		public void run() {
			signalRunLatch.trigger();
			continueRunLatch.countDown();

			try {
				// poor man's barrier because it can happen that the async operations thread gets
				// interrupted by the mail box thread. The CyclicBarrier would in this case fail
				// all participants of the barrier, leaving the future uncompleted
				continueRunLatch.await();
			} catch (InterruptedException e) {
				ExceptionUtils.rethrow(e);
			}

			future.complete(value);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return future.isCancelled();
		}

		@Override
		public boolean isDone() {
			return future.isDone();
		}

		@Override
		public V get() throws InterruptedException, ExecutionException {
			return future.get();
		}

		@Override
		public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return future.get(timeout, unit);
		}

		void awaitRun() throws InterruptedException {
			signalRunLatch.await();
		}
	}

	private static class FailingDummyEnvironment extends DummyEnvironment {

		final RuntimeException failingCause;

		private FailingDummyEnvironment(RuntimeException failingCause) {
			this.failingCause = failingCause;
		}

		@Override
		public void declineCheckpoint(long checkpointId, Throwable cause) {
			throw failingCause;
		}

		@Override
		public void failExternally(Throwable cause) {
			throw failingCause;
		}
	}
}
