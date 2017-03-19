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

import akka.dispatch.Futures;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionStateListener;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.DirectExecutorService;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamTask.class)
@PowerMockIgnore("org.apache.log4j.*")
@SuppressWarnings("deprecation")
public class StreamTaskTest extends TestLogger {

	private static OneShotLatch SYNC_LATCH;

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() throws Exception {
		Deadline deadline = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();
		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new SlowlyDeserializingOperator());
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(SourceStreamTask.class, cfg, new Configuration());

		TestingExecutionStateListener testingExecutionStateListener = new TestingExecutionStateListener();

		task.registerExecutionListener(testingExecutionStateListener);
		task.startTaskThread();

		Future<ExecutionState> running = testingExecutionStateListener.notifyWhenExecutionState(ExecutionState.RUNNING);

		// wait until the task thread reached state RUNNING
		ExecutionState executionState = Await.result(running, deadline.timeLeft());

		// make sure the task is really running
		if (executionState != ExecutionState.RUNNING) {
			fail("Task entered state " + task.getExecutionState() + " with error "
					+ ExceptionUtils.stringifyException(task.getFailureCause()));
		}

		// send a cancel. because the operator takes a long time to deserialize, this should
		// hit the task before the operator is deserialized
		task.cancelExecution();

		Future<ExecutionState> canceling = testingExecutionStateListener.notifyWhenExecutionState(ExecutionState.CANCELING);

		executionState = Await.result(canceling, deadline.timeLeft());

		// the task should reach state canceled eventually
		assertTrue(executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED);

		task.getExecutingThread().join(deadline.timeLeft().toMillis());

		assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testStateBackendLoadingAndClosing() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CoreOptions.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = false;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends are closed
		verify(StateBackendTestSource.operatorStateBackend).close();
		verify(StateBackendTestSource.keyedStateBackend).close();

		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	@Test
	public void testStateBackendClosingOnFailure() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(CoreOptions.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = true;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends are closed
		verify(StateBackendTestSource.operatorStateBackend).close();
		verify(StateBackendTestSource.keyedStateBackend).close();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
	}

	@Test
	public void testCancellationNotBlockedOnLock() throws Exception {
		SYNC_LATCH = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		Task task = createTask(CancelLockingTask.class, cfg, new Configuration());

		// start the task and wait until it runs
		// execution state RUNNING is not enough, we need to wait until the stream task's run() method
		// is entered
		task.startTaskThread();
		SYNC_LATCH.await();

		// cancel the execution - this should lead to smooth shutdown
		task.cancelExecution();
		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testCancellationFailsWithBlockingLock() throws Exception {
		SYNC_LATCH = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		Task task = createTask(CancelFailingTask.class, cfg, new Configuration());

		// start the task and wait until it runs
		// execution state RUNNING is not enough, we need to wait until the stream task's run() method
		// is entered
		task.startTaskThread();
		SYNC_LATCH.await();

		// cancel the execution - this should lead to smooth shutdown
		task.cancelExecution();
		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testFailingCheckpointStreamOperator() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		TaskInfo mockTaskInfo = mock(TaskInfo.class);
		when(mockTaskInfo.getTaskNameWithSubtasks()).thenReturn("foobar");
		when(mockTaskInfo.getIndexOfThisSubtask()).thenReturn(0);
		Environment mockEnvironment = mock(Environment.class);
		when(mockEnvironment.getTaskInfo()).thenReturn(mockTaskInfo);

		StreamTask<?, AbstractStreamOperator<?>> streamTask = mock(StreamTask.class, Mockito.CALLS_REAL_METHODS);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
		streamTask.setEnvironment(mockEnvironment);

		// mock the operators
		StreamOperator<?> streamOperator1 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));
		StreamOperator<?> streamOperator2 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));
		StreamOperator<?> streamOperator3 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));

		// mock the returned snapshots
		OperatorSnapshotResult operatorSnapshotResult1 = mock(OperatorSnapshotResult.class);
		OperatorSnapshotResult operatorSnapshotResult2 = mock(OperatorSnapshotResult.class);

		final Exception testException = new Exception("Test exception");

		when(streamOperator1.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult1);
		when(streamOperator2.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult2);
		when(streamOperator3.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenThrow(testException);

		// mock the returned legacy snapshots
		StreamStateHandle streamStateHandle1 = mock(StreamStateHandle.class);
		StreamStateHandle streamStateHandle2 = mock(StreamStateHandle.class);
		StreamStateHandle streamStateHandle3 = mock(StreamStateHandle.class);

		when(streamOperator1.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle1);
		when(streamOperator2.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle2);
		when(streamOperator3.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle3);

		// set up the task

		StreamOperator<?>[] streamOperators = {streamOperator1, streamOperator2, streamOperator3};

		OperatorChain<Void, AbstractStreamOperator<Void>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);

		Whitebox.setInternalState(streamTask, "isRunning", true);
		Whitebox.setInternalState(streamTask, "lock", new Object());
		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "cancelables", new CloseableRegistry());
		Whitebox.setInternalState(streamTask, "configuration", new StreamConfig(new Configuration()));

		try {
			streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forFullCheckpoint());
			fail("Expected test exception here.");
		} catch (Exception e) {
			assertEquals(testException, e.getCause());
		}

		verify(operatorSnapshotResult1).cancel();
		verify(operatorSnapshotResult2).cancel();

		verify(streamStateHandle1).discardState();
		verify(streamStateHandle2).discardState();
		verify(streamStateHandle3).discardState();
	}

	/**
	 * Tests that in case of a failing AsyncCheckpointRunnable all operator snapshot results are
	 * cancelled and all non partitioned state handles are discarded.
	 */
	@Test
	public void testFailingAsyncCheckpointRunnable() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		TaskInfo mockTaskInfo = mock(TaskInfo.class);
		when(mockTaskInfo.getTaskNameWithSubtasks()).thenReturn("foobar");
		when(mockTaskInfo.getIndexOfThisSubtask()).thenReturn(0);
		Environment mockEnvironment = mock(Environment.class);
		when(mockEnvironment.getTaskInfo()).thenReturn(mockTaskInfo);

		StreamTask<?, AbstractStreamOperator<?>> streamTask = mock(StreamTask.class, Mockito.CALLS_REAL_METHODS);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
		streamTask.setEnvironment(mockEnvironment);

		StreamOperator<?> streamOperator1 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));
		StreamOperator<?> streamOperator2 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));
		StreamOperator<?> streamOperator3 = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));

		// mock the new state handles / futures

		OperatorSnapshotResult operatorSnapshotResult1 = mock(OperatorSnapshotResult.class);
		OperatorSnapshotResult operatorSnapshotResult2 = mock(OperatorSnapshotResult.class);
		OperatorSnapshotResult operatorSnapshotResult3 = mock(OperatorSnapshotResult.class);

		RunnableFuture<OperatorStateHandle> failingFuture = mock(RunnableFuture.class);
		when(failingFuture.get()).thenThrow(new ExecutionException(new Exception("Test exception")));

		when(operatorSnapshotResult3.getOperatorStateRawFuture()).thenReturn(failingFuture);

		when(streamOperator1.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult1);
		when(streamOperator2.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult2);
		when(streamOperator3.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult3);

		// mock the legacy state snapshot
		StreamStateHandle streamStateHandle1 = mock(StreamStateHandle.class);
		StreamStateHandle streamStateHandle2 = mock(StreamStateHandle.class);
		StreamStateHandle streamStateHandle3 = mock(StreamStateHandle.class);

		when(streamOperator1.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle1);
		when(streamOperator2.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle2);
		when(streamOperator3.snapshotLegacyOperatorState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(streamStateHandle3);

		StreamOperator<?>[] streamOperators = {streamOperator1, streamOperator2, streamOperator3};

		OperatorChain<Void, AbstractStreamOperator<Void>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);

		Whitebox.setInternalState(streamTask, "isRunning", true);
		Whitebox.setInternalState(streamTask, "lock", new Object());
		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "cancelables", new CloseableRegistry());
		Whitebox.setInternalState(streamTask, "asyncOperationsThreadPool", new DirectExecutorService());
		Whitebox.setInternalState(streamTask, "configuration", new StreamConfig(new Configuration()));

		streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forFullCheckpoint());

		verify(streamTask).handleAsyncException(anyString(), any(Throwable.class));

		verify(operatorSnapshotResult1).cancel();
		verify(operatorSnapshotResult2).cancel();
		verify(operatorSnapshotResult3).cancel();

		verify(streamStateHandle1).discardState();
		verify(streamStateHandle2).discardState();
		verify(streamStateHandle3).discardState();
	}

	/**
	 * FLINK-5667
	 *
	 * Tests that a concurrent cancel operation does not discard the state handles of an
	 * acknowledged checkpoint. The situation can only happen if the cancel call is executed
	 * after Environment.acknowledgeCheckpoint() and before the
	 * CloseableRegistry.unregisterClosable() call.
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseAfterAcknowledge() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		final OneShotLatch acknowledgeCheckpointLatch = new OneShotLatch();
		final OneShotLatch completeAcknowledge = new OneShotLatch();

		TaskInfo mockTaskInfo = mock(TaskInfo.class);
		when(mockTaskInfo.getTaskNameWithSubtasks()).thenReturn("foobar");
		when(mockTaskInfo.getIndexOfThisSubtask()).thenReturn(0);
		Environment mockEnvironment = mock(Environment.class);
		when(mockEnvironment.getTaskInfo()).thenReturn(mockTaskInfo);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				acknowledgeCheckpointLatch.trigger();

				// block here so that we can issue the concurrent cancel call
				completeAcknowledge.await();

				return null;
			}
		}).when(mockEnvironment).acknowledgeCheckpoint(anyLong(), any(CheckpointMetrics.class), any(SubtaskState.class));

		StreamTask<?, AbstractStreamOperator<?>> streamTask = mock(StreamTask.class, Mockito.CALLS_REAL_METHODS);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
		streamTask.setEnvironment(mockEnvironment);

		StreamOperator<?> streamOperator = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));

		KeyedStateHandle managedKeyedStateHandle = mock(KeyedStateHandle.class);
		KeyedStateHandle rawKeyedStateHandle = mock(KeyedStateHandle.class);
		OperatorStateHandle managedOperatorStateHandle = mock(OperatorStateHandle.class);
		OperatorStateHandle rawOperatorStateHandle = mock(OperatorStateHandle.class);

		OperatorSnapshotResult operatorSnapshotResult = new OperatorSnapshotResult(
			new DoneFuture<>(managedKeyedStateHandle),
			new DoneFuture<>(rawKeyedStateHandle),
			new DoneFuture<>(managedOperatorStateHandle),
			new DoneFuture<>(rawOperatorStateHandle));

		when(streamOperator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult);

		StreamOperator<?>[] streamOperators = {streamOperator};

		OperatorChain<Void, AbstractStreamOperator<Void>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);

		StreamStateHandle streamStateHandle = mock(StreamStateHandle.class);

		CheckpointStreamFactory.CheckpointStateOutputStream outStream = mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);

		when(outStream.closeAndGetHandle()).thenReturn(streamStateHandle);

		CheckpointStreamFactory mockStreamFactory = mock(CheckpointStreamFactory.class);
		when(mockStreamFactory.createCheckpointStateOutputStream(anyLong(), anyLong())).thenReturn(outStream);

		AbstractStateBackend mockStateBackend = mock(AbstractStateBackend.class);
		when(mockStateBackend.createStreamFactory(any(JobID.class), anyString())).thenReturn(mockStreamFactory);

		Whitebox.setInternalState(streamTask, "isRunning", true);
		Whitebox.setInternalState(streamTask, "lock", new Object());
		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "cancelables", new CloseableRegistry());
		Whitebox.setInternalState(streamTask, "asyncOperationsThreadPool", Executors.newFixedThreadPool(1));
		Whitebox.setInternalState(streamTask, "configuration", new StreamConfig(new Configuration()));
		Whitebox.setInternalState(streamTask, "stateBackend", mockStateBackend);

		streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forFullCheckpoint());

		acknowledgeCheckpointLatch.await();

		ArgumentCaptor<SubtaskState> subtaskStateCaptor = ArgumentCaptor.forClass(SubtaskState.class);

		// check that the checkpoint has been completed
		verify(mockEnvironment).acknowledgeCheckpoint(eq(checkpointId), any(CheckpointMetrics.class), subtaskStateCaptor.capture());

		SubtaskState subtaskState = subtaskStateCaptor.getValue();

		// check that the subtask state contains the expected state handles
		assertEquals(managedKeyedStateHandle, subtaskState.getManagedKeyedState());
		assertEquals(rawKeyedStateHandle, subtaskState.getRawKeyedState());
		assertEquals(new ChainedStateHandle<>(Collections.singletonList(managedOperatorStateHandle)), subtaskState.getManagedOperatorState());
		assertEquals(new ChainedStateHandle<>(Collections.singletonList(rawOperatorStateHandle)), subtaskState.getRawOperatorState());

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
	}

	/**
	 * FLINK-5667
	 *
	 * Tests that a concurrent cancel operation discards the state handles of a not yet
	 * acknowledged checkpoint and prevents sending an acknowledge message to the
	 * CheckpointCoordinator. The situation can only happen if the cancel call is executed
	 * before Environment.acknowledgeCheckpoint().
	 */
	@Test
	public void testAsyncCheckpointingConcurrentCloseBeforeAcknowledge() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		final OneShotLatch createSubtask = new OneShotLatch();
		final OneShotLatch completeSubtask = new OneShotLatch();

		TaskInfo mockTaskInfo = mock(TaskInfo.class);
		when(mockTaskInfo.getTaskNameWithSubtasks()).thenReturn("foobar");
		when(mockTaskInfo.getIndexOfThisSubtask()).thenReturn(0);
		Environment mockEnvironment = mock(Environment.class);
		when(mockEnvironment.getTaskInfo()).thenReturn(mockTaskInfo);

		whenNew(SubtaskState.class).withAnyArguments().thenAnswer(new Answer<SubtaskState>() {
			@Override
			public SubtaskState answer(InvocationOnMock invocation) throws Throwable {
				createSubtask.trigger();
				completeSubtask.await();

				return new SubtaskState(
					(ChainedStateHandle<StreamStateHandle>)invocation.getArguments()[0],
					(ChainedStateHandle<OperatorStateHandle>)invocation.getArguments()[1],
					(ChainedStateHandle<OperatorStateHandle>)invocation.getArguments()[2],
					(KeyedStateHandle)invocation.getArguments()[3],
					(KeyedStateHandle)invocation.getArguments()[4]);
			}
		});

		StreamTask<?, AbstractStreamOperator<?>> streamTask = mock(StreamTask.class, Mockito.CALLS_REAL_METHODS);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
		streamTask.setEnvironment(mockEnvironment);

		StreamOperator<?> streamOperator = mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));

		KeyedStateHandle managedKeyedStateHandle = mock(KeyedStateHandle.class);
		KeyedStateHandle rawKeyedStateHandle = mock(KeyedStateHandle.class);
		OperatorStateHandle managedOperatorStateHandle = mock(OperatorStateHandle.class);
		OperatorStateHandle rawOperatorStateHandle = mock(OperatorStateHandle.class);

		OperatorSnapshotResult operatorSnapshotResult = new OperatorSnapshotResult(
			new DoneFuture<>(managedKeyedStateHandle),
			new DoneFuture<>(rawKeyedStateHandle),
			new DoneFuture<>(managedOperatorStateHandle),
			new DoneFuture<>(rawOperatorStateHandle));

		when(streamOperator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class))).thenReturn(operatorSnapshotResult);

		StreamOperator<?>[] streamOperators = {streamOperator};

		OperatorChain<Void, AbstractStreamOperator<Void>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);

		StreamStateHandle streamStateHandle = mock(StreamStateHandle.class);

		CheckpointStreamFactory.CheckpointStateOutputStream outStream = mock(CheckpointStreamFactory.CheckpointStateOutputStream.class);

		when(outStream.closeAndGetHandle()).thenReturn(streamStateHandle);

		CheckpointStreamFactory mockStreamFactory = mock(CheckpointStreamFactory.class);
		when(mockStreamFactory.createCheckpointStateOutputStream(anyLong(), anyLong())).thenReturn(outStream);

		AbstractStateBackend mockStateBackend = mock(AbstractStateBackend.class);
		when(mockStateBackend.createStreamFactory(any(JobID.class), anyString())).thenReturn(mockStreamFactory);

		ExecutorService executor = Executors.newFixedThreadPool(1);

		Whitebox.setInternalState(streamTask, "isRunning", true);
		Whitebox.setInternalState(streamTask, "lock", new Object());
		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "cancelables", new CloseableRegistry());
		Whitebox.setInternalState(streamTask, "asyncOperationsThreadPool", executor);
		Whitebox.setInternalState(streamTask, "configuration", new StreamConfig(new Configuration()));
		Whitebox.setInternalState(streamTask, "stateBackend", mockStateBackend);

		streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forFullCheckpoint());

		createSubtask.await();

		streamTask.cancel();

		completeSubtask.trigger();

		// wait for the completion of the async task
		executor.shutdown();

		if (!executor.awaitTermination(10000L, TimeUnit.MILLISECONDS)) {
			fail("Executor did not shut down within the given timeout. This indicates that the " +
				"checkpointing did not resume.");
		}

		// check that the checkpoint has not been acknowledged
		verify(mockEnvironment, never()).acknowledgeCheckpoint(eq(checkpointId), any(CheckpointMetrics.class), any(SubtaskState.class));

		// check that the state handles have been discarded
		verify(managedKeyedStateHandle).discardState();
		verify(rawKeyedStateHandle).discardState();
		verify(managedOperatorStateHandle).discardState();
		verify(rawOperatorStateHandle).discardState();
	}

	/**
	 * FLINK-5985
	 *
	 * This test ensures that empty snapshots (no op/keyed stated whatsoever) will be reported as stateless tasks. This
	 * happens by translating an empty {@link SubtaskState} into reporting 'null' to #acknowledgeCheckpoint.
	 */
	@Test
	public void testEmptySubtaskStateLeadsToStatelessAcknowledgment() throws Exception {
		final long checkpointId = 42L;
		final long timestamp = 1L;

		TaskInfo mockTaskInfo = mock(TaskInfo.class);

		when(mockTaskInfo.getTaskNameWithSubtasks()).thenReturn("foobar");
		when(mockTaskInfo.getIndexOfThisSubtask()).thenReturn(0);

		Environment mockEnvironment = mock(Environment.class);

		// latch blocks until the async checkpoint thread acknowledges
		final OneShotLatch checkpointCompletedLatch = new OneShotLatch();
		final List<SubtaskState> checkpointResult = new ArrayList<>(1);

		// we remember what is acknowledged (expected to be null as our task will snapshot empty states).
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				SubtaskState subtaskState = invocationOnMock.getArgumentAt(2, SubtaskState.class);
				checkpointResult.add(subtaskState);
				checkpointCompletedLatch.trigger();
				return null;
			}
		}).when(mockEnvironment).acknowledgeCheckpoint(anyLong(), any(CheckpointMetrics.class), any(SubtaskState.class));

		when(mockEnvironment.getTaskInfo()).thenReturn(mockTaskInfo);

		StreamTask<?, AbstractStreamOperator<?>> streamTask = mock(StreamTask.class, Mockito.CALLS_REAL_METHODS);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
		streamTask.setEnvironment(mockEnvironment);

		// mock the operators
		StreamOperator<?> statelessOperator =
				mock(StreamOperator.class, withSettings().extraInterfaces(StreamCheckpointedOperator.class));

		// mock the returned empty snapshot result (all state handles are null)
		OperatorSnapshotResult statelessOperatorSnapshotResult = new OperatorSnapshotResult();
		when(statelessOperator.snapshotState(anyLong(), anyLong(), any(CheckpointOptions.class)))
				.thenReturn(statelessOperatorSnapshotResult);

		// set up the task
		StreamOperator<?>[] streamOperators = {statelessOperator};
		OperatorChain<Void, AbstractStreamOperator<Void>> operatorChain = mock(OperatorChain.class);
		when(operatorChain.getAllOperators()).thenReturn(streamOperators);

		Whitebox.setInternalState(streamTask, "isRunning", true);
		Whitebox.setInternalState(streamTask, "lock", new Object());
		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "cancelables", new CloseableRegistry());
		Whitebox.setInternalState(streamTask, "configuration", new StreamConfig(new Configuration()));
		Whitebox.setInternalState(streamTask, "asyncOperationsThreadPool", Executors.newCachedThreadPool());

		streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forFullCheckpoint());
		checkpointCompletedLatch.await(30, TimeUnit.SECONDS);
		streamTask.cancel();

		// ensure that 'null' was acknowledged as subtask state
		Assert.assertNull(checkpointResult.get(0));
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class TestingExecutionStateListener implements TaskExecutionStateListener {

		private ExecutionState executionState = null;

		private final PriorityQueue<Tuple2<ExecutionState, Promise<ExecutionState>>> priorityQueue = new PriorityQueue<>(
			1,
			new Comparator<Tuple2<ExecutionState, Promise<ExecutionState>>>() {
				@Override
				public int compare(Tuple2<ExecutionState, Promise<ExecutionState>> o1, Tuple2<ExecutionState, Promise<ExecutionState>> o2) {
					return o1.f0.ordinal() - o2.f0.ordinal();
				}
			});

		public Future<ExecutionState> notifyWhenExecutionState(ExecutionState executionState) {
			synchronized (priorityQueue) {
				if (this.executionState != null && this.executionState.ordinal() >= executionState.ordinal()) {
					return Futures.successful(executionState);
				} else {
					Promise<ExecutionState> promise = new Promise.DefaultPromise<ExecutionState>();

					priorityQueue.offer(Tuple2.of(executionState, promise));

					return promise.future();
				}
			}
		}

		@Override
		public void notifyTaskExecutionStateChanged(TaskExecutionState taskExecutionState) {
			synchronized (priorityQueue) {
				this.executionState = taskExecutionState.getExecutionState();

				while (!priorityQueue.isEmpty() && priorityQueue.peek().f0.ordinal() <= executionState.ordinal()) {
					Promise<ExecutionState> promise = priorityQueue.poll().f1;

					promise.success(executionState);
				}
			}
		}
	}

	public static Task createTask(
			Class<? extends AbstractInvokable> invokable,
			StreamConfig taskConfig,
			Configuration taskManagerConfig) throws Exception {

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(StreamTaskTest.class.getClassLoader());

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);

		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(partitionManager);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));

		JobInformation jobInformation = new JobInformation(
			new JobID(),
			"Job Name",
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.<BlobKey>emptyList(),
			Collections.<URL>emptyList());

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
			new TaskStateHandles(),
			mock(MemoryManager.class),
			mock(IOManager.class),
			network,
			mock(BroadcastVariableManager.class),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			libCache,
			mock(FileCache.class),
			new TestingTaskManagerRuntimeInfo(taskManagerConfig, new String[] {System.getProperty("java.io.tmpdir")}),
			new UnregisteredTaskMetricsGroup(),
			consumableNotifier,
			partitionProducerStateChecker,
			executor);
	}

	// ------------------------------------------------------------------------
	//  Test operators
	// ------------------------------------------------------------------------

	public static class SlowlyDeserializingOperator extends StreamSource<Long, SourceFunction<Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean canceled = false;

		public SlowlyDeserializingOperator() {
			super(new MockSourceFunction());
		}

		@Override
		public void run(Object lockingObject,
						StreamStatusMaintainer streamStatusMaintainer,
						Output<StreamRecord<Long>> collector) throws Exception {
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
	public static final class MockStateBackend implements StateBackendFactory<AbstractStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public AbstractStateBackend createFromConfig(Configuration config) {
			AbstractStateBackend stateBackendMock = mock(AbstractStateBackend.class);

			try {
				Mockito.when(stateBackendMock.createOperatorStateBackend(
						Mockito.any(Environment.class),
						Mockito.any(String.class)))
					.thenAnswer(new Answer<OperatorStateBackend>() {
						@Override
						public OperatorStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {
							return Mockito.mock(OperatorStateBackend.class);
						}
					});

				Mockito.when(stateBackendMock.createKeyedStateBackend(
						Mockito.any(Environment.class),
						Mockito.any(JobID.class),
						Mockito.any(String.class),
						Mockito.any(TypeSerializer.class),
						Mockito.any(int.class),
						Mockito.any(KeyGroupRange.class),
						Mockito.any(TaskKvStateRegistry.class)))
					.thenAnswer(new Answer<AbstractKeyedStateBackend>() {
						@Override
						public AbstractKeyedStateBackend answer(InvocationOnMock invocationOnMock) throws Throwable {
							return Mockito.mock(AbstractKeyedStateBackend.class);
						}
					});
			}
			catch (Exception e) {
				// this is needed, because the signatures of the mocked methods throw 'Exception'
				throw new RuntimeException(e);
			}

			return stateBackendMock;
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * Source that instantiates the operator state backend and the keyed state backend.
	 * The created state backends can be retrieved from the static fields to check if the
	 * CloseableRegistry closed them correctly.
	 */
	public static class StateBackendTestSource extends StreamTask<Long, StreamSource<Long, SourceFunction<Long>>> {

		private static volatile boolean fail;

		private static volatile OperatorStateBackend operatorStateBackend;
		private static volatile AbstractKeyedStateBackend keyedStateBackend;

		@Override
		protected void init() throws Exception {
			operatorStateBackend = createOperatorStateBackend(
				Mockito.mock(StreamOperator.class),
				null);
			keyedStateBackend = createKeyedStateBackend(
				Mockito.mock(TypeSerializer.class),
				4,
				Mockito.mock(KeyGroupRange.class));
		}

		@Override
		protected void run() throws Exception {
			if (fail) {
				throw new RuntimeException();
			}
		}

		@Override
		protected void cleanup() throws Exception {}

		@Override
		protected void cancelTask() throws Exception {}

	}

	/**
	 * A task that locks if cancellation attempts to cleanly shut down
	 */
	public static class CancelLockingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		private final OneShotLatch latch = new OneShotLatch();

		private LockHolder holder;

		@Override
		protected void init() {}

		@Override
		protected void run() throws Exception {
			holder = new LockHolder(getCheckpointLock(), latch);
			holder.start();
			latch.await();

			// we are at the point where cancelling can happen
			SYNC_LATCH.trigger();

			// just put this to sleep until it is interrupted
			try {
				Thread.sleep(100000000);
			} catch (InterruptedException ignored) {
				// restore interruption state
				Thread.currentThread().interrupt();
			}
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
	 * A task that locks if cancellation attempts to cleanly shut down
	 */
	public static class CancelFailingTask extends StreamTask<String, AbstractStreamOperator<String>> {

		@Override
		protected void init() {}

		@Override
		protected void run() throws Exception {
			final OneShotLatch latch = new OneShotLatch();
			final Object lock = new Object();

			LockHolder holder = new LockHolder(lock, latch);
			holder.start();
			try {
				// cancellation should try and cancel this
				getCancelables().registerClosable(holder);

				// wait till the lock holder has the lock
				latch.await();

				// we are at the point where cancelling can happen
				SYNC_LATCH.trigger();

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

		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() throws Exception {
			throw new Exception("test exception");
		}

	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * A thread that holds a lock as long as it lives
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
}
