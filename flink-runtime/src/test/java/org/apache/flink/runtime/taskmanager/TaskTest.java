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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.WrappingRuntimeException;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the Task, which make sure that correct state transitions happen,
 * and failures are correctly handled.
 */
public class TaskTest extends TestLogger {

	private static OneShotLatch awaitLatch;
	private static OneShotLatch triggerLatch;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() {
		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
	}

	@Test
	public void testRegularExecution() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setTaskManagerActions(taskManagerActions)
			.build();

		// task should be new and perfect
		assertEquals(ExecutionState.CREATED, task.getExecutionState());
		assertFalse(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());

		// go into the run method. we should switch to DEPLOYING, RUNNING, then
		// FINISHED, and all should be good
		task.run();

		// verify final state
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
		assertFalse(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());
		assertNull(task.getInvokable());

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FINISHED, task, null);
	}

	@Test
	public void testCancelRightAway() throws Exception {
		final Task task = new TaskBuilder().build();
		task.cancelExecution();

		assertEquals(ExecutionState.CANCELING, task.getExecutionState());

		task.run();

		// verify final state
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());

		assertNull(task.getInvokable());
	}

	@Test
	public void testFailExternallyRightAway() throws Exception {
		final Task task = new TaskBuilder().build();
		task.failExternally(new Exception("fail externally"));

		assertEquals(ExecutionState.FAILED, task.getExecutionState());

		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
	}

	@Test
	public void testLibraryCacheRegistrationFailed() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setTaskManagerActions(taskManagerActions)
			.setLibraryCacheManager(mock(LibraryCacheManager.class)) // inactive manager
			.build();

		// task should be new and perfect
		assertEquals(ExecutionState.CREATED, task.getExecutionState());
		assertFalse(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());

		// should fail
		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertNotNull(task.getFailureCause());
		assertNotNull(task.getFailureCause().getMessage());
		assertTrue(task.getFailureCause().getMessage().contains("classloader"));

		assertNull(task.getInvokable());

		taskManagerActions.validateListenerMessage(
			ExecutionState.FAILED, task, new Exception("No user code classloader available."));
	}

	@Test
	public void testExecutionFailsInBlobsMissing() throws Exception {
		final PermanentBlobKey missingKey = new PermanentBlobKey();

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			TEMPORARY_FOLDER.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

		final BlobServer blobServer = new BlobServer(config, new VoidBlobStore());
		blobServer.start();
		InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());
		final PermanentBlobCache permanentBlobCache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress);

		final BlobLibraryCacheManager libraryCacheManager =
			new BlobLibraryCacheManager(
				permanentBlobCache,
				FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
				new String[0]);

		final Task task = new TaskBuilder()
			.setRequiredJarFileBlobKeys(Collections.singletonList(missingKey))
			.setLibraryCacheManager(libraryCacheManager)
			.build();

		// task should be new and perfect
		assertEquals(ExecutionState.CREATED, task.getExecutionState());
		assertFalse(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());

		// should fail
		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertNotNull(task.getFailureCause());
		assertNotNull(task.getFailureCause().getMessage());
		assertTrue(task.getFailureCause().getMessage().contains("Failed to fetch BLOB"));

		assertNull(task.getInvokable());
	}

	@Test
	public void testExecutionFailsInNetworkRegistration() throws Exception {
		// mock a network manager that rejects registration
		final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		final ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		final PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);

		final NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(partitionManager);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);
		doThrow(new RuntimeException("buffers")).when(network).registerTask(any(Task.class));

		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setTaskManagerActions(taskManagerActions)
			.setConsumableNotifier(consumableNotifier)
			.setPartitionProducerStateChecker(partitionProducerStateChecker)
			.setNetworkEnvironment(network)
			.build();

		// should fail
		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("buffers"));

		taskManagerActions.validateListenerMessage(
			ExecutionState.FAILED, task, new RuntimeException("buffers"));
	}

	@Test
	public void testInvokableInstantiationFailed() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setTaskManagerActions(taskManagerActions)
			.setInvokable(InvokableNonInstantiable.class)
			.build();

		// should fail
		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("instantiate"));

		taskManagerActions.validateListenerMessage(
			ExecutionState.FAILED, task, new FlinkException("Could not instantiate the task's invokable class."));
	}

	@Test
	public void testExecutionFailsInInvoke() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithExceptionInInvoke.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		task.run();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertNotNull(task.getFailureCause());
		assertNotNull(task.getFailureCause().getMessage());
		assertTrue(task.getFailureCause().getMessage().contains("test"));

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new Exception("test"));
	}

	@Test
	public void testFailWithWrappedException() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(FailingInvokableWithChainedException.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		task.run();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());

		final Throwable cause = task.getFailureCause();
		assertTrue(cause instanceof IOException);

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new IOException("test"));
	}

	@Test
	public void testCancelDuringInvoke() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		// run the task asynchronous
		task.startTaskThread();

		// wait till the task is in invoke
		awaitLatch.await();

		task.cancelExecution();
		assertTrue(task.getExecutionState() == ExecutionState.CANCELING ||
			task.getExecutionState() == ExecutionState.CANCELED);

		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.CANCELED, task, null);
	}

	@Test
	public void testFailExternallyDuringInvoke() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		// run the task asynchronous
		task.startTaskThread();

		// wait till the task is in invoke
		awaitLatch.await();

		task.failExternally(new Exception("test"));

		task.getExecutingThread().join();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("test"));

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new Exception("test"));
	}

	@Test
	public void testCanceledAfterExecutionFailedInInvoke() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithExceptionInInvoke.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		task.run();

		// this should not overwrite the failure state
		task.cancelExecution();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("test"));

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new Exception("test"));
	}

	@Test
	public void testExecutionFailsAfterCanceling() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithExceptionOnTrigger.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		// run the task asynchronous
		task.startTaskThread();

		// wait till the task is in invoke
		awaitLatch.await();

		task.cancelExecution();
		assertEquals(ExecutionState.CANCELING, task.getExecutionState());

		// this causes an exception
		triggerLatch.trigger();

		task.getExecutingThread().join();

		// we should still be in state canceled
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertNull(task.getFailureCause());

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.CANCELED, task, null);
	}

	@Test
	public void testExecutionFailsAfterTaskMarkedFailed() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithExceptionOnTrigger.class)
			.setTaskManagerActions(taskManagerActions)
			.build();

		// run the task asynchronous
		task.startTaskThread();

		// wait till the task is in invoke
		awaitLatch.await();

		task.failExternally(new Exception("external"));
		assertEquals(ExecutionState.FAILED, task.getExecutionState());

		// this causes an exception
		triggerLatch.trigger();

		task.getExecutingThread().join();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("external"));

		taskManagerActions.validateListenerMessage(ExecutionState.RUNNING, task, null);
		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new Exception("external"));
	}

	@Test
	public void testCancelTaskException() throws Exception {
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithCancelTaskExceptionInInvoke.class)
			.build();

		// Cause CancelTaskException.
		triggerLatch.trigger();

		task.run();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testCancelTaskExceptionAfterTaskMarkedFailed() throws Exception {
		final Task task = new TaskBuilder()
			.setInvokable(InvokableWithCancelTaskExceptionInInvoke.class)
			.build();

		task.startTaskThread();

		// Wait till the task is in invoke.
		awaitLatch.await();

		task.failExternally(new Exception("external"));
		assertEquals(ExecutionState.FAILED, task.getExecutionState());

		// Either we cause the CancelTaskException or the TaskCanceler
		// by interrupting the invokable.
		triggerLatch.trigger();

		task.getExecutingThread().join();

		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains("external"));
	}

	@Test
	public void testOnPartitionStateUpdate() throws Exception {
		final IntermediateDataSetID resultId = new IntermediateDataSetID();
		final ResultPartitionID partitionId = new ResultPartitionID();

		final SingleInputGate inputGate = mock(SingleInputGate.class);
		when(inputGate.getConsumedResultId()).thenReturn(resultId);

		final Task task = new TaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.build();

		// Set the mock input gate
		setInputGate(task, inputGate);

		// Expected task state for each producer state
		final Map<ExecutionState, ExecutionState> expected = new HashMap<>(ExecutionState.values().length);

		// Fail the task for unexpected states
		for (ExecutionState state : ExecutionState.values()) {
			expected.put(state, ExecutionState.FAILED);
		}

		expected.put(ExecutionState.RUNNING, ExecutionState.RUNNING);
		expected.put(ExecutionState.SCHEDULED, ExecutionState.RUNNING);
		expected.put(ExecutionState.DEPLOYING, ExecutionState.RUNNING);
		expected.put(ExecutionState.FINISHED, ExecutionState.RUNNING);

		expected.put(ExecutionState.CANCELED, ExecutionState.CANCELING);
		expected.put(ExecutionState.CANCELING, ExecutionState.CANCELING);
		expected.put(ExecutionState.FAILED, ExecutionState.CANCELING);

		for (ExecutionState state : ExecutionState.values()) {
			setState(task, ExecutionState.RUNNING);

			task.onPartitionStateUpdate(resultId, partitionId, state);

			ExecutionState newTaskState = task.getExecutionState();

			assertEquals(expected.get(state), newTaskState);
		}

		verify(inputGate, times(4)).retriggerPartitionRequest(eq(partitionId.getPartitionId()));
	}

	/**
	 * Tests the trigger partition state update future completions.
	 */
	@Test
	public void testTriggerPartitionStateUpdate() throws Exception {
		final IntermediateDataSetID resultId = new IntermediateDataSetID();
		final ResultPartitionID partitionId = new ResultPartitionID();

		final PartitionProducerStateChecker partitionChecker = mock(PartitionProducerStateChecker.class);
		final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);

		final ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		final NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(mock(ResultPartitionManager.class));
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
			.thenReturn(mock(TaskKvStateRegistry.class));
		when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		// Test all branches of trigger partition state check
		{
			// Reset latches
			setup();

			// PartitionProducerDisposedException
			final Task task =  new TaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setNetworkEnvironment(network)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

			promise.completeExceptionally(new PartitionProducerDisposedException(partitionId));
			assertEquals(ExecutionState.CANCELING, task.getExecutionState());
		}

		{
			// Reset latches
			setup();

			// Any other exception
			final Task task =  new TaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setNetworkEnvironment(network)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

			promise.completeExceptionally(new RuntimeException("Any other exception"));

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
		}

		{
			// Reset latches
			setup();

			// TimeoutException handled special => retry
			// Any other exception
			final Task task =  new TaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setNetworkEnvironment(network)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			final SingleInputGate inputGate = mock(SingleInputGate.class);
			when(inputGate.getConsumedResultId()).thenReturn(resultId);

			try {
				task.startTaskThread();
				awaitLatch.await();

				setInputGate(task, inputGate);

				CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
				when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

				task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

				promise.completeExceptionally(new TimeoutException());

				assertEquals(ExecutionState.RUNNING, task.getExecutionState());

				verify(inputGate, times(1)).retriggerPartitionRequest(eq(partitionId.getPartitionId()));
			} finally {
				task.getExecutingThread().interrupt();
				task.getExecutingThread().join();
			}
		}

		{
			// Reset latches
			setup();

			// Success
			final Task task =  new TaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setNetworkEnvironment(network)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			final SingleInputGate inputGate = mock(SingleInputGate.class);
			when(inputGate.getConsumedResultId()).thenReturn(resultId);

			try {
				task.startTaskThread();
				awaitLatch.await();

				setInputGate(task, inputGate);

				CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
				when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

				task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

				promise.complete(ExecutionState.RUNNING);

				assertEquals(ExecutionState.RUNNING, task.getExecutionState());

				verify(inputGate, times(1)).retriggerPartitionRequest(eq(partitionId.getPartitionId()));
			} finally {
				task.getExecutingThread().interrupt();
				task.getExecutingThread().join();
			}
		}
	}

	/**
	 * Tests that interrupt happens via watch dog if canceller is stuck in cancel.
	 * Task cancellation blocks the task canceller. Interrupt after cancel via
	 * cancellation watch dog.
	 */
	@Test
	public void testWatchDogInterruptsTask() throws Exception {
		final TaskManagerActions taskManagerActions = new ProhibitFatalErrorTaskManagerActions();

		final Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL.key(), 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT.key(), 60 * 1000);

		final Task task = new TaskBuilder()
			.setInvokable(InvokableBlockingInCancel.class)
			.setTaskManagerConfig(config)
			.setTaskManagerActions(taskManagerActions)
			.build();

		task.startTaskThread();

		awaitLatch.await();

		task.cancelExecution();
		task.getExecutingThread().join();
	}

	/**
	 * The invoke() method holds a lock (trigger awaitLatch after acquisition)
	 * and cancel cannot complete because it also tries to acquire the same lock.
	 * This is resolved by the watch dog, no fatal error.
	 */
	@Test
	public void testInterruptibleSharedLockInInvokeAndCancel() throws Exception {
		final TaskManagerActions taskManagerActions = new ProhibitFatalErrorTaskManagerActions();

		final Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 50);

		final Task task = new TaskBuilder()
			.setInvokable(InvokableInterruptibleSharedLockInInvokeAndCancel.class)
			.setTaskManagerConfig(config)
			.setTaskManagerActions(taskManagerActions)
			.build();

		task.startTaskThread();

		awaitLatch.await();

		task.cancelExecution();
		task.getExecutingThread().join();
	}

	/**
	 * The invoke() method blocks infinitely, but cancel() does not block. Only
	 * resolved by a fatal error.
	 */
	@Test
	public void testFatalErrorAfterUnInterruptibleInvoke() throws Exception {
		final AwaitFatalErrorTaskManagerActions taskManagerActions =
			new AwaitFatalErrorTaskManagerActions();

		final Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 50);

		final Task task = new TaskBuilder()
			.setInvokable(InvokableUnInterruptibleBlockingInvoke.class)
			.setTaskManagerConfig(config)
			.setTaskManagerActions(taskManagerActions)
			.build();

		try {
			task.startTaskThread();

			awaitLatch.await();

			task.cancelExecution();

			// wait for the notification of notifyFatalError
			taskManagerActions.latch.await();
		} finally {
			// Interrupt again to clean up Thread
			triggerLatch.trigger();
			task.getExecutingThread().interrupt();
			task.getExecutingThread().join();
		}
	}

	/**
	 * Tests that the task configuration is respected and overwritten by the execution config.
	 */
	@Test
	public void testTaskConfig() throws Exception {
		long interval = 28218123;
		long timeout = interval + 19292;

		final Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, interval);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, timeout);

		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setTaskCancellationInterval(interval + 1337);
		executionConfig.setTaskCancellationTimeout(timeout - 1337);

		final Task task = new TaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.setTaskManagerConfig(config)
			.setExecutionConfig(executionConfig)
			.build();

		assertEquals(interval, task.getTaskCancellationInterval());
		assertEquals(timeout, task.getTaskCancellationTimeout());

		task.startTaskThread();

		awaitLatch.await();

		assertEquals(executionConfig.getTaskCancellationInterval(), task.getTaskCancellationInterval());
		assertEquals(executionConfig.getTaskCancellationTimeout(), task.getTaskCancellationTimeout());

		task.getExecutingThread().interrupt();
		task.getExecutingThread().join();
	}

	// ------------------------------------------------------------------------
	//  customized TaskManagerActions
	// ------------------------------------------------------------------------

	/**
	 * Customized TaskManagerActions that queues all calls of updateTaskExecutionState.
	 */
	private static class QueuedNoOpTaskManagerActions extends NoOpTaskManagerActions {
		private final BlockingQueue<TaskExecutionState> queue = new LinkedBlockingDeque<>();

		@Override
		public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
			queue.offer(taskExecutionState);
		}

		private void validateListenerMessage(ExecutionState state, Task task, Throwable error) {
			try {
				// we may have to wait for a bit to give the actors time to receive the message
				// and put it into the queue
				final TaskExecutionState taskState = queue.take();
				assertNotNull("There is no additional listener message", state);

				assertEquals(task.getJobID(), taskState.getJobID());
				assertEquals(task.getExecutionId(), taskState.getID());
				assertEquals(state, taskState.getExecutionState());

				final Throwable t = taskState.getError(getClass().getClassLoader());
				if (error == null) {
					assertNull(t);
				} else {
					assertEquals(error.toString(), t.toString());
				}
			} catch (InterruptedException e) {
				fail("interrupted");
			}
		}
	}

	/**
	 * Customized TaskManagerActions that ensures no call of notifyFatalError.
	 */
	private static class ProhibitFatalErrorTaskManagerActions extends NoOpTaskManagerActions {
		@Override
		public void notifyFatalError(String message, Throwable cause) {
			throw new RuntimeException("Unexpected FatalError notification");
		}
	}

	/**
	 * Customized TaskManagerActions that waits for a call of notifyFatalError.
	 */
	private static class AwaitFatalErrorTaskManagerActions extends NoOpTaskManagerActions {
		private final OneShotLatch latch = new OneShotLatch();

		@Override
		public void notifyFatalError(String message, Throwable cause) {
			latch.trigger();
		}
	}

	// ------------------------------------------------------------------------
	//  helper functions
	// ------------------------------------------------------------------------

	private void setInputGate(Task task, SingleInputGate inputGate) {
		try {
			Field f = Task.class.getDeclaredField("inputGates");
			f.setAccessible(true);
			f.set(task, new SingleInputGate[]{inputGate});

			Map<IntermediateDataSetID, SingleInputGate> byId = new HashMap<>(1);
			byId.put(inputGate.getConsumedResultId(), inputGate);

			f = Task.class.getDeclaredField("inputGatesById");
			f.setAccessible(true);
			f.set(task, byId);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the task state failed", e);
		}
	}

	private void setState(Task task, ExecutionState state) {
		try {
			Field f = Task.class.getDeclaredField("executionState");
			f.setAccessible(true);
			f.set(task, state);
		}
		catch (Exception e) {
			throw new RuntimeException("Modifying the task state failed", e);
		}
	}

	private final class TaskBuilder {
		private Class<? extends AbstractInvokable> invokable;
		private TaskManagerActions taskManagerActions;
		private LibraryCacheManager libraryCacheManager;
		private ResultPartitionConsumableNotifier consumableNotifier;
		private PartitionProducerStateChecker partitionProducerStateChecker;
		private NetworkEnvironment networkEnvironment;
		private Executor executor;
		private Configuration taskManagerConfig;
		private ExecutionConfig executionConfig;
		private Collection<PermanentBlobKey> requiredJarFileBlobKeys;

		{
			invokable = TestInvokableCorrect.class;
			taskManagerActions = mock(TaskManagerActions.class);

			libraryCacheManager = mock(LibraryCacheManager.class);
			when(libraryCacheManager.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());

			consumableNotifier = new NoOpResultPartitionConsumableNotifier();
			partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);

			final ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
			final TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
			networkEnvironment = mock(NetworkEnvironment.class);
			when(networkEnvironment.getResultPartitionManager()).thenReturn(partitionManager);
			when(networkEnvironment.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
			when(networkEnvironment.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));
			when(networkEnvironment.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

			executor = TestingUtils.defaultExecutor();

			taskManagerConfig = new Configuration();
			executionConfig = new ExecutionConfig();

			requiredJarFileBlobKeys = Collections.emptyList();
		}

		TaskBuilder setInvokable(Class<? extends AbstractInvokable> invokable) {
			this.invokable = invokable;
			return this;
		}

		TaskBuilder setTaskManagerActions(TaskManagerActions taskManagerActions) {
			this.taskManagerActions = taskManagerActions;
			return this;
		}

		TaskBuilder setLibraryCacheManager(LibraryCacheManager libraryCacheManager) {
			this.libraryCacheManager = libraryCacheManager;
			return this;
		}

		TaskBuilder setConsumableNotifier(ResultPartitionConsumableNotifier consumableNotifier) {
			this.consumableNotifier = consumableNotifier;
			return this;
		}

		TaskBuilder setPartitionProducerStateChecker(PartitionProducerStateChecker partitionProducerStateChecker) {
			this.partitionProducerStateChecker = partitionProducerStateChecker;
			return this;
		}

		TaskBuilder setNetworkEnvironment(NetworkEnvironment networkEnvironment) {
			this.networkEnvironment = networkEnvironment;
			return this;
		}

		TaskBuilder setExecutor(Executor executor) {
			this.executor = executor;
			return this;
		}

		TaskBuilder setTaskManagerConfig(Configuration taskManagerConfig) {
			this.taskManagerConfig = taskManagerConfig;
			return this;
		}

		TaskBuilder setExecutionConfig(ExecutionConfig executionConfig) {
			this.executionConfig = executionConfig;
			return this;
		}

		TaskBuilder setRequiredJarFileBlobKeys(Collection<PermanentBlobKey> requiredJarFileBlobKeys) {
			this.requiredJarFileBlobKeys = requiredJarFileBlobKeys;
			return this;
		}

		private Task build() throws Exception {
			final JobID jobId = new JobID();
			final JobVertexID jobVertexId = new JobVertexID();
			final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

			final SerializedValue<ExecutionConfig> serializedExecutionConfig = new SerializedValue<>(executionConfig);

			final JobInformation jobInformation = new JobInformation(
				jobId,
				"Test Job",
				serializedExecutionConfig,
				new Configuration(),
				requiredJarFileBlobKeys,
				Collections.emptyList());

			final TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				"Test Task",
				1,
				1,
				invokable.getName(),
				new Configuration());

			final BlobCacheService blobCacheService = new BlobCacheService(
				mock(PermanentBlobCache.class),
				mock(TransientBlobCache.class));

			final TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
			when(taskMetricGroup.getIOMetricGroup()).thenReturn(mock(TaskIOMetricGroup.class));

			return new Task(
				jobInformation,
				taskInformation,
				executionAttemptId,
				new AllocationID(),
				0,
				0,
				Collections.emptyList(),
				Collections.emptyList(),
				0,
				mock(MemoryManager.class),
				mock(IOManager.class),
				networkEnvironment,
				mock(BroadcastVariableManager.class),
				new TestTaskStateManager(),
				taskManagerActions,
				new MockInputSplitProvider(),
				new TestCheckpointResponder(),
				blobCacheService,
				libraryCacheManager,
				mock(FileCache.class),
				new TestingTaskManagerRuntimeInfo(taskManagerConfig),
				taskMetricGroup,
				consumableNotifier,
				partitionProducerStateChecker,
				executor);
		}
	}

	// ------------------------------------------------------------------------
	//  test task classes
	// ------------------------------------------------------------------------

	private static final class TestInvokableCorrect extends AbstractInvokable {
		public TestInvokableCorrect(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {}

		@Override
		public void cancel() {
			fail("This should not be called");
		}
	}

	private abstract static class InvokableNonInstantiable extends AbstractInvokable {
		public InvokableNonInstantiable(Environment environment) {
			super(environment);
		}
	}

	private static final class InvokableWithExceptionInInvoke extends AbstractInvokable {
		public InvokableWithExceptionInInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			throw new Exception("test");
		}
	}

	private static final class FailingInvokableWithChainedException extends AbstractInvokable {
		public FailingInvokableWithChainedException(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			throw new TestWrappedException(new IOException("test"));
		}

		@Override
		public void cancel() {}
	}

	private static final class InvokableBlockingInInvoke extends AbstractInvokable {
		public InvokableBlockingInInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			// block forever
			synchronized (this) {
				wait();
			}
		}
	}

	/**
	 * {@link AbstractInvokable} which throws {@link RuntimeException} on invoke.
	 */
	public static final class InvokableWithExceptionOnTrigger extends AbstractInvokable {
		public InvokableWithExceptionOnTrigger(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			awaitLatch.trigger();

			// make sure that the interrupt call does not
			// grab us out of the lock early
			while (true) {
				try {
					triggerLatch.await();
					break;
				}
				catch (InterruptedException e) {
					// fall through the loop
				}
			}

			throw new RuntimeException("test");
		}
	}

	/**
	 * {@link AbstractInvokable} which throws {@link CancelTaskException} on invoke.
	 */
	public static final class InvokableWithCancelTaskExceptionInInvoke extends AbstractInvokable {
		public InvokableWithCancelTaskExceptionInInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			awaitLatch.trigger();

			try {
				triggerLatch.await();
			}
			catch (Throwable ignored) {}

			throw new CancelTaskException();
		}
	}

	/**
	 * {@link AbstractInvokable} which blocks in cancel.
	 */
	public static final class InvokableBlockingInCancel extends AbstractInvokable {
		public InvokableBlockingInCancel(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			awaitLatch.trigger();

			try {
				triggerLatch.await(); // await cancel
				synchronized (this) {
					wait();
				}
			} catch (InterruptedException ignored) {
				synchronized (this) {
					notifyAll(); // notify all that are stuck in cancel
				}
			}
		}

		@Override
		public void cancel() throws Exception {
			synchronized (this) {
				triggerLatch.trigger();
				wait();
			}
		}
	}

	/**
	 * {@link AbstractInvokable} which blocks in cancel and is interruptible.
	 */
	public static final class InvokableInterruptibleSharedLockInInvokeAndCancel extends AbstractInvokable {
		private final Object lock = new Object();

		public InvokableInterruptibleSharedLockInInvokeAndCancel(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			synchronized (lock) {
				awaitLatch.trigger();
				wait();
			}
		}

		@Override
		public void cancel() {
			synchronized (lock) {
				// do nothing but a placeholder
				triggerLatch.trigger();
			}
		}
	}

	/**
	 * {@link AbstractInvokable} which blocks in cancel and is not interruptible.
	 */
	public static final class InvokableUnInterruptibleBlockingInvoke extends AbstractInvokable {
		public InvokableUnInterruptibleBlockingInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			while (!triggerLatch.isTriggered()) {
				try {
					synchronized (this) {
						awaitLatch.trigger();
						wait();
					}
				} catch (InterruptedException ignored) {
				}
			}
		}

		@Override
		public void cancel() {
		}
	}

	// ------------------------------------------------------------------------
	//  test exceptions
	// ------------------------------------------------------------------------

	private static class TestWrappedException extends WrappingRuntimeException {
		private static final long serialVersionUID = 1L;

		TestWrappedException(@Nonnull Throwable cause) {
			super(cause);
		}
	}
}
