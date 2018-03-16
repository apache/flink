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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.WrappingRuntimeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the Task, which make sure that correct state transitions happen,
 * and failures are correctly handled.
 *
 * <p>All tests here have a set of mock actors for TaskManager, JobManager, and
 * execution listener, which simply put the messages in a queue to be picked
 * up by the test and validated.
 */
public class TaskTest extends TestLogger {

	private static OneShotLatch awaitLatch;
	private static OneShotLatch triggerLatch;
	private static OneShotLatch cancelLatch;

	private ActorGateway taskManagerGateway;
	private ActorGateway jobManagerGateway;
	private ActorGateway listenerGateway;

	private ActorGatewayTaskExecutionStateListener listener;
	private ActorGatewayTaskManagerActions taskManagerConnection;

	private BlockingQueue<Object> taskManagerMessages;
	private BlockingQueue<Object> jobManagerMessages;
	private BlockingQueue<Object> listenerMessages;

	@Before
	public void createQueuesAndActors() {
		taskManagerMessages = new LinkedBlockingQueue<>();
		jobManagerMessages = new LinkedBlockingQueue<>();
		listenerMessages = new LinkedBlockingQueue<>();
		taskManagerGateway = new ForwardingActorGateway(taskManagerMessages);
		jobManagerGateway = new ForwardingActorGateway(jobManagerMessages);
		listenerGateway = new ForwardingActorGateway(listenerMessages);

		listener = new ActorGatewayTaskExecutionStateListener(listenerGateway);
		taskManagerConnection = new ActorGatewayTaskManagerActions(taskManagerGateway);

		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
		cancelLatch = new OneShotLatch();
	}

	@After
	public void clearActorsAndMessages() {
		jobManagerMessages = null;
		taskManagerMessages = null;
		listenerMessages = null;

		taskManagerGateway = null;
		jobManagerGateway = null;
		listenerGateway = null;
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testRegularExecution() {
		try {
			Task task = createTask(TestInvokableCorrect.class);

			// task should be new and perfect
			assertEquals(ExecutionState.CREATED, task.getExecutionState());
			assertFalse(task.isCanceledOrFailed());
			assertNull(task.getFailureCause());

			task.registerExecutionListener(listener);

			// go into the run method. we should switch to DEPLOYING, RUNNING, then
			// FINISHED, and all should be good
			task.run();

			// verify final state
			assertEquals(ExecutionState.FINISHED, task.getExecutionState());
			assertFalse(task.isCanceledOrFailed());
			assertNull(task.getFailureCause());

			// verify listener messages
			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FINISHED, task, false);

			// make sure that the TaskManager received an message to unregister the task
			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelRightAway() {
		try {
			Task task = createTask(TestInvokableCorrect.class);
			task.cancelExecution();

			assertEquals(ExecutionState.CANCELING, task.getExecutionState());

			task.run();

			// verify final state
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			validateUnregisterTask(task.getExecutionId());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFailExternallyRightAway() {
		try {
			Task task = createTask(TestInvokableCorrect.class);
			task.failExternally(new Exception("fail externally"));

			assertEquals(ExecutionState.FAILED, task.getExecutionState());

			task.run();

			// verify final state
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			validateUnregisterTask(task.getExecutionId());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testLibraryCacheRegistrationFailed() {
		try {
			BlobCacheService blobService = createBlobCache();
			Task task = createTask(TestInvokableCorrect.class, blobService,
				mock(LibraryCacheManager.class));

			// task should be new and perfect
			assertEquals(ExecutionState.CREATED, task.getExecutionState());
			assertFalse(task.isCanceledOrFailed());
			assertNull(task.getFailureCause());

			task.registerExecutionListener(listener);

			// should fail
			task.run();

			// verify final state
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertNotNull(task.getFailureCause());
			assertNotNull(task.getFailureCause().getMessage());
			assertTrue(task.getFailureCause().getMessage().contains("classloader"));

			// verify listener messages
			validateListenerMessage(ExecutionState.FAILED, task, true);

			// make sure that the TaskManager received an message to unregister the task
			validateUnregisterTask(task.getExecutionId());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testExecutionFailsInNetworkRegistration() {
		try {
			BlobCacheService blobService = createBlobCache();
			// mock a working library cache
			LibraryCacheManager libCache = mock(LibraryCacheManager.class);
			when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());

			// mock a network manager that rejects registration
			ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
			ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
			PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
			TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
			Executor executor = mock(Executor.class);
			NetworkEnvironment network = mock(NetworkEnvironment.class);
			when(network.getResultPartitionManager()).thenReturn(partitionManager);
			when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
			when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);
			doThrow(new RuntimeException("buffers")).when(network).registerTask(any(Task.class));

			Task task = createTask(TestInvokableCorrect.class, blobService, libCache, network, consumableNotifier, partitionProducerStateChecker, executor);

			task.registerExecutionListener(listener);

			task.run();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("buffers"));

			validateUnregisterTask(task.getExecutionId());
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInvokableInstantiationFailed() {
		try {
			Task task = createTask(InvokableNonInstantiable.class);
			task.registerExecutionListener(listener);

			task.run();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("instantiate"));

			validateUnregisterTask(task.getExecutionId());
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testExecutionFailsInInvoke() {
		try {
			Task task = createTask(InvokableWithExceptionInInvoke.class);
			task.registerExecutionListener(listener);

			task.run();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertNotNull(task.getFailureCause());
			assertNotNull(task.getFailureCause().getMessage());
			assertTrue(task.getFailureCause().getMessage().contains("test"));

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFailWithWrappedException() {
		try {
			Task task = createTask(FailingInvokableWithChainedException.class);
			task.registerExecutionListener(listener);

			task.run();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());

			Throwable cause = task.getFailureCause();
			assertTrue(cause instanceof IOException);

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelDuringInvoke() {
		try {
			Task task = createTask(InvokableBlockingInInvoke.class);
			task.registerExecutionListener(listener);

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

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateCancelingAndCanceledListenerMessage(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFailExternallyDuringInvoke() {
		try {
			Task task = createTask(InvokableBlockingInInvoke.class);
			task.registerExecutionListener(listener);

			// run the task asynchronous
			task.startTaskThread();

			// wait till the task is in regInOut
			awaitLatch.await();

			task.failExternally(new Exception("test"));
			assertTrue(task.getExecutionState() == ExecutionState.FAILED);

			task.getExecutingThread().join();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("test"));

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCanceledAfterExecutionFailedInInvoke() {
		try {
			Task task = createTask(InvokableWithExceptionInInvoke.class);
			task.registerExecutionListener(listener);

			task.run();

			// this should not overwrite the failure state
			task.cancelExecution();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("test"));

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testExecutionFailesAfterCanceling() {
		try {
			Task task = createTask(InvokableWithExceptionOnTrigger.class);
			task.registerExecutionListener(listener);

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

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateCancelingAndCanceledListenerMessage(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testExecutionFailsAfterTaskMarkedFailed() {
		try {
			Task task = createTask(InvokableWithExceptionOnTrigger.class);
			task.registerExecutionListener(listener);

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

			validateTaskManagerStateChange(ExecutionState.RUNNING, task, false);
			validateUnregisterTask(task.getExecutionId());

			validateListenerMessage(ExecutionState.RUNNING, task, false);
			validateListenerMessage(ExecutionState.FAILED, task, true);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelTaskException() throws Exception {
		final Task task = createTask(InvokableWithCancelTaskExceptionInInvoke.class);

		// Cause CancelTaskException.
		triggerLatch.trigger();

		task.run();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testCancelTaskExceptionAfterTaskMarkedFailed() throws Exception {
		final Task task = createTask(InvokableWithCancelTaskExceptionInInvoke.class);

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
		IntermediateDataSetID resultId = new IntermediateDataSetID();
		ResultPartitionID partitionId = new ResultPartitionID();

		SingleInputGate inputGate = mock(SingleInputGate.class);
		when(inputGate.getConsumedResultId()).thenReturn(resultId);

		final Task task = createTask(InvokableBlockingInInvoke.class);

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
		IntermediateDataSetID resultId = new IntermediateDataSetID();
		ResultPartitionID partitionId = new ResultPartitionID();

		BlobCacheService blobService = createBlobCache();
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());

		PartitionProducerStateChecker partitionChecker = mock(PartitionProducerStateChecker.class);
		TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);

		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(mock(ResultPartitionManager.class));
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
			.thenReturn(mock(TaskKvStateRegistry.class));
		when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		createTask(InvokableBlockingInInvoke.class, blobService, libCache, network, consumableNotifier, partitionChecker, Executors.directExecutor());

		// Test all branches of trigger partition state check

		{
			// Reset latches
			createQueuesAndActors();

			// PartitionProducerDisposedException
			Task task = createTask(InvokableBlockingInInvoke.class, blobService, libCache, network, consumableNotifier, partitionChecker, Executors.directExecutor());

			CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

			promise.completeExceptionally(new PartitionProducerDisposedException(partitionId));
			assertEquals(ExecutionState.CANCELING, task.getExecutionState());
		}

		{
			// Reset latches
			createQueuesAndActors();

			// Any other exception
			Task task = createTask(InvokableBlockingInInvoke.class, blobService, libCache, network, consumableNotifier, partitionChecker, Executors.directExecutor());

			CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.triggerPartitionProducerStateCheck(task.getJobID(), resultId, partitionId);

			promise.completeExceptionally(new RuntimeException("Any other exception"));

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
		}

		{
			// Reset latches
			createQueuesAndActors();

			// TimeoutException handled special => retry
			Task task = createTask(InvokableBlockingInInvoke.class, blobService, libCache, network, consumableNotifier, partitionChecker, Executors.directExecutor());
			SingleInputGate inputGate = mock(SingleInputGate.class);
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
			createQueuesAndActors();

			// Success
			Task task = createTask(InvokableBlockingInInvoke.class, blobService, libCache, network, consumableNotifier, partitionChecker, Executors.directExecutor());
			SingleInputGate inputGate = mock(SingleInputGate.class);
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
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL.key(), 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT.key(), 60 * 1000);

		Task task = createTask(InvokableBlockingInCancel.class, config);
		task.startTaskThread();

		awaitLatch.await();

		task.cancelExecution();
		task.getExecutingThread().join();

		// No fatal error
		for (Object msg : taskManagerMessages) {
			assertFalse("Unexpected FatalError message", msg instanceof TaskManagerMessages.FatalError);
		}
	}

	/**
	 * The invoke() method holds a lock (trigger awaitLatch after acquisition)
	 * and cancel cannot complete because it also tries to acquire the same lock.
	 * This is resolved by the watch dog, no fatal error.
	 */
	@Test
	public void testInterruptableSharedLockInInvokeAndCancel() throws Exception {
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 50);

		Task task = createTask(InvokableInterruptableSharedLockInInvokeAndCancel.class, config);
		task.startTaskThread();

		awaitLatch.await();

		task.cancelExecution();
		task.getExecutingThread().join();

		// No fatal error
		for (Object msg : taskManagerMessages) {
			assertFalse("Unexpected FatalError message", msg instanceof TaskManagerMessages.FatalError);
		}
	}

	/**
	 * The invoke() method blocks infinitely, but cancel() does not block. Only
	 * resolved by a fatal error.
	 */
	@Test
	public void testFatalErrorAfterUninterruptibleInvoke() throws Exception {
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, 5);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 50);

		Task task = createTask(InvokableUninterruptibleBlockingInvoke.class, config);

		try {
			task.startTaskThread();

			awaitLatch.await();

			task.cancelExecution();

			for (int i = 0; i < 10; i++) {
				Object msg = taskManagerMessages.poll(1, TimeUnit.SECONDS);
				if (msg instanceof TaskManagerMessages.FatalError) {
					return; // success
				}
			}

			fail("Did not receive expected task manager message");
		} finally {
			// Interrupt again to clean up Thread
			cancelLatch.trigger();
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

		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL, interval);
		config.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, timeout);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setTaskCancellationInterval(interval + 1337);
		executionConfig.setTaskCancellationTimeout(timeout - 1337);

		Task task = createTask(InvokableBlockingInInvoke.class, config, executionConfig);

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

	/**
	 * Creates a {@link BlobCacheService} mock that is suitable to be used in the tests above.
	 *
	 * @return BlobCache mock with the bare minimum of implemented functions that work
	 */
	private BlobCacheService createBlobCache() {
		return new BlobCacheService(
				mock(PermanentBlobCache.class),
				mock(TransientBlobCache.class));
	}

	private Task createTask(Class<? extends AbstractInvokable> invokable) throws IOException {
		return createTask(invokable, new Configuration(), new ExecutionConfig());
	}

	private Task createTask(Class<? extends AbstractInvokable> invokable, Configuration config) throws IOException {
		BlobCacheService blobService = createBlobCache();
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
		return createTask(invokable, blobService, libCache, config, new ExecutionConfig());
	}

	private Task createTask(Class<? extends AbstractInvokable> invokable, Configuration config, ExecutionConfig execConfig) throws IOException {
		BlobCacheService blobService = createBlobCache();
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
		return createTask(invokable, blobService, libCache, config, execConfig);
	}

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			BlobCacheService blobService,
			LibraryCacheManager libCache) throws IOException {

		return createTask(invokable, blobService, libCache, new Configuration(), new ExecutionConfig());
	}

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			BlobCacheService blobService,
			LibraryCacheManager libCache,
			Configuration config,
			ExecutionConfig execConfig) throws IOException {

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		Executor executor = mock(Executor.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(partitionManager);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));
		when(network.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		return createTask(invokable, blobService, libCache, network, consumableNotifier, partitionProducerStateChecker, executor, config, execConfig);
	}

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			BlobCacheService blobService,
			LibraryCacheManager libCache,
			NetworkEnvironment networkEnvironment,
			ResultPartitionConsumableNotifier consumableNotifier,
			PartitionProducerStateChecker partitionProducerStateChecker,
			Executor executor) throws IOException {
		return createTask(invokable, blobService, libCache, networkEnvironment, consumableNotifier, partitionProducerStateChecker, executor, new Configuration(), new ExecutionConfig());
	}

	private Task createTask(
		Class<? extends AbstractInvokable> invokable,
		BlobCacheService blobService,
		LibraryCacheManager libCache,
		NetworkEnvironment networkEnvironment,
		ResultPartitionConsumableNotifier consumableNotifier,
		PartitionProducerStateChecker partitionProducerStateChecker,
		Executor executor,
		Configuration taskManagerConfig,
		ExecutionConfig execConfig) throws IOException {

		JobID jobId = new JobID();
		JobVertexID jobVertexId = new JobVertexID();
		ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();

		InputSplitProvider inputSplitProvider = new TaskInputSplitProvider(
			jobManagerGateway,
			jobId,
			jobVertexId,
			executionAttemptId,
			new FiniteDuration(60, TimeUnit.SECONDS));

		CheckpointResponder checkpointResponder = new ActorGatewayCheckpointResponder(jobManagerGateway);

		SerializedValue<ExecutionConfig> serializedExecutionConfig = new SerializedValue<>(execConfig);

		JobInformation jobInformation = new JobInformation(
			jobId,
			"Test Job",
			serializedExecutionConfig,
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());

		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			"Test Task",
			1,
			1,
			invokable.getName(),
			new Configuration());

		TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
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
			taskManagerConnection,
			inputSplitProvider,
			checkpointResponder,
			blobService,
			libCache,
			mock(FileCache.class),
			new TestingTaskManagerRuntimeInfo(taskManagerConfig),
			taskMetricGroup,
			consumableNotifier,
			partitionProducerStateChecker,
			executor);
	}

	// ------------------------------------------------------------------------
	// Validation Methods
	// ------------------------------------------------------------------------

	private void validateUnregisterTask(ExecutionAttemptID id) {
		try {
			// we may have to wait for a bit to give the actors time to receive the message
			// and put it into the queue
			Object rawMessage = taskManagerMessages.take();

			assertNotNull("There is no additional TaskManager message", rawMessage);
			if (!(rawMessage instanceof TaskMessages.TaskInFinalState)) {
				fail("TaskManager message is not 'UnregisterTask', but " + rawMessage.getClass());
			}

			TaskMessages.TaskInFinalState message = (TaskMessages.TaskInFinalState) rawMessage;
			assertEquals(id, message.executionID());
		}
		catch (InterruptedException e) {
			fail("interrupted");
		}
	}

	private void validateTaskManagerStateChange(ExecutionState state, Task task, boolean hasError) {
		try {
			// we may have to wait for a bit to give the actors time to receive the message
			// and put it into the queue
			Object rawMessage = taskManagerMessages.take();

			assertNotNull("There is no additional TaskManager message", rawMessage);
			if (!(rawMessage instanceof TaskMessages.UpdateTaskExecutionState)) {
				fail("TaskManager message is not 'UpdateTaskExecutionState', but " + rawMessage.getClass());
			}

			TaskMessages.UpdateTaskExecutionState message =
					(TaskMessages.UpdateTaskExecutionState) rawMessage;

			TaskExecutionState taskState =  message.taskExecutionState();

			assertEquals(task.getJobID(), taskState.getJobID());
			assertEquals(task.getExecutionId(), taskState.getID());
			assertEquals(state, taskState.getExecutionState());

			if (hasError) {
				assertNotNull(taskState.getError(getClass().getClassLoader()));
			} else {
				assertNull(taskState.getError(getClass().getClassLoader()));
			}
		}
		catch (InterruptedException e) {
			fail("interrupted");
		}
	}

	private void validateListenerMessage(ExecutionState state, Task task, boolean hasError) {
		try {
			// we may have to wait for a bit to give the actors time to receive the message
			// and put it into the queue
			TaskMessages.UpdateTaskExecutionState message =
					(TaskMessages.UpdateTaskExecutionState) listenerMessages.take();
			assertNotNull("There is no additional listener message", message);

			TaskExecutionState taskState =  message.taskExecutionState();

			assertEquals(task.getJobID(), taskState.getJobID());
			assertEquals(task.getExecutionId(), taskState.getID());
			assertEquals(state, taskState.getExecutionState());

			if (hasError) {
				assertNotNull(taskState.getError(getClass().getClassLoader()));
			} else {
				assertNull(taskState.getError(getClass().getClassLoader()));
			}
		}
		catch (InterruptedException e) {
			fail("interrupted");
		}
	}

	private void validateCancelingAndCanceledListenerMessage(Task task) {
		try {
			// we may have to wait for a bit to give the actors time to receive the message
			// and put it into the queue
			TaskMessages.UpdateTaskExecutionState message1 =
					(TaskMessages.UpdateTaskExecutionState) listenerMessages.take();
			TaskMessages.UpdateTaskExecutionState message2 =
					(TaskMessages.UpdateTaskExecutionState) listenerMessages.take();

			assertNotNull("There is no additional listener message", message1);
			assertNotNull("There is no additional listener message", message2);

			TaskExecutionState taskState1 =  message1.taskExecutionState();
			TaskExecutionState taskState2 =  message2.taskExecutionState();

			assertEquals(task.getJobID(), taskState1.getJobID());
			assertEquals(task.getJobID(), taskState2.getJobID());
			assertEquals(task.getExecutionId(), taskState1.getID());
			assertEquals(task.getExecutionId(), taskState2.getID());

			ExecutionState state1 = taskState1.getExecutionState();
			ExecutionState state2 = taskState2.getExecutionState();

			// it may be (very rarely) that the following race happens:
			//  - OUTSIDE THREAD: call to cancel()
			//  - OUTSIDE THREAD: atomic state change from running to canceling
			//  - TASK THREAD: finishes, atomic change from canceling to canceled
			//  - TASK THREAD: send notification that state is canceled
			//  - OUTSIDE THREAD: send notification that state is canceling

			// for that reason, we allow the notification messages in any order.
			assertTrue((state1 == ExecutionState.CANCELING && state2 == ExecutionState.CANCELED) ||
						(state2 == ExecutionState.CANCELING && state1 == ExecutionState.CANCELED));
		}
		catch (InterruptedException e) {
			fail("interrupted");
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Mock invokable code
	// --------------------------------------------------------------------------------------------

	/** Test task class. */
	public static final class TestInvokableCorrect extends AbstractInvokable {

		public TestInvokableCorrect(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {}

		@Override
		public void cancel() throws Exception {
			fail("This should not be called");
		}
	}

	/** Test task class. */
	public static final class InvokableWithExceptionInInvoke extends AbstractInvokable {

		public InvokableWithExceptionInInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			throw new Exception("test");
		}
	}

	/** Test task class. */
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

	/** Test task class. */
	public abstract static class InvokableNonInstantiable extends AbstractInvokable {

		public InvokableNonInstantiable(Environment environment) {
			super(environment);
		}
	}

	/** Test task class. */
	public static final class InvokableBlockingInInvoke extends AbstractInvokable {

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

	/** Test task class. */
	public static final class InvokableWithCancelTaskExceptionInInvoke extends AbstractInvokable {

		public InvokableWithCancelTaskExceptionInInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			try {
				triggerLatch.await();
			}
			catch (Throwable ignored) {}

			throw new CancelTaskException();
		}
	}

	/** Test task class. */
	public static final class InvokableInterruptableSharedLockInInvokeAndCancel extends AbstractInvokable {

		private final Object lock = new Object();

		public InvokableInterruptableSharedLockInInvokeAndCancel(Environment environment) {
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
		public void cancel() throws Exception {
			synchronized (lock) {
				cancelLatch.trigger();
			}
		}
	}

	/** Test task class. */
	public static final class InvokableBlockingInCancel extends AbstractInvokable {

		public InvokableBlockingInCancel(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			try {
				cancelLatch.await();
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
				cancelLatch.trigger();
				wait();
			}
		}
	}

	/** Test task class. */
	public static final class InvokableUninterruptibleBlockingInvoke extends AbstractInvokable {

		public InvokableUninterruptibleBlockingInvoke(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			while (!cancelLatch.isTriggered()) {
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
		public void cancel() throws Exception {
		}
	}

	/** Test task class. */
	public static final class FailingInvokableWithChainedException extends AbstractInvokable {

		public FailingInvokableWithChainedException(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			throw new TestWrappedException(new IOException("test"));
		}

		@Override
		public void cancel() {}
	}

	// ------------------------------------------------------------------------
	//  test exceptions
	// ------------------------------------------------------------------------

	private static class TestWrappedException extends WrappingRuntimeException {
		private static final long serialVersionUID = 1L;

		public TestWrappedException(@Nonnull Throwable cause) {
			super(cause);
		}
	}
}
