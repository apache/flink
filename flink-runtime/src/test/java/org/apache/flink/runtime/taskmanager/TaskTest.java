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

import com.google.common.collect.Maps;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.TaskMessages;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.concurrent.duration.FiniteDuration;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
 * All tests here have a set of mock actors for TaskManager, JobManager, and
 * execution listener, which simply put the messages in a queue to be picked
 * up by the test and validated.
 */
public class TaskTest {
	
	private static OneShotLatch awaitLatch;
	private static OneShotLatch triggerLatch;
	
	private ActorGateway taskManagerGateway;
	private ActorGateway jobManagerGateway;
	private ActorGateway listenerGateway;

	private BlockingQueue<Object> taskManagerMessages;
	private BlockingQueue<Object> jobManagerMessages;
	private BlockingQueue<Object> listenerMessages;
	
	@Before
	public void createQueuesAndActors() {
		taskManagerMessages = new LinkedBlockingQueue<Object>();
		jobManagerMessages = new LinkedBlockingQueue<Object>();
		listenerMessages = new LinkedBlockingQueue<Object>();
		taskManagerGateway = new ForwardingActorGateway(taskManagerMessages);
		jobManagerGateway = new ForwardingActorGateway(jobManagerMessages);
		listenerGateway = new ForwardingActorGateway(listenerMessages);
		
		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
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
			
			task.registerExecutionListener(listenerGateway);
			
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
			Task task = createTask(TestInvokableCorrect.class, mock(LibraryCacheManager.class));

			// task should be new and perfect
			assertEquals(ExecutionState.CREATED, task.getExecutionState());
			assertFalse(task.isCanceledOrFailed());
			assertNull(task.getFailureCause());

			task.registerExecutionListener(listenerGateway);

			// should fail
			task.run();

			// verify final state
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertNotNull(task.getFailureCause());
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
			// mock a working library cache
			LibraryCacheManager libCache = mock(LibraryCacheManager.class);
			when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
			
			// mock a network manager that rejects registration
			ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
			ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
			NetworkEnvironment network = mock(NetworkEnvironment.class);
			when(network.getPartitionManager()).thenReturn(partitionManager);
			when(network.getPartitionConsumableNotifier()).thenReturn(consumableNotifier);
			when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
			doThrow(new RuntimeException("buffers")).when(network).registerTask(any(Task.class));
			
			Task task = createTask(TestInvokableCorrect.class, libCache, network);

			task.registerExecutionListener(listenerGateway);

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
			task.registerExecutionListener(listenerGateway);

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
	public void testExecutionFailsInRegisterInputOutput() {
		try {
			Task task = createTask(InvokableWithExceptionInRegisterInOut.class);
			task.registerExecutionListener(listenerGateway);

			task.run();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("registerInputOutput"));

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
			task.registerExecutionListener(listenerGateway);
			
			task.run();

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
	public void testCancelDuringRegisterInputOutput() {
		try {
			Task task = createTask(InvokableBlockingInRegisterInOut.class);
			task.registerExecutionListener(listenerGateway);

			// run the task asynchronous
			task.startTaskThread();
			
			// wait till the task is in regInOut
			awaitLatch.await();
			
			task.cancelExecution();
			assertEquals(ExecutionState.CANCELING, task.getExecutionState());
			triggerLatch.trigger();
			
			task.getExecutingThread().join();

			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertNull(task.getFailureCause());
			
			validateUnregisterTask(task.getExecutionId());
			validateCancelingAndCanceledListenerMessage(task);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFailDuringRegisterInputOutput() {
		try {
			Task task = createTask(InvokableBlockingInRegisterInOut.class);
			task.registerExecutionListener(listenerGateway);

			// run the task asynchronous
			task.startTaskThread();

			// wait till the task is in regInOut
			awaitLatch.await();

			task.failExternally(new Exception("test"));
			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			triggerLatch.trigger();

			task.getExecutingThread().join();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("test"));

			validateUnregisterTask(task.getExecutionId());
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
			task.registerExecutionListener(listenerGateway);

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
			task.registerExecutionListener(listenerGateway);

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
	public void testCanceledAfterExecutionFailedInRegInOut() {
		try {
			Task task = createTask(InvokableWithExceptionInRegisterInOut.class);
			task.registerExecutionListener(listenerGateway);

			task.run();
			
			// this should not overwrite the failure state
			task.cancelExecution();

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
			assertTrue(task.isCanceledOrFailed());
			assertTrue(task.getFailureCause().getMessage().contains("registerInputOutput"));

			validateUnregisterTask(task.getExecutionId());
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
			task.registerExecutionListener(listenerGateway);

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
			task.registerExecutionListener(listenerGateway);

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
			task.registerExecutionListener(listenerGateway);

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

		// Expected task state for each partition state
		final Map<ExecutionState, ExecutionState> expected = Maps
				.newHashMapWithExpectedSize(ExecutionState.values().length);

		// Fail the task for unexpected states
		for (ExecutionState state : ExecutionState.values()) {
			expected.put(state, ExecutionState.FAILED);
		}

		expected.put(ExecutionState.RUNNING, ExecutionState.RUNNING);

		expected.put(ExecutionState.CANCELED, ExecutionState.CANCELING);
		expected.put(ExecutionState.CANCELING, ExecutionState.CANCELING);
		expected.put(ExecutionState.FAILED, ExecutionState.CANCELING);

		for (ExecutionState state : ExecutionState.values()) {
			setState(task, ExecutionState.RUNNING);

			task.onPartitionStateUpdate(resultId, partitionId.getPartitionId(), state);

			ExecutionState newTaskState = task.getExecutionState();

			assertEquals(expected.get(state), newTaskState);
		}

		verify(inputGate, times(1)).retriggerPartitionRequest(eq(partitionId.getPartitionId()));
	}

	// ------------------------------------------------------------------------

	private void setInputGate(Task task, SingleInputGate inputGate) {
		try {
			Field f = Task.class.getDeclaredField("inputGates");
			f.setAccessible(true);
			f.set(task, new SingleInputGate[]{inputGate});

			Map<IntermediateDataSetID, SingleInputGate> byId = Maps.newHashMapWithExpectedSize(1);
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

	private Task createTask(Class<? extends AbstractInvokable> invokable) {
		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());
		return createTask(invokable, libCache);
	}

	private Task createTask(Class<? extends AbstractInvokable> invokable,
							LibraryCacheManager libCache) {

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getPartitionManager()).thenReturn(partitionManager);
		when(network.getPartitionConsumableNotifier()).thenReturn(consumableNotifier);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		
		return createTask(invokable, libCache, network);
	}
	
	private Task createTask(Class<? extends AbstractInvokable> invokable,
							LibraryCacheManager libCache,
							NetworkEnvironment networkEnvironment) {
		
		TaskDeploymentDescriptor tdd = createTaskDeploymentDescriptor(invokable);
		
		return new Task(
				tdd,
				mock(MemoryManager.class),
				mock(IOManager.class),
				networkEnvironment,
				mock(BroadcastVariableManager.class),
				taskManagerGateway,
				jobManagerGateway,
				new FiniteDuration(60, TimeUnit.SECONDS),
				libCache,
				mock(FileCache.class),
				new TaskManagerRuntimeInfo("localhost", new Configuration()));
	}

	private TaskDeploymentDescriptor createTaskDeploymentDescriptor(Class<? extends AbstractInvokable> invokable) {
		return new TaskDeploymentDescriptor(
				new JobID(), new JobVertexID(), new ExecutionAttemptID(),
				"Test Task", 0, 1, 0,
				new Configuration(), new Configuration(),
				invokable.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList(),
				0);
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
			assertTrue( (state1 == ExecutionState.CANCELING && state2 == ExecutionState.CANCELED) ||
						(state2 == ExecutionState.CANCELING && state1 == ExecutionState.CANCELED));
		}
		catch (InterruptedException e) {
			fail("interrupted");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Mock invokable code
	// --------------------------------------------------------------------------------------------
	
	public static final class TestInvokableCorrect extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() {}

		@Override
		public void cancel() throws Exception {
			fail("This should not be called");
		}
	}

	public static final class InvokableWithExceptionInRegisterInOut extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			throw new RuntimeException("test");
		}

		@Override
		public void invoke() {}
	}
	
	public static final class InvokableWithExceptionInInvoke extends AbstractInvokable {
		
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {
			throw new Exception("test");
		}
	}

	public static final class InvokableWithExceptionOnTrigger extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

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

	public static abstract class InvokableNonInstantiable extends AbstractInvokable {}

	public static final class InvokableBlockingInRegisterInOut extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
			awaitLatch.trigger();
			
			try {
				triggerLatch.await();
			}
			catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}

		@Override
		public void invoke() {}
	}

	public static final class InvokableBlockingInInvoke extends AbstractInvokable {

		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();
			
			// block forever
			synchronized (this) {
				wait();
			}
		}
	}

	public static final class InvokableWithCancelTaskExceptionInInvoke extends AbstractInvokable {

		@Override
		public void registerInputOutput() {
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
}
