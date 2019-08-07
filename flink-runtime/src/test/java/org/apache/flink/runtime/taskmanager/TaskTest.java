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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteChannelStateChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.WrappingRuntimeException;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the Task, which make sure that correct state transitions happen,
 * and failures are correctly handled.
 */
public class TaskTest extends TestLogger {

	private static OneShotLatch awaitLatch;
	private static OneShotLatch triggerLatch;

	private ShuffleEnvironment<?, ?> shuffleEnvironment;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() {
		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();

		shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();
	}

	@After
	public void teardown() throws Exception {
		if (shuffleEnvironment != null) {
			shuffleEnvironment.close();
		}
	}

	@Test
	public void testRegularExecution() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = createTaskBuilder()
			.setInvokable(TestInvokableCorrect.class)
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
		final Task task = createTaskBuilder().build();
		task.cancelExecution();

		assertEquals(ExecutionState.CANCELING, task.getExecutionState());

		task.run();

		// verify final state
		assertEquals(ExecutionState.CANCELED, task.getExecutionState());

		assertNull(task.getInvokable());
	}

	@Test
	public void testFailExternallyRightAway() throws Exception {
		final Task task = createTaskBuilder().build();
		task.failExternally(new Exception("fail externally"));

		assertEquals(ExecutionState.FAILED, task.getExecutionState());

		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
	}

	@Test
	public void testLibraryCacheRegistrationFailed() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = createTaskBuilder()
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

		final Task task = createTaskBuilder()
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
	public void testExecutionFailsInNetworkRegistrationForPartitions() throws Exception {
		final PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
			new IntermediateDataSetID(),
			new IntermediateResultPartitionID(),
			ResultPartitionType.PIPELINED,
			1,
			1);
		final ShuffleDescriptor shuffleDescriptor = NettyShuffleDescriptorBuilder.newBuilder().buildLocal();
		final ResultPartitionDeploymentDescriptor dummyPartition = new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			false);
		testExecutionFailsInNetworkRegistration(Collections.singleton(dummyPartition), Collections.emptyList());
	}

	@Test
	public void testExecutionFailsInNetworkRegistrationForGates() throws Exception {
		final ShuffleDescriptor dummyChannel = NettyShuffleDescriptorBuilder.newBuilder().buildRemote();
		final InputGateDeploymentDescriptor dummyGate = new InputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			new ShuffleDescriptor[] { dummyChannel });
		testExecutionFailsInNetworkRegistration(Collections.emptyList(), Collections.singleton(dummyGate));
	}

	private void testExecutionFailsInNetworkRegistration(
			Collection<ResultPartitionDeploymentDescriptor> resultPartitions,
			Collection<InputGateDeploymentDescriptor> inputGates) throws Exception {
		final String errorMessage = "Network buffer pool has already been destroyed.";

		final ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		final PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);

		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = new TestTaskBuilder(shuffleEnvironment)
			.setTaskManagerActions(taskManagerActions)
			.setConsumableNotifier(consumableNotifier)
			.setPartitionProducerStateChecker(partitionProducerStateChecker)
			.setResultPartitions(resultPartitions)
			.setInputGates(inputGates)
			.build();

		// shut down the network to make the following task registration failure
		shuffleEnvironment.close();

		// should fail
		task.run();

		// verify final state
		assertEquals(ExecutionState.FAILED, task.getExecutionState());
		assertTrue(task.isCanceledOrFailed());
		assertTrue(task.getFailureCause().getMessage().contains(errorMessage));

		taskManagerActions.validateListenerMessage(ExecutionState.FAILED, task, new IllegalStateException(errorMessage));
	}

	@Test
	public void testInvokableInstantiationFailed() throws Exception {
		final QueuedNoOpTaskManagerActions taskManagerActions = new QueuedNoOpTaskManagerActions();
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
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
		final Task task = createTaskBuilder()
			.setInvokable(InvokableWithCancelTaskExceptionInInvoke.class)
			.build();

		// Cause CancelTaskException.
		triggerLatch.trigger();

		task.run();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
	}

	@Test
	public void testCancelTaskExceptionAfterTaskMarkedFailed() throws Exception {
		final Task task = createTaskBuilder()
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
	public void testOnPartitionStateUpdateWhileRunning() throws Exception {
		testOnPartitionStateUpdate(ExecutionState.RUNNING);
	}

	/**
	 * Partition state updates can also happen when {@link Task} is in
	 * {@link ExecutionState#DEPLOYING} state, because we are requesting for partitions during
	 * setting up input gates.
	 */
	@Test
	public void testOnPartitionStateUpdateWhileDeploying() throws Exception {
		testOnPartitionStateUpdate(ExecutionState.DEPLOYING);
	}

	public void testOnPartitionStateUpdate(ExecutionState initialTaskState) throws Exception {
		final ResultPartitionID partitionId = new ResultPartitionID();

		final Task task = createTaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.build();

		RemoteChannelStateChecker checker =
			new RemoteChannelStateChecker(partitionId, "test task");

		// Expected task state for each producer state
		final Map<ExecutionState, ExecutionState> expected = new HashMap<>(ExecutionState.values().length);

		// Fail the task for unexpected states
		for (ExecutionState state : ExecutionState.values()) {
			expected.put(state, ExecutionState.FAILED);
		}

		expected.put(ExecutionState.RUNNING, initialTaskState);
		expected.put(ExecutionState.SCHEDULED, initialTaskState);
		expected.put(ExecutionState.DEPLOYING, initialTaskState);
		expected.put(ExecutionState.FINISHED, initialTaskState);

		expected.put(ExecutionState.CANCELED, ExecutionState.CANCELING);
		expected.put(ExecutionState.CANCELING, ExecutionState.CANCELING);
		expected.put(ExecutionState.FAILED, ExecutionState.CANCELING);

		int producingStateCounter = 0;
		for (ExecutionState state : ExecutionState.values()) {
			TestTaskBuilder.setTaskState(task, initialTaskState);

			if (checker.isProducerReadyOrAbortConsumption(task.new PartitionProducerStateResponseHandle(state, null))) {
				producingStateCounter++;
			}

			ExecutionState newTaskState = task.getExecutionState();

			assertEquals(expected.get(state), newTaskState);
		}

		assertEquals(4, producingStateCounter);
	}

	/**
	 * Tests the trigger partition state update future completions.
	 */
	@Test
	public void testTriggerPartitionStateUpdate() throws Exception {
		final IntermediateDataSetID resultId = new IntermediateDataSetID();
		final ResultPartitionID partitionId = new ResultPartitionID();

		final PartitionProducerStateChecker partitionChecker = mock(PartitionProducerStateChecker.class);

		final ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();

		AtomicInteger callCount = new AtomicInteger(0);

		RemoteChannelStateChecker remoteChannelStateChecker =
			new RemoteChannelStateChecker(partitionId, "test task");

		// Test all branches of trigger partition state check
		{
			// Reset latches
			setup();

			// PartitionProducerDisposedException
			final Task task = createTaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();
			TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

			final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.requestPartitionProducerState(resultId, partitionId, checkResult ->
				assertThat(remoteChannelStateChecker.isProducerReadyOrAbortConsumption(checkResult), is(false))
			);

			promise.completeExceptionally(new PartitionProducerDisposedException(partitionId));
			assertEquals(ExecutionState.CANCELING, task.getExecutionState());
		}

		{
			// Reset latches
			setup();

			// Any other exception
			final Task task = createTaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();
			TestTaskBuilder.setTaskState(task, ExecutionState.RUNNING);

			final CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
			when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

			task.requestPartitionProducerState(resultId, partitionId, checkResult ->
				assertThat(remoteChannelStateChecker.isProducerReadyOrAbortConsumption(checkResult), is(false))
			);

			promise.completeExceptionally(new RuntimeException("Any other exception"));

			assertEquals(ExecutionState.FAILED, task.getExecutionState());
		}

		{
			callCount.set(0);

			// Reset latches
			setup();

			// TimeoutException handled special => retry
			// Any other exception
			final Task task = createTaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			try {
				task.startTaskThread();
				awaitLatch.await();

				CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
				when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

				task.requestPartitionProducerState(resultId, partitionId, checkResult -> {
					if (remoteChannelStateChecker.isProducerReadyOrAbortConsumption(checkResult)) {
						callCount.incrementAndGet();
					}
				});

				promise.completeExceptionally(new TimeoutException());

				assertEquals(ExecutionState.RUNNING, task.getExecutionState());

				assertEquals(1, callCount.get());
			} finally {
				task.getExecutingThread().interrupt();
				task.getExecutingThread().join();
			}
		}

		{
			callCount.set(0);

			// Reset latches
			setup();

			// Success
			final Task task =  createTaskBuilder()
				.setInvokable(InvokableBlockingInInvoke.class)
				.setConsumableNotifier(consumableNotifier)
				.setPartitionProducerStateChecker(partitionChecker)
				.setExecutor(Executors.directExecutor())
				.build();

			try {
				task.startTaskThread();
				awaitLatch.await();

				CompletableFuture<ExecutionState> promise = new CompletableFuture<>();
				when(partitionChecker.requestPartitionProducerState(eq(task.getJobID()), eq(resultId), eq(partitionId))).thenReturn(promise);

				task.requestPartitionProducerState(resultId, partitionId, checkResult -> {
					if (remoteChannelStateChecker.isProducerReadyOrAbortConsumption(checkResult)) {
						callCount.incrementAndGet();
					}
				});

				promise.complete(ExecutionState.RUNNING);

				assertEquals(ExecutionState.RUNNING, task.getExecutionState());

				assertEquals(1, callCount.get());
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

		final Task task = createTaskBuilder()
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

		final Task task = createTaskBuilder()
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

		final Task task = createTaskBuilder()
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

		final Task task = createTaskBuilder()
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

	@Test
	public void testTerminationFutureCompletesOnNormalExecution() throws Exception {
		final Task task = createTaskBuilder()
			.setInvokable(InvokableBlockingWithTrigger.class)
			.setTaskManagerActions(new NoOpTaskManagerActions())
			.build();

		// run the task asynchronous
		task.startTaskThread();

		// wait till the task is in invoke
		awaitLatch.await();

		assertFalse(task.getTerminationFuture().isDone());

		triggerLatch.trigger();

		task.getExecutingThread().join();

		assertEquals(ExecutionState.FINISHED, task.getTerminationFuture().getNow(null));
	}

	@Test
	public void testTerminationFutureCompletesOnImmediateCancellation() throws Exception {
		final Task task = createTaskBuilder()
			.setInvokable(InvokableBlockingInInvoke.class)
			.setTaskManagerActions(new NoOpTaskManagerActions())
			.build();

		task.cancelExecution();

		assertFalse(task.getTerminationFuture().isDone());

		// run the task asynchronous
		task.startTaskThread();

		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getTerminationFuture().getNow(null));
	}

	@Test
	public void testTerminationFutureCompletesOnErrorInInvoke() throws Exception {
		final Task task = createTaskBuilder()
			.setInvokable(InvokableWithExceptionInInvoke.class)
			.setTaskManagerActions(new NoOpTaskManagerActions())
			.build();

		// run the task asynchronous
		task.startTaskThread();

		task.getExecutingThread().join();

		assertEquals(ExecutionState.FAILED, task.getTerminationFuture().getNow(null));
	}

	@Test
	public void testReturnsEmptyStackTraceIfTaskNotStarted() throws Exception {
		final Task task = createTaskBuilder().build();
		final StackTraceElement[] actualStackTrace = task.getStackTraceOfExecutingThread();
		assertEquals(0, actualStackTrace.length);
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

	private TestTaskBuilder createTaskBuilder() {
		return new TestTaskBuilder(shuffleEnvironment);
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

	private static final class InvokableBlockingWithTrigger extends AbstractInvokable {
		public InvokableBlockingWithTrigger(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			triggerLatch.await();
		}
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
				while (true) {
					wait();
				}
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
