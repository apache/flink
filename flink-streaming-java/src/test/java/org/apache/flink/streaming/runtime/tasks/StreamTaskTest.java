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

import akka.actor.ActorRef;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsynchronousStateHandle;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import org.mockito.internal.util.reflection.Whitebox;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamTaskTest {

	private static OneShotLatch SYNC_LATCH;

	/**
	 * This test checks that cancel calls that are issued before the operator is
	 * instantiated still lead to proper canceling.
	 */
	@Test
	public void testEarlyCanceling() {
		try {
			StreamConfig cfg = new StreamConfig(new Configuration());
			cfg.setStreamOperator(new SlowlyDeserializingOperator());
			
			Task task = createTask(SourceStreamTask.class, cfg, new Configuration());
			task.startTaskThread();
			
			// wait until the task thread reached state RUNNING 
			while (task.getExecutionState() == ExecutionState.CREATED ||
					task.getExecutionState() == ExecutionState.DEPLOYING)
			{
				Thread.sleep(5);
			}
			
			// make sure the task is really running
			if (task.getExecutionState() != ExecutionState.RUNNING) {
				fail("Task entered state " + task.getExecutionState() + " with error "
						+ ExceptionUtils.stringifyException(task.getFailureCause()));
			}
			
			// send a cancel. because the operator takes a long time to deserialize, this should
			// hit the task before the operator is deserialized
			task.cancelExecution();
			
			// the task should reach state canceled eventually
			assertTrue(task.getExecutionState() == ExecutionState.CANCELING ||
					task.getExecutionState() == ExecutionState.CANCELED);
			
			task.getExecutingThread().join(60000);
			
			assertFalse("Task did not cancel", task.getExecutingThread().isAlive());
			assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateBackendLoading() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(ConfigConstants.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(SourceStreamTask.class, cfg, taskManagerConfig);

		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	@Test
	public void testCancellationNotBlockedOnLock() throws Exception {
		SYNC_LATCH = new OneShotLatch();

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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

	/**
	 * Tests that all created StreamTaskStates are properly cleaned up when a snapshotting method
	 * of an operator fails.
	 */
	@Test
	public void testStateCleanupWhenFailingCheckpoint() throws Exception {
		final long checkpointId = 1L;
		final long timestamp = 42L;

		StreamTask<Integer, StreamOperator<Integer>> streamTask = new TestingStreamTask();
		streamTask.setEnvironment(new DummyEnvironment("test task", 1, 0));

		OperatorChain<Integer> operatorChain = mock(OperatorChain.class);

		StreamOperator<Integer> firstOperator = mock(StreamOperator.class);
		StreamTaskState firstStreamTaskState = mock(StreamTaskState.class);
		StreamOperator<Integer> secondOperator = mock(StreamOperator.class);

		doReturn(firstStreamTaskState).when(firstOperator).snapshotOperatorState(anyLong(), anyLong());
		doThrow(new Exception("Test Exception")).when(secondOperator).snapshotOperatorState(anyLong(), anyLong());

		doReturn(new StreamOperator<?>[]{firstOperator, secondOperator}).when(operatorChain).getAllOperators();

		Whitebox.setInternalState(streamTask, "operatorChain", operatorChain);
		Whitebox.setInternalState(streamTask, "isRunning", true);

		try {
			streamTask.triggerCheckpoint(checkpointId, timestamp);
			fail("Expected exception here.");
		} catch (Exception expected) {
			// expected failing trigger checkpoint here
		}

		verify(firstStreamTaskState).discardState();
	}

	/**
	 * Tests that the AsyncCheckpointThread discards the given StreamTaskStates in case a failure
	 * occurs while materializing the asynchronous state handles.
	 */
	@Test
	public void testAsyncCheckpointThreadStateCleanup() throws Exception {
		final long checkpointId = 1L;
		StreamTaskState firstState = mock(StreamTaskState.class);
		StreamTaskState secondState = mock(StreamTaskState.class);
		AsynchronousStateHandle<Integer> functionStateHandle = mock(AsynchronousStateHandle.class);

		doReturn(functionStateHandle).when(firstState).getFunctionState();
		doThrow(new Exception("Test exception")).when(functionStateHandle).materialize();

		StreamTask<Integer, StreamOperator<Integer>> owner = mock(StreamTask.class);
		StreamTaskState[] states = {firstState, secondState};

		StreamTask.AsyncCheckpointThread asyncCheckpointThread = new StreamTask.AsyncCheckpointThread(
			"AsyncCheckpointThread",
			owner,
			new HashSet<Closeable>(),
			states,
			checkpointId);

		asyncCheckpointThread.run();

		for (StreamTaskState streamTaskState : states) {
			verify(streamTaskState).discardState();
		}
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			StreamConfig taskConfig,
			Configuration taskManagerConfig) throws Exception {

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getPartitionManager()).thenReturn(partitionManager);
		when(network.getPartitionConsumableNotifier()).thenReturn(consumableNotifier);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);

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
			invokable.getName(),
			taskConfig.getConfiguration());

		return new Task(
			jobInformation,
			taskInformation,
			new ExecutionAttemptID(),
			0,
			0,
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			0,
			null,
			mock(MemoryManager.class),
			mock(IOManager.class),
			network,
			mock(BroadcastVariableManager.class),
				new DummyGateway(),
				new DummyGateway(),
				new FiniteDuration(60, TimeUnit.SECONDS),
			libCache,
			mock(FileCache.class),
			new TaskManagerRuntimeInfo("localhost", taskManagerConfig, System.getProperty("java.io.tmpdir")),
				new UnregisteredTaskMetricsGroup());
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
		public void run(Object lockingObject, Output<StreamRecord<Long>> collector) throws Exception {
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

	// ------------------------------------------------------------------------
	//  Test JobManager/TaskManager gateways
	// ------------------------------------------------------------------------
	
	private static class DummyGateway implements ActorGateway {
		private static final long serialVersionUID = 1L;

		@Override
		public Future<Object> ask(Object message, FiniteDuration timeout) {
			return null;
		}

		@Override
		public void tell(Object message) {}

		@Override
		public void tell(Object message, ActorGateway sender) {}

		@Override
		public void forward(Object message, ActorGateway sender) {}

		@Override
		public Future<Object> retry(Object message, int numberRetries, FiniteDuration timeout, ExecutionContext executionContext) {
			return null;
		}

		@Override
		public String path() {
			return null;
		}

		@Override
		public ActorRef actor() {
			return null;
		}

		@Override
		public UUID leaderSessionID() {
			return null;
		}
	}

	public static final class MockStateBackend implements StateBackendFactory<AbstractStateBackend> {

		@Override
		public AbstractStateBackend createFromConfig(Configuration config) throws Exception {
			return mock(AbstractStateBackend.class);
		}
	}

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

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

		@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
		@Override
		protected void run() throws Exception {
			final OneShotLatch latch = new OneShotLatch();
			final Object lock = new Object();

			LockHolder holder = new LockHolder(lock, latch);
			holder.start();
			try {
				// cancellation should try and cancel this
				Set<Closeable> canceleables = getCancelables();
				synchronized (canceleables) {
					canceleables.add(holder);
				}

				// wait till the lock holder has the lock
				latch.await();

				// we are at the point where cancelling can happen
				SYNC_LATCH.trigger();
	
				// try to acquire the lock - this is not possible as long as the lock holder
				// thread lives
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

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * Testing class for StreamTask methods
	 */
	private static class TestingStreamTask extends StreamTask<Integer, StreamOperator<Integer>> {

		@Override
		protected void init() throws Exception {

		}

		@Override
		protected void run() throws Exception {

		}

		@Override
		protected void cleanup() throws Exception {

		}

		@Override
		protected void cancelTask() throws Exception {

		}
	}
}
