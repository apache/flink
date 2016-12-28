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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionStateListener;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
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

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamTaskTest {

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
		taskManagerConfig.setString(ConfigConstants.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = false;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends are closed
		Mockito.verify(StateBackendTestSource.operatorStateBackend).close();
		Mockito.verify(StateBackendTestSource.keyedStateBackend).close();

		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	@Test
	public void testStateBackendClosingOnFailure() throws Exception {
		Configuration taskManagerConfig = new Configuration();
		taskManagerConfig.setString(ConfigConstants.STATE_BACKEND, MockStateBackend.class.getName());

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new StreamSource<>(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(StateBackendTestSource.class, cfg, taskManagerConfig);

		StateBackendTestSource.fail = true;
		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();

		// ensure that the state backends are closed
		Mockito.verify(StateBackendTestSource.operatorStateBackend).close();
		Mockito.verify(StateBackendTestSource.keyedStateBackend).close();

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

	/**
	 * Mocked state backend factory which returns mocks for the operator and keyed state backends.
	 */
	public static final class MockStateBackend implements StateBackendFactory<AbstractStateBackend> {
		private static final long serialVersionUID = 1L;

		@Override
		public AbstractStateBackend createFromConfig(Configuration config) throws Exception {
			AbstractStateBackend stateBackendMock = mock(AbstractStateBackend.class);

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
