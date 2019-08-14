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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
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
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskAsyncCallTest extends TestLogger {

	/** Number of expected checkpoints. */
	private static int numCalls;

	/** Triggered at the beginning of {@link CheckpointsInOrderInvokable#invoke()}. */
	private static OneShotLatch awaitLatch;

	/**
	 * Triggered when {@link CheckpointsInOrderInvokable#triggerCheckpoint(CheckpointMetaData, CheckpointOptions, boolean)}
	 * was called {@link #numCalls} times.
	 */
	private static OneShotLatch triggerLatch;

	/**
	 * Triggered when {@link CheckpointsInOrderInvokable#notifyCheckpointComplete(long)}
	 * was called {@link #numCalls} times.
	 */
	private static OneShotLatch notifyCheckpointCompleteLatch;

	/** Triggered on {@link ContextClassLoaderInterceptingInvokable#cancel()}. */
	private static OneShotLatch stopLatch;

	private static final List<ClassLoader> classLoaders = Collections.synchronizedList(new ArrayList<>());

	private ShuffleEnvironment<?, ?> shuffleEnvironment;

	@Before
	public void createQueuesAndActors() {
		numCalls = 1000;

		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
		notifyCheckpointCompleteLatch = new OneShotLatch();
		stopLatch = new OneShotLatch();

		shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

		classLoaders.clear();
	}

	@After
	public void teardown() throws Exception {
		if (shuffleEnvironment != null) {
			shuffleEnvironment.close();
		}
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testCheckpointCallsInOrder() throws Exception {

		Task task = createTask(CheckpointsInOrderInvokable.class);
		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();

			awaitLatch.await();

			for (int i = 1; i <= numCalls; i++) {
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation(), false);
			}

			triggerLatch.await();

			assertFalse(task.isCanceledOrFailed());

			ExecutionState currentState = task.getExecutionState();
			assertThat(currentState, isOneOf(ExecutionState.RUNNING, ExecutionState.FINISHED));
		}
	}

	@Test
	public void testMixedAsyncCallsInOrder() throws Exception {

		Task task = createTask(CheckpointsInOrderInvokable.class);
		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();

			awaitLatch.await();

			for (int i = 1; i <= numCalls; i++) {
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation(), false);
				task.notifyCheckpointComplete(i);
			}

			triggerLatch.await();

			assertFalse(task.isCanceledOrFailed());

			ExecutionState currentState = task.getExecutionState();
			assertThat(currentState, isOneOf(ExecutionState.RUNNING, ExecutionState.FINISHED));
		}
	}

	/**
	 * Asserts that {@link AbstractInvokable#triggerCheckpoint(CheckpointMetaData, CheckpointOptions, boolean)},
	 * and {@link AbstractInvokable#notifyCheckpointComplete(long)} are invoked by a thread whose context
	 * class loader is set to the user code class loader.
	 */
	@Test
	public void testSetsUserCodeClassLoader() throws Exception {
		numCalls = 1;

		Task task = createTask(ContextClassLoaderInterceptingInvokable.class);
		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();

			awaitLatch.await();

			task.triggerCheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation(), false);
			triggerLatch.await();

			task.notifyCheckpointComplete(1);
			notifyCheckpointCompleteLatch.await();

			task.cancelExecution();
			stopLatch.await();

			assertThat(classLoaders, hasSize(greaterThanOrEqualTo(2)));
			assertThat(classLoaders, everyItem(instanceOf(TestUserCodeClassLoader.class)));
		}
	}

	private Task createTask(Class<? extends AbstractInvokable> invokableClass) throws Exception {
		BlobCacheService blobService =
			new BlobCacheService(mock(PermanentBlobCache.class), mock(TransientBlobCache.class));

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(new TestUserCodeClassLoader());

		ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);
		TaskMetricGroup taskMetricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

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
			invokableClass.getName(),
			new Configuration());

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
			new TestTaskStateManager(),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			new TestGlobalAggregateManager(),
			blobService,
			libCache,
			mock(FileCache.class),
			new TestingTaskManagerRuntimeInfo(),
			taskMetricGroup,
			consumableNotifier,
			partitionProducerStateChecker,
			executor);
	}

	public static class CheckpointsInOrderInvokable extends AbstractInvokable {

		private volatile long lastCheckpointId = 0;

		private volatile Exception error;

		public CheckpointsInOrderInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			awaitLatch.trigger();

			// wait forever (until canceled)
			synchronized (this) {
				while (error == null) {
					wait();
				}
			}

			if (error != null) {
				// exit method prematurely due to error but make sure that the tests can finish
				triggerLatch.trigger();
				notifyCheckpointCompleteLatch.trigger();
				stopLatch.trigger();

				throw error;
			}
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
			lastCheckpointId++;
			if (checkpointMetaData.getCheckpointId() == lastCheckpointId) {
				if (lastCheckpointId == numCalls) {
					triggerLatch.trigger();
				}
			}
			else if (this.error == null) {
				this.error = new Exception("calls out of order");
				synchronized (this) {
					notifyAll();
				}
			}
			return true;
		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			if (checkpointId != lastCheckpointId && this.error == null) {
				this.error = new Exception("calls out of order");
				synchronized (this) {
					notifyAll();
				}
			} else if (lastCheckpointId == numCalls) {
				notifyCheckpointCompleteLatch.trigger();
			}
		}
	}

	/**
	 * This is an {@link AbstractInvokable} that stores the context class loader of the invoking
	 * thread in a static field so that tests can assert on the class loader instances.
	 *
	 * @see #testSetsUserCodeClassLoader()
	 */
	public static class ContextClassLoaderInterceptingInvokable extends CheckpointsInOrderInvokable {

		public ContextClassLoaderInterceptingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
			classLoaders.add(Thread.currentThread().getContextClassLoader());

			return super.triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			classLoaders.add(Thread.currentThread().getContextClassLoader());

			super.notifyCheckpointComplete(checkpointId);
		}

		@Override
		public void cancel() {
			stopLatch.trigger();
		}

	}

	/**
	 * A {@link ClassLoader} that delegates everything to {@link ClassLoader#getSystemClassLoader()}.
	 *
	 * @see #testSetsUserCodeClassLoader()
	 */
	private static class TestUserCodeClassLoader extends ClassLoader {
		public TestUserCodeClassLoader() {
			super(ClassLoader.getSystemClassLoader());
		}
	}

	private static class TaskCleaner implements AutoCloseable {

		private final Task task;

		private TaskCleaner(Task task) {
			this.task = task;
		}

		@Override
		public void close() throws Exception {
			task.cancelExecution();
			task.getExecutingThread().join(5000);
		}
	}

}
