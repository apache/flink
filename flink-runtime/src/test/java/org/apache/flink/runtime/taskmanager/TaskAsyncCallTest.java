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
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskAsyncCallTest extends TestLogger {

	/** Number of expected checkpoints. */
	private static int numCalls;

	/** Triggered at the beginning of {@link CheckpointsInOrderInvokable#invoke()}. */
	private static OneShotLatch awaitLatch;

	/**
	 * Triggered when {@link CheckpointsInOrderInvokable#triggerCheckpoint(CheckpointMetaData, CheckpointOptions)}
	 * was called {@link #numCalls} times.
	 */
	private static OneShotLatch triggerLatch;

	/**
	 * Triggered when {@link CheckpointsInOrderInvokable#notifyCheckpointComplete(long)}
	 * was called {@link #numCalls} times.
	 */
	private static OneShotLatch notifyCheckpointCompleteLatch;

	/** Triggered on {@link ContextClassLoaderInterceptingInvokable#stop()}}. */
	private static OneShotLatch stopLatch;

	private static final List<ClassLoader> classLoaders = Collections.synchronizedList(new ArrayList<>());

	@Before
	public void createQueuesAndActors() {
		numCalls = 1000;

		awaitLatch = new OneShotLatch();
		triggerLatch = new OneShotLatch();
		notifyCheckpointCompleteLatch = new OneShotLatch();
		stopLatch = new OneShotLatch();

		classLoaders.clear();
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
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation());
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
				task.triggerCheckpointBarrier(i, 156865867234L, CheckpointOptions.forCheckpointWithDefaultLocation());
				task.notifyCheckpointComplete(i);
			}

			triggerLatch.await();

			assertFalse(task.isCanceledOrFailed());

			ExecutionState currentState = task.getExecutionState();
			assertThat(currentState, isOneOf(ExecutionState.RUNNING, ExecutionState.FINISHED));
		}
	}

	@Test
	public void testThrowExceptionIfStopInvokedWithNotStoppableTask() throws Exception {
		Task task = createTask(CheckpointsInOrderInvokable.class);
		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();
			awaitLatch.await();

			try {
				task.stopExecution();
				fail("Expected exception not thrown");
			} catch (UnsupportedOperationException e) {
				assertThat(e.getMessage(), containsString("Stopping not supported by task"));
			}
		}
	}

	/**
	 * Asserts that {@link AbstractInvokable#triggerCheckpoint(CheckpointMetaData, CheckpointOptions)},
	 * {@link AbstractInvokable#notifyCheckpointComplete(long)}, and {@link StoppableTask#stop()} are
	 * invoked by a thread whose context class loader is set to the user code class loader.
	 */
	@Test
	public void testSetsUserCodeClassLoader() throws Exception {
		numCalls = 1;

		Task task = createTask(ContextClassLoaderInterceptingInvokable.class);
		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();

			awaitLatch.await();

			task.triggerCheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation());
			task.notifyCheckpointComplete(1);
			task.stopExecution();

			triggerLatch.await();
			notifyCheckpointCompleteLatch.await();
			stopLatch.await();

			assertThat(classLoaders, hasSize(greaterThanOrEqualTo(3)));
			assertThat(classLoaders, everyItem(instanceOf(TestUserCodeClassLoader.class)));
		}
	}

	private Task createTask(Class<? extends AbstractInvokable> invokableClass) throws Exception {
		BlobCacheService blobService =
			new BlobCacheService(mock(PermanentBlobCache.class), mock(TransientBlobCache.class));

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(new TestUserCodeClassLoader());

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);
		TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
		NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
		when(networkEnvironment.getResultPartitionManager()).thenReturn(partitionManager);
		when(networkEnvironment.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(networkEnvironment.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));
		when(networkEnvironment.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		when(taskMetricGroup.getIOMetricGroup()).thenReturn(mock(TaskIOMetricGroup.class));

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
			networkEnvironment,
			mock(BroadcastVariableManager.class),
			new TestTaskStateManager(),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
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
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
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
	public static class ContextClassLoaderInterceptingInvokable extends CheckpointsInOrderInvokable implements StoppableTask {

		public ContextClassLoaderInterceptingInvokable(Environment environment) {
			super(environment);
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
			classLoaders.add(Thread.currentThread().getContextClassLoader());

			return super.triggerCheckpoint(checkpointMetaData, checkpointOptions);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			classLoaders.add(Thread.currentThread().getContextClassLoader());

			super.notifyCheckpointComplete(checkpointId);
		}

		@Override
		public void stop() {
			classLoaders.add(Thread.currentThread().getContextClassLoader());
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
