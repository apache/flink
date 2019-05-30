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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
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
import org.apache.flink.runtime.io.network.NetworkEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.SerializedValue;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that the cached thread pool used by the {@link Task} allows
 * synchronous checkpoints to complete successfully.
 */
public class SynchronousCheckpointITCase {

	private static OneShotLatch executionLatch;
	private static OneShotLatch cancellationLatch;
	private static OneShotLatch checkpointCompletionLatch;
	private static OneShotLatch notifyLatch;

	private static MultiShotLatch checkpointLatch;

	private static AtomicReference<Throwable> error = new AtomicReference<>();

	private static volatile CheckpointingStateHolder synchronousCheckpointPhase = new CheckpointingStateHolder();

	@Before
	public void initializeLatchesAndError() {
		executionLatch = new OneShotLatch();
		cancellationLatch = new OneShotLatch();
		checkpointCompletionLatch = new OneShotLatch();
		notifyLatch = new OneShotLatch();

		checkpointLatch = new MultiShotLatch();

		synchronousCheckpointPhase.setState(CheckpointingState.NONE);
		error.set(null);
	}

	@Test
	public void taskCachedThreadPoolAllowsForSynchronousCheckpoints() throws Exception {
		final Task task = createTask(SynchronousCheckpointTestingTask.class);

		try (TaskCleaner ignored = new TaskCleaner(task)) {
			task.startTaskThread();

			executionLatch.await();

			assertEquals(ExecutionState.RUNNING, task.getExecutionState());
			assertEquals(CheckpointingState.NONE, synchronousCheckpointPhase.getState());

			task.triggerCheckpointBarrier(
					42,
					156865867234L,
					new CheckpointOptions(CheckpointType.SYNC_SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
					false);
			checkpointLatch.await();

			assertNull(error.get());
			assertEquals(CheckpointingState.PERFORMING_CHECKPOINT, synchronousCheckpointPhase.getState());

			task.notifyCheckpointComplete(42);

			notifyLatch.await();
			assertNull(error.get());
			assertEquals(CheckpointingState.EXECUTED_CALLBACK, synchronousCheckpointPhase.getState());

			checkpointCompletionLatch.trigger();
			checkpointLatch.await();

			assertNull(error.get());
			assertEquals(CheckpointingState.FINISHED_CHECKPOINT, synchronousCheckpointPhase.getState());
			assertEquals(ExecutionState.RUNNING, task.getExecutionState());
		}
	}

	/**
	 * A {@link StreamTask} which makes sure that the different phases of a synchronous checkpoint
	 * are reflected in the {@link SynchronousCheckpointITCase#synchronousCheckpointPhase} field.
	 */
	public static class SynchronousCheckpointTestingTask extends StreamTask {

		public SynchronousCheckpointTestingTask(Environment environment) {
			super(environment);
		}

		@Override
		protected void performDefaultAction(ActionContext context) throws Exception {
			executionLatch.trigger();
			cancellationLatch.await();
			context.allActionsCompleted();
		}

		@Override
		protected void cancelTask() {
			cancellationLatch.trigger();
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) throws Exception {
			SynchronousCheckpointITCase.synchronousCheckpointPhase.setState(CheckpointingState.PERFORMING_CHECKPOINT);
			checkpointLatch.trigger();

			super.triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);

			checkpointCompletionLatch.await();
			SynchronousCheckpointITCase.synchronousCheckpointPhase.setState(CheckpointingState.FINISHED_CHECKPOINT);
			checkpointLatch.trigger();

			return true;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			SynchronousCheckpointITCase.synchronousCheckpointPhase.setState(CheckpointingState.EXECUTING_CALLBACK);
			super.notifyCheckpointComplete(checkpointId);

			SynchronousCheckpointITCase.synchronousCheckpointPhase.setState(CheckpointingState.EXECUTED_CALLBACK);
			notifyLatch.trigger();
		}

		@Override
		protected void init() {

		}

		@Override
		public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
			throw new UnsupportedOperationException("Should not be called");
		}

		@Override
		protected void cleanup() {

		}
	}

	/**
	 * The different state transitions during a synchronous checkpoint along with their expected previous state.
	 */
	private enum CheckpointingState {
		NONE(null),
		PERFORMING_CHECKPOINT(NONE),
		EXECUTING_CALLBACK(PERFORMING_CHECKPOINT),
		EXECUTED_CALLBACK(EXECUTING_CALLBACK),
		FINISHED_CHECKPOINT(EXECUTED_CALLBACK);

		private final CheckpointingState expectedPreviousState;

		CheckpointingState(final CheckpointingState previousState) {
			this.expectedPreviousState = previousState;
		}

		void checkValidStateTransition(final CheckpointingState actualPreviousState) {
			if (this.expectedPreviousState != actualPreviousState) {
				error.set(new AssertionError());
			}
		}
	}

	/**
	 * A container holding the current {@link CheckpointingState}.
	 */
	private static final class CheckpointingStateHolder {

		private volatile CheckpointingState checkpointingState = null;

		void setState(CheckpointingState state) {
			state.checkValidStateTransition(checkpointingState);
			checkpointingState = state;
		}

		CheckpointingState getState() {
			return checkpointingState;
		}
	}

	// --------------------------		Boilerplate tools copied from the TaskAsyncCallTest		--------------------------

	private Task createTask(Class<? extends AbstractInvokable> invokableClass) throws Exception {
		BlobCacheService blobService =
				new BlobCacheService(mock(PermanentBlobCache.class), mock(TransientBlobCache.class));

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());

		ResultPartitionConsumableNotifier consumableNotifier = new NoOpResultPartitionConsumableNotifier();
		PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		Executor executor = mock(Executor.class);
		ShuffleEnvironment shuffleEnvironment = new NetworkEnvironmentBuilder().build();

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
