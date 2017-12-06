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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.TransientBlobCache;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the StreamTask termination.
 */
public class StreamTaskTerminationTest extends TestLogger {

	public static final OneShotLatch RUN_LATCH = new OneShotLatch();
	public static final OneShotLatch CHECKPOINTING_LATCH = new OneShotLatch();
	private static final OneShotLatch CLEANUP_LATCH = new OneShotLatch();
	private static final OneShotLatch HANDLE_ASYNC_EXCEPTION_LATCH = new OneShotLatch();

	/**
	 * FLINK-6833
	 *
	 * <p>Tests that a finished stream task cannot be failed by an asynchronous checkpointing operation after
	 * the stream task has stopped running.
	 */
	@Test
	public void testConcurrentAsyncCheckpointCannotFailFinishedStreamTask() throws Exception {
		final Configuration taskConfiguration = new Configuration();
		final StreamConfig streamConfig = new StreamConfig(taskConfiguration);
		final NoOpStreamOperator<Long> noOpStreamOperator = new NoOpStreamOperator<>();

		final AbstractStateBackend blockingStateBackend = new BlockingStateBackend();

		streamConfig.setStreamOperator(noOpStreamOperator);
		streamConfig.setOperatorID(new OperatorID());
		streamConfig.setStateBackend(blockingStateBackend);

		final long checkpointId = 0L;
		final long checkpointTimestamp = 0L;

		final JobInformation jobInformation = new JobInformation(
			new JobID(),
			"Test Job",
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());

		final TaskInformation taskInformation = new TaskInformation(
			new JobVertexID(),
			"Test Task",
			1,
			1,
			BlockingStreamTask.class.getName(),
			taskConfiguration);

		final TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();

		TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();
		final NetworkEnvironment networkEnv = mock(NetworkEnvironment.class);
		when(networkEnv.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class))).thenReturn(mock(TaskKvStateRegistry.class));
		when(networkEnv.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);

		BlobCacheService blobService =
			new BlobCacheService(mock(PermanentBlobCache.class), mock(TransientBlobCache.class));

		final Task task = new Task(
			jobInformation,
			taskInformation,
			new ExecutionAttemptID(),
			new AllocationID(),
			0,
			0,
			Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
			Collections.<InputGateDeploymentDescriptor>emptyList(),
			0,
			null,
			new MemoryManager(32L * 1024L, 1),
			new IOManagerAsync(),
			networkEnv,
			mock(BroadcastVariableManager.class),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			blobService,
			new BlobLibraryCacheManager(
				blobService.getPermanentBlobService(),
				FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
				new String[0]),
			mock(FileCache.class),
			taskManagerRuntimeInfo,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			mock(ResultPartitionConsumableNotifier.class),
			mock(PartitionProducerStateChecker.class),
			Executors.directExecutor());

		CompletableFuture<Void> taskRun = CompletableFuture.runAsync(
			() -> task.run(),
			TestingUtils.defaultExecutor());

		// wait until the stream task started running
		RUN_LATCH.await();

		// trigger a checkpoint
		task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, CheckpointOptions.forCheckpoint());

		// wait until the task has completed execution
		taskRun.get();

		// check that no failure occurred
		if (task.getFailureCause() != null) {
			throw new Exception("Task failed", task.getFailureCause());
		}

		// check that we have entered the finished state
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	/**
	 * Blocking stream task which waits on and triggers a set of one shot latches to establish a certain
	 * interleaving with a concurrently running checkpoint operation.
	 */
	public static class BlockingStreamTask<T, OP extends StreamOperator<T>> extends StreamTask<T, OP> {

		public BlockingStreamTask() {
		}

		@Override
		protected void init() throws Exception {

		}

		@Override
		protected void run() throws Exception {
			RUN_LATCH.trigger();
			// wait until we have started an asynchronous checkpoint
			CHECKPOINTING_LATCH.await();
		}

		@Override
		protected void cleanup() throws Exception {
			// notify the asynchronous checkpoint operation that we have reached the cleanup stage --> the task
			// has been stopped
			CLEANUP_LATCH.trigger();

			// wait until handle async exception has been called to proceed with the termination of the
			// StreamTask
			HANDLE_ASYNC_EXCEPTION_LATCH.await();
		}

		@Override
		protected void cancelTask() throws Exception {
		}

		@Override
		public void handleAsyncException(String message, Throwable exception) {
			super.handleAsyncException(message, exception);

			HANDLE_ASYNC_EXCEPTION_LATCH.trigger();
		}
	}

	static class NoOpStreamOperator<T> extends AbstractStreamOperator<T> {
		private static final long serialVersionUID = 4517845269225218312L;
	}

	static class BlockingStateBackend extends AbstractStateBackend {

		private static final long serialVersionUID = -5053068148933314100L;

		@Override
		public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
			return mock(CheckpointStreamFactory.class);
		}

		@Override
		public CheckpointStreamFactory createSavepointStreamFactory(JobID jobId, String operatorIdentifier, @Nullable String targetLocation) throws IOException {
			return null;
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry) throws IOException {
			return null;
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception {
			OperatorStateBackend operatorStateBackend = mock(OperatorStateBackend.class);
			when(operatorStateBackend.snapshot(anyLong(), anyLong(), any(CheckpointStreamFactory.class), any(CheckpointOptions.class)))
				.thenReturn(new FutureTask<>(new BlockingCallable()));

			return operatorStateBackend;
		}
	}

	static class BlockingCallable implements Callable<OperatorStateHandle> {

		@Override
		public OperatorStateHandle call() throws Exception {
			// notify that we have started the asynchronous checkpointint operation
			CHECKPOINTING_LATCH.trigger();
			// wait until we have reached the StreamTask#cleanup --> This will already cancel this FutureTask
			CLEANUP_LATCH.await();

			// now throw exception to fail the async checkpointing operation if it has not already been cancelled
			// by the StreamTask in the meantime
			throw new FlinkException("Checkpointing operation failed");
		}
	}
}
