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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the StreamTask termination.
 */
public class StreamTaskTerminationTest extends TestLogger {

	private static final OneShotLatch RUN_LATCH = new OneShotLatch();
	private static final AtomicBoolean SNAPSHOT_HAS_STARTED = new AtomicBoolean();
	private static final OneShotLatch CLEANUP_LATCH = new OneShotLatch();

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(10);

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

		final StateBackend blockingStateBackend = new BlockingStateBackend();

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

		final ShuffleEnvironment<?, ?> shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

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
			MemoryManagerBuilder.newBuilder().setMemorySize(32L * 1024L).build(),
			new IOManagerAsync(),
			shuffleEnvironment,
			new KvStateService(new KvStateRegistry(), null, null),
			mock(BroadcastVariableManager.class),
			new TaskEventDispatcher(),
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			new TestTaskStateManager(),
			mock(TaskManagerActions.class),
			mock(InputSplitProvider.class),
			mock(CheckpointResponder.class),
			new NoOpTaskOperatorEventGateway(),
			new TestGlobalAggregateManager(),
			TestingClassLoaderLease.newBuilder().build(),
			mock(FileCache.class),
			taskManagerRuntimeInfo,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
			new NoOpResultPartitionConsumableNotifier(),
			mock(PartitionProducerStateChecker.class),
			Executors.directExecutor());

		CompletableFuture<Void> taskRun = CompletableFuture.runAsync(
			() -> task.run(),
			TestingUtils.defaultExecutor());

		// wait until the stream task started running
		RUN_LATCH.await();

		// trigger a checkpoint
		task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, CheckpointOptions.forCheckpointWithDefaultLocation(), false);

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

		private boolean isRunning;

		public BlockingStreamTask(Environment env) throws Exception {
			super(env);
		}

		@Override
		protected void init() {
		}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
			if (!isRunning) {
				isRunning = true;
				RUN_LATCH.trigger();
			}
			// wait until we have started an asynchronous checkpoint
			if (isCanceled() || SNAPSHOT_HAS_STARTED.get()) {
				controller.allActionsCompleted();
			}
		}

		@Override
		protected void cleanup() throws Exception {
			// notify the asynchronous checkpoint operation that we have reached the cleanup stage --> the task
			// has been stopped
			CLEANUP_LATCH.trigger();

			// wait until all async checkpoint threads are terminated, so that no more exceptions can be reported
			Assert.assertTrue(getAsyncOperationsThreadPool().awaitTermination(30L, TimeUnit.SECONDS));
		}
	}

	private static class NoOpStreamOperator<T> extends AbstractStreamOperator<T> {
		private static final long serialVersionUID = 4517845269225218312L;
	}

	static class BlockingStateBackend implements StateBackend {

		private static final long serialVersionUID = -5053068148933314100L;

		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) {
			throw new UnsupportedOperationException();
		}

		@Override
		public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
			return new MemoryBackendCheckpointStorage(jobId, null, null, Integer.MAX_VALUE);
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
			Environment env,
			JobID jobID,
			String operatorIdentifier,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			TaskKvStateRegistry kvStateRegistry,
			TtlTimeProvider ttlTimeProvider,
			MetricGroup metricGroup,
			@Nonnull Collection<KeyedStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) {
			return null;
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
			Environment env,
			String operatorIdentifier,
			@Nonnull Collection<OperatorStateHandle> stateHandles,
			CloseableRegistry cancelStreamRegistry) throws Exception {
			OperatorStateBackend operatorStateBackend = mock(OperatorStateBackend.class);
			when(operatorStateBackend.snapshot(anyLong(), anyLong(), any(CheckpointStreamFactory.class), any(CheckpointOptions.class)))
				.thenReturn(new FutureTask<>(new BlockingCallable()));

			return operatorStateBackend;
		}
	}

	static class BlockingCallable implements Callable<SnapshotResult<OperatorStateHandle>> {

		@Override
		public SnapshotResult<OperatorStateHandle> call() throws Exception {
			// notify that we have started the asynchronous checkpointed operation
			SNAPSHOT_HAS_STARTED.set(true);
			// wait until we have reached the StreamTask#cleanup --> This will already cancel this FutureTask
			CLEANUP_LATCH.await();

			// now throw exception to fail the async checkpointing operation if it has not already been cancelled
			// by the StreamTask in the meantime
			throw new FlinkException("Checkpointing operation failed");
		}
	}
}
