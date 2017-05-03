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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.FallbackLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This test checks that task checkpoints that block and do not react to thread interrupts
 * are
 */
public class BlockingCheckpointsTest {

	private static final OneShotLatch IN_CHECKPOINT_LATCH = new OneShotLatch();

	@Test
	public void testBlockingNonInterruptibleCheckpoint() throws Exception {

		Configuration taskConfig = new Configuration();
		StreamConfig cfg = new StreamConfig(taskConfig);
		cfg.setStreamOperator(new TestOperator());
		cfg.setStateBackend(new LockingStreamStateBackend());

		Task task = createTask(taskConfig);

		// start the task and wait until it is in "restore"
		task.startTaskThread();
		IN_CHECKPOINT_LATCH.await();

		// cancel the task and wait. unless cancellation properly closes
		// the streams, this will never terminate
		task.cancelExecution();
		task.getExecutingThread().join();

		assertEquals(ExecutionState.CANCELED, task.getExecutionState());
		assertNull(task.getFailureCause());
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static Task createTask(Configuration taskConfig) throws IOException {

		JobInformation jobInformation = new JobInformation(
				new JobID(),
				"test job name",
				new SerializedValue<>(new ExecutionConfig()),
				new Configuration(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList());

		TaskInformation taskInformation = new TaskInformation(
				new JobVertexID(),
				"test task name",
				1,
				11,
				TestStreamTask.class.getName(),
				taskConfig);

		TaskKvStateRegistry mockKvRegistry = mock(TaskKvStateRegistry.class);
		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class))).thenReturn(mockKvRegistry);

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
				null,
				mock(MemoryManager.class),
				mock(IOManager.class),
				network,
				mock(BroadcastVariableManager.class),
				mock(TaskManagerActions.class),
				mock(InputSplitProvider.class),
				mock(CheckpointResponder.class),
				new FallbackLibraryCacheManager(),
				new FileCache(new String[] { EnvironmentInformation.getTemporaryFileDirectory() }),
				new TestingTaskManagerRuntimeInfo(),
				new UnregisteredTaskMetricsGroup(),
				mock(ResultPartitionConsumableNotifier.class),
				mock(PartitionProducerStateChecker.class),
				Executors.directExecutor());
	}

	// ------------------------------------------------------------------------
	//  state backend with locking output stream
	// ------------------------------------------------------------------------

	private static class LockingStreamStateBackend extends AbstractStateBackend {

		private static final long serialVersionUID = 1L;

		@Override
		public CheckpointStreamFactory createStreamFactory(JobID jobId, String operatorIdentifier) throws IOException {
			return new LockingOutputStreamFactory();
		}

		@Override
		public CheckpointStreamFactory createSavepointStreamFactory(JobID jobId, String operatorIdentifier, String targetLocation) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
				Environment env, JobID jobID, String operatorIdentifier,
				TypeSerializer<K> keySerializer, int numberOfKeyGroups,
				KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry) {

			throw new UnsupportedOperationException();
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier) throws Exception {
			return new DefaultOperatorStateBackend(
				getClass().getClassLoader(),
				new ExecutionConfig(),
				true);
		}
	}

	private static final class LockingOutputStreamFactory implements CheckpointStreamFactory {

		@Override
		public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) {
			return new LockingOutputStream();
		}

		@Override
		public void close() {}
	}

	private static final class LockingOutputStream extends CheckpointStateOutputStream {

		private final Object lock = new Object();
		private volatile boolean closed;

		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			return null;
		}

		@Override
		public void write(int b) throws IOException {
			// this needs to not react to interrupts until the handle is closed
			synchronized (lock) {
				while (!closed) {
					try {
						lock.wait();
					}
					catch (InterruptedException ignored) {}
				}
			}
		}

		@Override
		public void close() throws IOException {
			synchronized (lock) {
				closed = true;
				lock.notifyAll();
			}
		}

		@Override
		public long getPos() {
			return 0;
		}

		@Override
		public void flush() {}

		@Override
		public void sync() {}
	}

	// ------------------------------------------------------------------------
	//  test source operator that calls into the locking checkpoint output stream
	// ------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class TestOperator extends StreamFilter<Object> {
		private static final long serialVersionUID = 1L;

		public TestOperator() {
			super(new FilterFunction<Object>() {
				@Override
				public boolean filter(Object value) {
					return false;
				}
			});
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			OperatorStateCheckpointOutputStream outStream = context.getRawOperatorStateOutput();

			IN_CHECKPOINT_LATCH.trigger();

			// this should lock
			outStream.write(1);
		}
	}

	// ------------------------------------------------------------------------
	//  stream task that simply triggers a checkpoint
	// ------------------------------------------------------------------------

	public static final class TestStreamTask extends OneInputStreamTask<Object, Object> {

		@Override
		public void init() {}

		@Override
		protected void run() throws Exception {
			triggerCheckpointOnBarrier(new CheckpointMetaData(11L, System.currentTimeMillis()), CheckpointOptions.forFullCheckpoint(), new CheckpointMetrics());
		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() {}
	}
}
