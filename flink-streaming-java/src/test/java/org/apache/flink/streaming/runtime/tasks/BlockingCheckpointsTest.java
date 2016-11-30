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
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.FallbackLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

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
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
			TestStreamTask.class.getName(),
			taskConfig);

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
			mock(NetworkEnvironment.class),
			mock(BroadcastVariableManager.class),
			mock(ActorGateway.class),
			mock(ActorGateway.class),
			new FiniteDuration(10, TimeUnit.SECONDS),
			new FallbackLibraryCacheManager(),
			new FileCache(new Configuration()),
			new TaskManagerRuntimeInfo(
					"localhost", new Configuration(), EnvironmentInformation.getTemporaryFileDirectory()),
			new UnregisteredTaskMetricsGroup());
		
	}

	// ------------------------------------------------------------------------
	//  state backend with locking output stream
	// ------------------------------------------------------------------------

	private static class LockingStreamStateBackend extends AbstractStateBackend {

		private static final long serialVersionUID = 1L;

		private final LockingOutputStream out = new LockingOutputStream();

		@Override
		public void disposeAllStateForCurrentJob() {}

		@Override
		public void close() throws IOException {
			out.close();
		}

		@Override
		public CheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
			return out;
		}

		@Override
		protected <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		protected <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		protected <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
			throw new UnsupportedOperationException();
		}

		@Override
		public <S extends Serializable> StateHandle<S> checkpointStateSerializable(S state, long checkpointID, long timestamp) throws Exception {
			throw new UnsupportedOperationException();
		}
	}

	private static final class LockingOutputStream extends CheckpointStateOutputStream implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final SerializableObject lock = new SerializableObject();
		private volatile boolean closed;

		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			throw new UnsupportedOperationException();
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
		public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
			AbstractStateBackend stateBackend = getStateBackend();
			CheckpointStateOutputStream outStream = stateBackend.createCheckpointStateOutputStream(checkpointId, timestamp);

			IN_CHECKPOINT_LATCH.trigger();

			// this should lock
			outStream.write(1);

			// this should be unreachable
			return null;
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
			triggerCheckpointOnBarrier(11L, System.currentTimeMillis());
		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() {}
	}
}
