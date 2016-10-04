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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerConnection;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.SerializedValue;
import org.junit.Test;

import java.io.Serializable;
import java.net.URL;
import java.util.Collections;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractUdfStreamOperatorTest {

	@Test
	public void testLifeCycle() throws Exception {

		Configuration taskManagerConfig = new Configuration();

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStreamOperator(new LifecycleTrackingStreamSource(new MockSourceFunction()));
		cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Task task = createTask(SourceStreamTask.class, cfg, taskManagerConfig);

		task.startTaskThread();

		// wait for clean termination
		task.getExecutingThread().join();
		assertEquals(ExecutionState.FINISHED, task.getExecutionState());
	}

	private static class MockSourceFunction extends RichSourceFunction<Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<Long> ctx) {
		}

		@Override
		public void cancel() {
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			System.out.println("!setRuntimeContext");
			super.setRuntimeContext(t);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			System.out.println("!open");
			super.open(parameters);
		}

		@Override
		public void close() throws Exception {
			System.out.println("!close");
			super.close();
		}
	}

	private Task createTask(
			Class<? extends AbstractInvokable> invokable,
			StreamConfig taskConfig,
			Configuration taskManagerConfig) throws Exception {

		LibraryCacheManager libCache = mock(LibraryCacheManager.class);
		when(libCache.getClassLoader(any(JobID.class))).thenReturn(getClass().getClassLoader());

		ResultPartitionManager partitionManager = mock(ResultPartitionManager.class);
		ResultPartitionConsumableNotifier consumableNotifier = mock(ResultPartitionConsumableNotifier.class);
		PartitionStateChecker partitionStateChecker = mock(PartitionStateChecker.class);
		Executor executor = mock(Executor.class);

		NetworkEnvironment network = mock(NetworkEnvironment.class);
		when(network.getResultPartitionManager()).thenReturn(partitionManager);
		when(network.getDefaultIOMode()).thenReturn(IOManager.IOMode.SYNC);
		when(network.createKvStateTaskRegistry(any(JobID.class), any(JobVertexID.class)))
				.thenReturn(mock(TaskKvStateRegistry.class));

		TaskDeploymentDescriptor tdd = new TaskDeploymentDescriptor(
				new JobID(), "Job Name", new JobVertexID(), new ExecutionAttemptID(),
				new SerializedValue<>(new ExecutionConfig()),
				"Test Task", 1, 0, 1, 0,
				new Configuration(),
				taskConfig.getConfiguration(),
				invokable.getName(),
				Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
				Collections.<InputGateDeploymentDescriptor>emptyList(),
				Collections.<BlobKey>emptyList(),
				Collections.<URL>emptyList(),
				0);

		return new Task(
				tdd,
				mock(MemoryManager.class),
				mock(IOManager.class),
				network,
				mock(BroadcastVariableManager.class),
				mock(TaskManagerConnection.class),
				mock(InputSplitProvider.class),
				mock(CheckpointResponder.class),
				libCache,
				mock(FileCache.class),
				new TaskManagerRuntimeInfo("localhost", taskManagerConfig, System.getProperty("java.io.tmpdir")),
				new UnregisteredTaskMetricsGroup(),
				consumableNotifier,
				partitionStateChecker,
				executor);
	}

	static class LifecycleTrackingStreamSource<OUT, SRC extends SourceFunction<OUT>>
			extends StreamSource<OUT, SRC> implements Serializable {

		//private transient final AtomicInteger currentState;

		private static final long serialVersionUID = 2431488948886850562L;

		public LifecycleTrackingStreamSource(SRC sourceFunction) {
			super(sourceFunction);
		}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
			System.out.println("setup");
			super.setup(containingTask, config, output);
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			System.out.println("snapshotState");
			super.snapshotState(context);
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			System.out.println("initializeState");
			super.initializeState(context);
		}

		@Override
		public void open() throws Exception {
			System.out.println("open");
			super.open();
		}

		@Override
		public void close() throws Exception {
			System.out.println("close");
			super.close();
		}

		@Override
		public void dispose() throws Exception {
			super.dispose();
			System.out.println("dispose");
		}
	}
}
