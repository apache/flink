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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.RecordCollectingResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.fail;

/**
 * IMPORTANT! Remember to close environment after usage!
 */
public class MockEnvironment implements Environment, AutoCloseable {
	
	private final TaskInfo taskInfo;
	
	private final ExecutionConfig executionConfig;

	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final TaskStateManager taskStateManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<InputGate> inputs;

	private final List<ResultPartitionWriter> outputs;

	private final JobID jobID;

	private final JobVertexID jobVertexID;

	private final BroadcastVariableManager bcVarManager = new BroadcastVariableManager();

	private final AccumulatorRegistry accumulatorRegistry;

	private final TaskKvStateRegistry kvStateRegistry;

	private final int bufferSize;

	private final ClassLoader userCodeClassLoader;

	private final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

	private Optional<Class<Throwable>> expectedExternalFailureCause = Optional.empty();

	private Optional<Throwable> actualExternalFailureCause = Optional.empty();

	public MockEnvironment() {
		this(
			"mock-task",
			1024 * MemoryManager.DEFAULT_PAGE_SIZE,
			null,
			16,
			new TestTaskStateManager());
	}

	public MockEnvironment(
		String taskName,
		long memorySize,
		MockInputSplitProvider inputSplitProvider,
		int bufferSize,
		TaskStateManager taskStateManager) {
		this(
			taskName,
			memorySize,
			inputSplitProvider,
			bufferSize,
			new Configuration(),
			new ExecutionConfig(),
			taskStateManager);
	}

	public MockEnvironment(
		String taskName,
		long memorySize,
		MockInputSplitProvider inputSplitProvider,
		int bufferSize, Configuration taskConfiguration,
		ExecutionConfig executionConfig,
		TaskStateManager taskStateManager) {
		this(
			taskName,
			memorySize,
			inputSplitProvider,
			bufferSize,
			taskConfiguration,
			executionConfig,
			taskStateManager,
			1,
			1,
			0);
	}

	public MockEnvironment(
			String taskName,
			long memorySize,
			MockInputSplitProvider inputSplitProvider,
			int bufferSize,
			Configuration taskConfiguration,
			ExecutionConfig executionConfig,
			TaskStateManager taskStateManager,
			int maxParallelism,
			int parallelism,
			int subtaskIndex) {
		this(
			taskName,
			memorySize,
			inputSplitProvider,
			bufferSize,
			taskConfiguration,
			executionConfig,
			taskStateManager,
			maxParallelism,
			parallelism,
			subtaskIndex,
			Thread.currentThread().getContextClassLoader());

	}

	public MockEnvironment(
			String taskName,
			long memorySize,
			MockInputSplitProvider inputSplitProvider,
			int bufferSize,
			Configuration taskConfiguration,
			ExecutionConfig executionConfig,
			TaskStateManager taskStateManager,
			int maxParallelism,
			int parallelism,
			int subtaskIndex,
			ClassLoader userCodeClassLoader) {
		this(
			new JobID(),
			new JobVertexID(),
			taskName,
			memorySize,
			inputSplitProvider,
			bufferSize,
			taskConfiguration,
			executionConfig,
			taskStateManager,
			maxParallelism,
			parallelism,
			subtaskIndex,
			userCodeClassLoader);
	}

	public MockEnvironment(
		JobID jobID,
		JobVertexID jobVertexID,
		String taskName,
		long memorySize,
		MockInputSplitProvider inputSplitProvider,
		int bufferSize,
		Configuration taskConfiguration,
		ExecutionConfig executionConfig,
		TaskStateManager taskStateManager,
		int maxParallelism,
		int parallelism,
		int subtaskIndex,
		ClassLoader userCodeClassLoader) {

		this.jobID = jobID;
		this.jobVertexID = jobVertexID;

		this.taskInfo = new TaskInfo(taskName, maxParallelism, subtaskIndex, parallelism, 0);
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = taskConfiguration;
		this.inputs = new LinkedList<InputGate>();
		this.outputs = new LinkedList<ResultPartitionWriter>();

		this.memManager = new MemoryManager(memorySize, 1);
		this.ioManager = new IOManagerAsync();

		this.executionConfig = executionConfig;
		this.inputSplitProvider = inputSplitProvider;
		this.bufferSize = bufferSize;

		this.accumulatorRegistry = new AccumulatorRegistry(jobID, getExecutionId());

		KvStateRegistry registry = new KvStateRegistry();
		this.kvStateRegistry = registry.createTaskRegistry(jobID, getJobVertexId());

		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
	}


	public IteratorWrappingTestSingleInputGate<Record> addInput(MutableObjectIterator<Record> inputIterator) {
		try {
			final IteratorWrappingTestSingleInputGate<Record> reader = new IteratorWrappingTestSingleInputGate<Record>(bufferSize, Record.class, inputIterator);

			inputs.add(reader.getInputGate());

			return reader;
		}
		catch (Throwable t) {
			throw new RuntimeException("Error setting up mock readers: " + t.getMessage(), t);
		}
	}

	public void addOutput(final List<Record> outputList) {
		try {
			outputs.add(new RecordCollectingResultPartitionWriter(outputList, new TestPooledBufferProvider(Integer.MAX_VALUE)));
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}
	}

	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}

	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return this.executionConfig;
	}

	@Override
	public JobID getJobID() {
		return this.jobID;
	}

	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return new TestingTaskManagerRuntimeInfo();
	}

	@Override
	public TaskMetricGroup getMetricGroup() {
		return UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	@Override
	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return Collections.emptyMap();
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		return outputs.get(index);
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return outputs.toArray(new ResultPartitionWriter[outputs.size()]);
	}

	@Override
	public InputGate getInputGate(int index) {
		return inputs.get(index);
	}

	@Override
	public InputGate[] getAllInputGates() {
		InputGate[] gates = new InputGate[inputs.size()];
		inputs.toArray(gates);
		return gates;
	}

	@Override
	public TaskEventDispatcher getTaskEventDispatcher() {
		return taskEventDispatcher;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return jobVertexID;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return new ExecutionAttemptID(0L, 0L);
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return this.bcVarManager;
	}

	@Override
	public TaskStateManager getTaskStateManager() {
		return taskStateManager;
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		return this.accumulatorRegistry;
	}

	@Override
	public TaskKvStateRegistry getTaskKvStateRegistry() {
		return kvStateRegistry;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void declineCheckpoint(long checkpointId, Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void failExternally(Throwable cause) {
		if (!expectedExternalFailureCause.isPresent()) {
			throw new UnsupportedOperationException("MockEnvironment does not support external task failure.");
		}
		checkArgument(expectedExternalFailureCause.get().isInstance(checkNotNull(cause)));
		checkState(!actualExternalFailureCause.isPresent());
		actualExternalFailureCause = Optional.of(cause);
	}

	@Override
	public void close() {
		// close() method should be idempotent and calling memManager.verifyEmpty() will throw after it was shutdown.
		if (!memManager.isShutdown()) {
			checkState(memManager.verifyEmpty(), "Memory Manager managed memory was not completely freed.");
		}

		memManager.shutdown();
		ioManager.shutdown();

		checkState(ioManager.isProperlyShutDown(), "IO Manager has not properly shut down.");
	}

	public void setExpectedExternalFailureCause(Class<Throwable> expectedThrowableClass) {
		this.expectedExternalFailureCause = Optional.of(expectedThrowableClass);
	}

	public Optional<Throwable> getActualExternalFailureCause() {
		return actualExternalFailureCause;
	}
}
