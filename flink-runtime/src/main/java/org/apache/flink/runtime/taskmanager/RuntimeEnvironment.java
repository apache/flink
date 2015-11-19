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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.util.Map;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * In implementation of the {@link Environment}.
 */
public class RuntimeEnvironment implements Environment {
	
	private final JobID jobId;
	private final JobVertexID jobVertexId;
	private final ExecutionAttemptID executionId;
	
	private final TaskInfo taskInfo;
	
	private final Configuration jobConfiguration;
	private final Configuration taskConfiguration;
	
	private final ClassLoader userCodeClassLoader;

	private final MemoryManager memManager;
	private final IOManager ioManager;
	private final BroadcastVariableManager bcVarManager;
	private final InputSplitProvider splitProvider;
	
	private final Map<String, Future<Path>> distCacheEntries;

	private final ResultPartitionWriter[] writers;
	private final InputGate[] inputGates;
	
	private final ActorGateway jobManager;

	private final AccumulatorRegistry accumulatorRegistry;

	private final TaskManagerRuntimeInfo taskManagerInfo;

	// ------------------------------------------------------------------------

	public RuntimeEnvironment(
			JobID jobId,
			JobVertexID jobVertexId,
			ExecutionAttemptID executionId,
			TaskInfo taskInfo,
			Configuration jobConfiguration,
			Configuration taskConfiguration,
			ClassLoader userCodeClassLoader,
			MemoryManager memManager,
			IOManager ioManager,
			BroadcastVariableManager bcVarManager,
			AccumulatorRegistry accumulatorRegistry,
			InputSplitProvider splitProvider,
			Map<String, Future<Path>> distCacheEntries,
			ResultPartitionWriter[] writers,
			InputGate[] inputGates,
			ActorGateway jobManager,
			TaskManagerRuntimeInfo taskManagerInfo) {

		this.jobId = checkNotNull(jobId);
		this.jobVertexId = checkNotNull(jobVertexId);
		this.executionId = checkNotNull(executionId);
		this.taskInfo = checkNotNull(taskInfo);
		this.jobConfiguration = checkNotNull(jobConfiguration);
		this.taskConfiguration = checkNotNull(taskConfiguration);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.memManager = checkNotNull(memManager);
		this.ioManager = checkNotNull(ioManager);
		this.bcVarManager = checkNotNull(bcVarManager);
		this.accumulatorRegistry = checkNotNull(accumulatorRegistry);
		this.splitProvider = checkNotNull(splitProvider);
		this.distCacheEntries = checkNotNull(distCacheEntries);
		this.writers = checkNotNull(writers);
		this.inputGates = checkNotNull(inputGates);
		this.jobManager = checkNotNull(jobManager);
		this.taskManagerInfo = checkNotNull(taskManagerInfo);
	}

	// ------------------------------------------------------------------------
	
	@Override
	public JobID getJobID() {
		return jobId;
	}

	@Override
	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	@Override
	public TaskInfo getTaskInfo() {
		return this.taskInfo;
	}

	@Override
	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	@Override
	public Configuration getTaskConfiguration() {
		return taskConfiguration;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return taskManagerInfo;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return memManager;
	}

	@Override
	public IOManager getIOManager() {
		return ioManager;
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return bcVarManager;
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return splitProvider;
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return distCacheEntries;
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		return writers[index];
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return writers;
	}

	@Override
	public InputGate getInputGate(int index) {
		return inputGates[index];
	}

	@Override
	public InputGate[] getAllInputGates() {
		return inputGates;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId) {
		acknowledgeCheckpoint(checkpointId, null);
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
		// try and create a serialized version of the state handle
		SerializedValue<StateHandle<?>> serializedState;
		if (state == null) {
			serializedState = null;
		} else {
			try {
				serializedState = new SerializedValue<StateHandle<?>>(state);
			} catch (Exception e) {
				throw new RuntimeException("Failed to serialize state handle during checkpoint confirmation", e);
			}
		}
		
		AcknowledgeCheckpoint message = new AcknowledgeCheckpoint(jobId, executionId, checkpointId, serializedState);
		jobManager.tell(message);
	}
}
