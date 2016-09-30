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
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStateHandles;

import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
	private final ExecutionConfig executionConfig;

	private final ClassLoader userCodeClassLoader;

	private final MemoryManager memManager;
	private final IOManager ioManager;
	private final BroadcastVariableManager bcVarManager;
	private final InputSplitProvider splitProvider;
	
	private final Map<String, Future<Path>> distCacheEntries;

	private final ResultPartitionWriter[] writers;
	private final InputGate[] inputGates;
	
	private final CheckpointResponder checkpointResponder;

	private final AccumulatorRegistry accumulatorRegistry;

	private final TaskKvStateRegistry kvStateRegistry;

	private final TaskManagerRuntimeInfo taskManagerInfo;
	private final TaskMetricGroup metrics;

	private final Task containingTask;

	// ------------------------------------------------------------------------

	public RuntimeEnvironment(
			JobID jobId,
			JobVertexID jobVertexId,
			ExecutionAttemptID executionId,
			ExecutionConfig executionConfig,
			TaskInfo taskInfo,
			Configuration jobConfiguration,
			Configuration taskConfiguration,
			ClassLoader userCodeClassLoader,
			MemoryManager memManager,
			IOManager ioManager,
			BroadcastVariableManager bcVarManager,
			AccumulatorRegistry accumulatorRegistry,
			TaskKvStateRegistry kvStateRegistry,
			InputSplitProvider splitProvider,
			Map<String, Future<Path>> distCacheEntries,
			ResultPartitionWriter[] writers,
			InputGate[] inputGates,
			CheckpointResponder checkpointResponder,
			TaskManagerRuntimeInfo taskManagerInfo,
			TaskMetricGroup metrics,
			Task containingTask) {

		this.jobId = checkNotNull(jobId);
		this.jobVertexId = checkNotNull(jobVertexId);
		this.executionId = checkNotNull(executionId);
		this.taskInfo = checkNotNull(taskInfo);
		this.executionConfig = checkNotNull(executionConfig);
		this.jobConfiguration = checkNotNull(jobConfiguration);
		this.taskConfiguration = checkNotNull(taskConfiguration);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.memManager = checkNotNull(memManager);
		this.ioManager = checkNotNull(ioManager);
		this.bcVarManager = checkNotNull(bcVarManager);
		this.accumulatorRegistry = checkNotNull(accumulatorRegistry);
		this.kvStateRegistry = checkNotNull(kvStateRegistry);
		this.splitProvider = checkNotNull(splitProvider);
		this.distCacheEntries = checkNotNull(distCacheEntries);
		this.writers = checkNotNull(writers);
		this.inputGates = checkNotNull(inputGates);
		this.checkpointResponder = checkNotNull(checkpointResponder);
		this.taskManagerInfo = checkNotNull(taskManagerInfo);
		this.containingTask = containingTask;
		this.metrics = metrics;
	}

	// ------------------------------------------------------------------------

	@Override
	public ExecutionConfig getExecutionConfig() {
		return this.executionConfig;
	}

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
	public TaskMetricGroup getMetricGroup() {
		return metrics;
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
	public TaskKvStateRegistry getTaskKvStateRegistry() {
		return kvStateRegistry;
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
	public void acknowledgeCheckpoint(
			long checkpointId,
			long synchronousDurationMillis,
			long asynchronousDurationMillis,
			long bytesBufferedInAlignment,
			long alignmentDurationNanos) {

		acknowledgeCheckpoint(checkpointId, null,
				synchronousDurationMillis, asynchronousDurationMillis,
				bytesBufferedInAlignment, alignmentDurationNanos);
	}

	@Override
	public void acknowledgeCheckpoint(
			long checkpointId,
			CheckpointStateHandles checkpointStateHandles,
			long synchronousDurationMillis,
			long asynchronousDurationMillis,
			long bytesBufferedInAlignment,
			long alignmentDurationNanos) {


		checkpointResponder.acknowledgeCheckpoint(
				jobId, executionId, checkpointId,
				checkpointStateHandles,
				synchronousDurationMillis, asynchronousDurationMillis,
				bytesBufferedInAlignment, alignmentDurationNanos);
	}

	@Override
	public void failExternally(Throwable cause) {
		this.containingTask.failExternally(cause);
	}
}
