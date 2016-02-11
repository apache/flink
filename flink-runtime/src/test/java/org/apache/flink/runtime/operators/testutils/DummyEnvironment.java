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

import java.util.Map;
import java.util.concurrent.Future;

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
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;

public class DummyEnvironment implements Environment {

	private final TaskInfo taskInfo;
	private final JobID jobId = new JobID();
	private final JobVertexID jobVertexId = new JobVertexID();

	public DummyEnvironment(String taskName, int numSubTasks, int subTaskIndex) {
		this.taskInfo = new TaskInfo(taskName, subTaskIndex, numSubTasks, 0);
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
		return null;
	}

	@Override
	public Configuration getTaskConfiguration() {
		return null;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return null;
	}

	@Override
	public Configuration getJobConfiguration() {
		return null;
	}

	@Override
	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return null;
	}

	@Override
	public IOManager getIOManager() {
		return null;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return null;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return null;
	}

	@Override
	public Map<String, Future<Path>> getDistributedCacheEntries() {
		return null;
	}

	@Override
	public BroadcastVariableManager getBroadcastVariableManager() {
		return null;
	}

	@Override
	public AccumulatorRegistry getAccumulatorRegistry() {
		return null;
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId) {
	}

	@Override
	public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
	}

	@Override
	public ResultPartitionWriter getWriter(int index) {
		return null;
	}

	@Override
	public ResultPartitionWriter[] getAllWriters() {
		return null;
	}

	@Override
	public InputGate getInputGate(int index) {
		return null;
	}

	@Override
	public InputGate[] getAllInputGates() {
		return null;
	}

}
