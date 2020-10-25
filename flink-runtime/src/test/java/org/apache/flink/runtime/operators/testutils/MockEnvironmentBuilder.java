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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

public class MockEnvironmentBuilder {
	private String taskName = "mock-task";
	private MockInputSplitProvider inputSplitProvider = null;
	private int bufferSize = 16;
	private TaskStateManager taskStateManager = new TestTaskStateManager();
	private GlobalAggregateManager aggregateManager = new TestGlobalAggregateManager();
	private Configuration taskConfiguration = new Configuration();
	private ExecutionConfig executionConfig = new ExecutionConfig();
	private int maxParallelism = 1;
	private int parallelism = 1;
	private int subtaskIndex = 0;
	private UserCodeClassLoader userCodeClassLoader = TestingUserCodeClassLoader.newBuilder().build();
	private JobID jobID = new JobID();
	private JobVertexID jobVertexID = new JobVertexID();
	private TaskMetricGroup taskMetricGroup = UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
	private TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();
	private IOManager ioManager;
	private MemoryManager memoryManager = buildMemoryManager(1024 * MemoryManager.DEFAULT_PAGE_SIZE);
	private ExternalResourceInfoProvider externalResourceInfoProvider = ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;

	private MemoryManager buildMemoryManager(long memorySize) {
		return MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();
	}

	public MockEnvironmentBuilder setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public MockEnvironmentBuilder setManagedMemorySize(long managedMemorySize) {
		this.memoryManager = buildMemoryManager(managedMemorySize);
		return this;
	}

	public MockEnvironmentBuilder setInputSplitProvider(MockInputSplitProvider inputSplitProvider) {
		this.inputSplitProvider = inputSplitProvider;
		return this;
	}

	public MockEnvironmentBuilder setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public MockEnvironmentBuilder setTaskStateManager(TaskStateManager taskStateManager) {
		this.taskStateManager = taskStateManager;
		return this;
	}

	public MockEnvironmentBuilder setTaskConfiguration(Configuration taskConfiguration) {
		this.taskConfiguration = taskConfiguration;
		return this;
	}

	public MockEnvironmentBuilder setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
		return this;
	}

	public MockEnvironmentBuilder setTaskManagerRuntimeInfo(TaskManagerRuntimeInfo taskManagerRuntimeInfo) {
		this.taskManagerRuntimeInfo = taskManagerRuntimeInfo;
		return this;
	}

	public MockEnvironmentBuilder setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
		return this;
	}

	public MockEnvironmentBuilder setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public MockEnvironmentBuilder setSubtaskIndex(int subtaskIndex) {
		this.subtaskIndex = subtaskIndex;
		return this;
	}

	public MockEnvironmentBuilder setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = TestingUserCodeClassLoader.newBuilder()
			.setClassLoader(userCodeClassLoader)
			.build();
		return this;
	}

	public MockEnvironmentBuilder setJobID(JobID jobID) {
		this.jobID = jobID;
		return this;
	}

	public MockEnvironmentBuilder setJobVertexID(JobVertexID jobVertexID) {
		this.jobVertexID = jobVertexID;
		return this;
	}

	public MockEnvironmentBuilder setMetricGroup(TaskMetricGroup taskMetricGroup) {
		this.taskMetricGroup = taskMetricGroup;
		return this;
	}

	public MockEnvironmentBuilder setIOManager(IOManager ioManager) {
		this.ioManager = ioManager;
		return this;
	}

	public MockEnvironmentBuilder setMemoryManager(MemoryManager memoryManager) {
		this.memoryManager = memoryManager;
		return this;
	}

	public MockEnvironmentBuilder setExternalResourceInfoProvider(ExternalResourceInfoProvider externalResourceInfoProvider) {
		this.externalResourceInfoProvider = externalResourceInfoProvider;
		return this;
	}

	public MockEnvironment build() {
		if (ioManager == null) {
			ioManager = new IOManagerAsync();
		}
		return new MockEnvironment(
			jobID,
			jobVertexID,
			taskName,
			inputSplitProvider,
			bufferSize,
			taskConfiguration,
			executionConfig,
			ioManager,
			taskStateManager,
			aggregateManager,
			maxParallelism,
			parallelism,
			subtaskIndex,
			userCodeClassLoader,
			taskMetricGroup,
			taskManagerRuntimeInfo,
			memoryManager,
			externalResourceInfoProvider);
	}
}
