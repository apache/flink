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
package org.apache.flink.runtime.testutils.recordutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskDeploymentDescriptorBuilder {
	private JobID jobID;
	private JobVertexID jobVertexID;
	private ExecutionAttemptID executionId;
	private ExecutionConfig executionConfig;
	private String taskName;
	private Integer subtaskIndex;
	private Integer subtaskCount;
	private Integer attemptNumber;
	private Configuration jobConfiguration;
	private Configuration taskConfiguration;
	private String invokableClassName;
	private List<ResultPartitionDeploymentDescriptor> producedPartitions;
	private List<InputGateDeploymentDescriptor> inputGates;
	private List<BlobKey> requiredJarFiles;
	private List<URL> requiredClasspaths;
	private Integer targetSlotNumber;
	private SerializedValue<StateHandle<?>> operatorState;
	private Long recoveryTimestamp;

	public TaskDeploymentDescriptorBuilder setJobID(JobID jobID) {
		this.jobID = jobID;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setJobVertexID(JobVertexID vertexID) {
		this.jobVertexID = vertexID;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setExecutionAttemptID(ExecutionAttemptID executionAttemptID) {
		this.executionId = executionAttemptID;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setSubtaskIndex(int subtaskIndex) {
		this.subtaskIndex = subtaskIndex;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setSubtaskCount(int subtaskCount) {
		this.subtaskCount = subtaskCount;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setAttemptNumber(int attemptNumber) {
		this.attemptNumber = attemptNumber;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setJobConfiguration(Configuration configuration) {
		this.jobConfiguration = configuration;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setTaskConfiguration(Configuration configuration) {
		this.taskConfiguration = configuration;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setInvokableClassName(String invokableClassName) {
		this.invokableClassName = invokableClassName;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setPartitions(List<ResultPartitionDeploymentDescriptor> partitions) {
		this.producedPartitions = partitions;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setInputGates(List<InputGateDeploymentDescriptor> inputGates) {
		this.inputGates = inputGates;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setReguiredJarFiles(List<BlobKey> requiredJarFiles) {
		this.requiredJarFiles = requiredJarFiles;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setClassPaths(List<URL> requiredClasspaths) {
		this.requiredClasspaths = requiredClasspaths;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setTargetSlotNumber(int targetSlotNumber) {
		this.targetSlotNumber = targetSlotNumber;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setOperatorState(SerializedValue<StateHandle<?>> operatorState) {
		this.operatorState = operatorState;
		return this;
	}

	public TaskDeploymentDescriptorBuilder setRecoveryTimeStamp(long recoveryTimestamp) {
		this.recoveryTimestamp = recoveryTimestamp;
		return this;
	}

	public TaskDeploymentDescriptor build() {
		if (invokableClassName == null) {
			throw new IllegalArgumentException("InvokableClassName must not be null.");
		}
		return new TaskDeploymentDescriptor(
			jobID == null ? new JobID() : jobID,
			jobVertexID == null ? new JobVertexID() : jobVertexID,
			executionId == null ? new ExecutionAttemptID() : executionId,
			executionConfig == null ? new ExecutionConfig() : executionConfig,
			taskName == null ? "Test Task" : taskName,
			subtaskIndex == null ? 0 : subtaskIndex,
			subtaskCount == null ? 1 : subtaskCount,
			attemptNumber == null ? 0 : attemptNumber,
			jobConfiguration == null ? new Configuration() : jobConfiguration,
			taskConfiguration == null ? new Configuration() : taskConfiguration,
			invokableClassName,
			producedPartitions == null ? Collections.<ResultPartitionDeploymentDescriptor>emptyList() : producedPartitions,
			inputGates == null ? Collections.<InputGateDeploymentDescriptor>emptyList() : inputGates,
			requiredJarFiles == null ? new ArrayList<BlobKey>() : requiredJarFiles,
			requiredClasspaths == null ? new ArrayList<URL>() : requiredClasspaths,
			targetSlotNumber == null ? 0 : targetSlotNumber,
			operatorState,
			recoveryTimestamp == null ? -1 : recoveryTimestamp
		);
	}
}
