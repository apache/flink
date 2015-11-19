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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -3233562176034358530L;

	/** The ID of the job the tasks belongs to. */
	private final JobID jobID;

	/** The task's job vertex ID. */
	private final JobVertexID vertexID;

	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The task's name. */
	private final String taskName;

	/** The task's index in the subtask group. */
	private final int indexInSubtaskGroup;

	/** The number of sub tasks. */
	private final int numberOfSubtasks;

	/** Attempt number the task */
	private final int attemptNumber;

	/** The configuration of the job the task belongs to. */
	private final Configuration jobConfiguration;

	/** The task's configuration object. */
	private final Configuration taskConfiguration;

	/** The name of the class containing the task code to be executed. */
	private final String invokableClassName;

	/** The list of produced intermediate result partition deployment descriptors. */
	private final List<ResultPartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private final List<InputGateDeploymentDescriptor> inputGates;

	private final int targetSlotNumber;

	/** The list of JAR files required to run this task. */
	private final List<BlobKey> requiredJarFiles;
	
	/** The list of classpaths required to run this task. */
	private final List<URL> requiredClasspaths;

	private final SerializedValue<StateHandle<?>> operatorState;

	private long recoveryTimestamp;
		
	/**
	 * Constructs a task deployment descriptor.
	 */
	public TaskDeploymentDescriptor(
			JobID jobID, JobVertexID vertexID, ExecutionAttemptID executionId, String taskName,
			int indexInSubtaskGroup, int numberOfSubtasks, int attemptNumber, Configuration jobConfiguration,
			Configuration taskConfiguration, String invokableClassName,
			List<ResultPartitionDeploymentDescriptor> producedPartitions,
			List<InputGateDeploymentDescriptor> inputGates,
			List<BlobKey> requiredJarFiles, List<URL> requiredClasspaths,
			int targetSlotNumber, SerializedValue<StateHandle<?>> operatorState, long recoveryTimestamp) {

		checkArgument(indexInSubtaskGroup >= 0);
		checkArgument(numberOfSubtasks > indexInSubtaskGroup);
		checkArgument(targetSlotNumber >= 0);
		checkArgument(attemptNumber >= 0);

		this.jobID = checkNotNull(jobID);
		this.vertexID = checkNotNull(vertexID);
		this.executionId = checkNotNull(executionId);
		this.taskName = checkNotNull(taskName);
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		this.numberOfSubtasks = numberOfSubtasks;
		this.attemptNumber = attemptNumber;
		this.jobConfiguration = checkNotNull(jobConfiguration);
		this.taskConfiguration = checkNotNull(taskConfiguration);
		this.invokableClassName = checkNotNull(invokableClassName);
		this.producedPartitions = checkNotNull(producedPartitions);
		this.inputGates = checkNotNull(inputGates);
		this.requiredJarFiles = checkNotNull(requiredJarFiles);
		this.requiredClasspaths = checkNotNull(requiredClasspaths);
		this.targetSlotNumber = targetSlotNumber;
		this.operatorState = operatorState;
		this.recoveryTimestamp = recoveryTimestamp;
	}

	public TaskDeploymentDescriptor(
			JobID jobID, JobVertexID vertexID, ExecutionAttemptID executionId, String taskName,
			int indexInSubtaskGroup, int numberOfSubtasks, int attemptNumber, Configuration jobConfiguration,
			Configuration taskConfiguration, String invokableClassName,
			List<ResultPartitionDeploymentDescriptor> producedPartitions,
			List<InputGateDeploymentDescriptor> inputGates,
			List<BlobKey> requiredJarFiles, List<URL> requiredClasspaths,
			int targetSlotNumber) {

		this(jobID, vertexID, executionId, taskName, indexInSubtaskGroup, numberOfSubtasks, attemptNumber,
				jobConfiguration, taskConfiguration, invokableClassName, producedPartitions,
				inputGates, requiredJarFiles, requiredClasspaths, targetSlotNumber, null, -1);
	}

	/**
	 * Returns the ID of the job the tasks belongs to.
	 */
	public JobID getJobID() {
		return jobID;
	}

	/**
	 * Returns the task's execution vertex ID.
	 */
	public JobVertexID getVertexID() {
		return vertexID;
	}

	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	/**
	 * Returns the task's name.
	 */
	public String getTaskName() {
		return taskName;
	}

	/**
	 * Returns the task's index in the subtask group.
	 *
	 * @return the task's index in the subtask group
	 */
	public int getIndexInSubtaskGroup() {
		return indexInSubtaskGroup;
	}

	/**
	 * Returns the current number of subtasks.
	 */
	public int getNumberOfSubtasks() {
		return numberOfSubtasks;
	}

	/**
	 * Returns the attempt number of the subtask
	 */
	public int getAttemptNumber() {
		return attemptNumber;
	}

	/**
	 * Returns the {@link TaskInfo} object for the subtask
	 */
	public TaskInfo getTaskInfo() {
		return new TaskInfo(taskName, indexInSubtaskGroup, numberOfSubtasks, attemptNumber);
	}

	/**
	 * Gets the number of the slot into which the task is to be deployed.
	 *
	 * @return The number of the target slot.
	 */
	public int getTargetSlotNumber() {
		return targetSlotNumber;
	}

	/**
	 * Returns the configuration of the job the task belongs to.
	 */
	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	/**
	 * Returns the task's configuration object.
	 */
	public Configuration getTaskConfiguration() {
		return taskConfiguration;
	}

	/**
	 * Returns the name of the class containing the task code to be executed.
	 */
	public String getInvokableClassName() {
		return invokableClassName;
	}

	public List<ResultPartitionDeploymentDescriptor> getProducedPartitions() {
		return producedPartitions;
	}

	public List<InputGateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	public List<BlobKey> getRequiredJarFiles() {
		return requiredJarFiles;
	}

	public List<URL> getRequiredClasspaths() {
		return requiredClasspaths;
	}

	@Override
	public String toString() {
		return String.format("TaskDeploymentDescriptor [job id: %s, job vertex id: %s, " +
						"execution id: %s, task name: %s (%d/%d), attempt: %d, invokable: %s, " +
						"produced partitions: %s, input gates: %s]",
				jobID, vertexID, executionId, taskName, indexInSubtaskGroup, numberOfSubtasks,
				attemptNumber, invokableClassName, collectionToString(producedPartitions),
				collectionToString(inputGates));
	}

	private String collectionToString(Collection<?> collection) {
		final StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");

		for (Object elem : collection) {
			strBuilder.append(elem.toString());
		}

		strBuilder.append("]");

		return strBuilder.toString();
	}

	public SerializedValue<StateHandle<?>> getOperatorState() {
		return operatorState;
	}
	
	public long getRecoveryTimestamp() {
		return recoveryTimestamp;
	}
}
