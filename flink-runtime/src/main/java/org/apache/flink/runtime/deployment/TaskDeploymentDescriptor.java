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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.OperatorState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
	private String taskName;

	/** The task's index in the subtask group. */
	private int indexInSubtaskGroup;

	/** The number of sub tasks. */
	private int numberOfSubtasks;

	/** The configuration of the job the task belongs to. */
	private Configuration jobConfiguration;

	/** The task's configuration object. */
	private Configuration taskConfiguration;

	/** The name of the class containing the task code to be executed. */
	private String invokableClassName;


	/** The list of produced intermediate result partition deployment descriptors. */
	private List<PartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private List<PartitionConsumerDeploymentDescriptor> consumedPartitions;

	private int targetSlotNumber;

	/** The list of JAR files required to run this task. */
	private final List<BlobKey> requiredJarFiles;
	
	private Map<String, OperatorState<?>> operatorStates;

	/**
	 * Constructs a task deployment descriptor.
	 */
	public TaskDeploymentDescriptor(
			JobID jobID, JobVertexID vertexID,  ExecutionAttemptID executionId,  String taskName,
			int indexInSubtaskGroup,  int numberOfSubtasks, Configuration jobConfiguration,
			Configuration taskConfiguration, String invokableClassName,
			List<PartitionDeploymentDescriptor> producedPartitions,
			List<PartitionConsumerDeploymentDescriptor> consumedPartitions,
			List<BlobKey> requiredJarFiles, int targetSlotNumber){

		this.jobID = checkNotNull(jobID);
		this.vertexID = checkNotNull(vertexID);
		this.executionId = checkNotNull(executionId);
		this.taskName = checkNotNull(taskName);
		checkArgument(indexInSubtaskGroup >= 0);
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		checkArgument(numberOfSubtasks > indexInSubtaskGroup);
		this.numberOfSubtasks = numberOfSubtasks;
		this.jobConfiguration = checkNotNull(jobConfiguration);
		this.taskConfiguration = checkNotNull(taskConfiguration);
		this.invokableClassName = checkNotNull(invokableClassName);
		this.producedPartitions = checkNotNull(producedPartitions);
		this.consumedPartitions = checkNotNull(consumedPartitions);
		this.requiredJarFiles = checkNotNull(requiredJarFiles);
		this.targetSlotNumber = targetSlotNumber;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TaskDeploymentDescriptor() {
		this.jobID = new JobID();
		this.vertexID = new JobVertexID();
		this.executionId = new ExecutionAttemptID();
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.producedPartitions = new ArrayList<PartitionDeploymentDescriptor>();
		this.consumedPartitions = new ArrayList<PartitionConsumerDeploymentDescriptor>();
		this.requiredJarFiles = new ArrayList<BlobKey>();
	}

	public TaskDeploymentDescriptor(
			JobID jobID, JobVertexID vertexID, ExecutionAttemptID executionId, String taskName,
			int indexInSubtaskGroup, int numberOfSubtasks, Configuration jobConfiguration,
			Configuration taskConfiguration, String invokableClassName,
			List<PartitionDeploymentDescriptor> producedPartitions,
			List<PartitionConsumerDeploymentDescriptor> consumedPartitions,
			List<BlobKey> requiredJarFiles, int targetSlotNumber, Map<String,OperatorState<?>> operatorStates) {

		this(jobID, vertexID, executionId, taskName, indexInSubtaskGroup, numberOfSubtasks,
				jobConfiguration, taskConfiguration, invokableClassName, producedPartitions,
				consumedPartitions, requiredJarFiles, targetSlotNumber);
		
		setOperatorStates(operatorStates);
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

	public List<PartitionDeploymentDescriptor> getProducedPartitions() {
		return producedPartitions;
	}

	public List<PartitionConsumerDeploymentDescriptor> getConsumedPartitions() {
		return consumedPartitions;
	}

	public List<BlobKey> getRequiredJarFiles() {
		return requiredJarFiles;
	}

	@Override
	public String toString() {
		final StringBuilder pddBuilder = new StringBuilder("");
		final StringBuilder pcddBuilder = new StringBuilder("");

		for(PartitionDeploymentDescriptor pdd: producedPartitions) {
			pddBuilder.append(pdd);
		}

		for(PartitionConsumerDeploymentDescriptor pcdd: consumedPartitions) {
			pcddBuilder.append(pcdd);
		}

		final String strProducedPartitions = pddBuilder.toString();
		final String strConsumedPartitions = pcddBuilder.toString();

		return String.format("TaskDeploymentDescriptor(JobID: %s, JobVertexID: %s, " +
				"ExecutionID: %s, Task name: %s, (%d/%d), Invokable: %s, " +
				"Produced partitions: %s, Consumed partitions: %s", jobID, vertexID, executionId,
				taskName, indexInSubtaskGroup, numberOfSubtasks, invokableClassName,
				strProducedPartitions, strConsumedPartitions);
	}

	public void setOperatorStates(Map<String,OperatorState<?>> operatorStates) {
		this.operatorStates = operatorStates;
	}

	public Map<String, OperatorState<?>> getOperatorStates() {
		return operatorStates;
	}
}
