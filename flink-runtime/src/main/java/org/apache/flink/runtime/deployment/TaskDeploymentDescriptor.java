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

import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.util.Collection;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -3233562176034358530L;

	/** Serialized job information. */
	private final SerializedValue<JobInformation> serializedJobInformation;

	/** Serialized task information. */
	private final SerializedValue<TaskInformation> serializedTaskInformation;

	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The allocation ID of the slot in which the task shall be run. */
	private final AllocationID allocationId;

	/** The task's index in the subtask group. */
	private final int subtaskIndex;

	/** Attempt number the task. */
	private final int attemptNumber;

	/** The list of produced intermediate result partition deployment descriptors. */
	private final Collection<ResultPartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private final Collection<InputGateDeploymentDescriptor> inputGates;

	/** Slot number to run the sub task in on the target machine. */
	private final int targetSlotNumber;

	/** State handles for the sub task. */
	private final TaskStateSnapshot taskStateHandles;

	public TaskDeploymentDescriptor(
			SerializedValue<JobInformation> serializedJobInformation,
			SerializedValue<TaskInformation> serializedTaskInformation,
			ExecutionAttemptID executionAttemptId,
			AllocationID allocationId,
			int subtaskIndex,
			int attemptNumber,
			int targetSlotNumber,
			TaskStateSnapshot taskStateHandles,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

		this.serializedJobInformation = Preconditions.checkNotNull(serializedJobInformation);
		this.serializedTaskInformation = Preconditions.checkNotNull(serializedTaskInformation);
		this.executionId = Preconditions.checkNotNull(executionAttemptId);
		this.allocationId = Preconditions.checkNotNull(allocationId);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		this.subtaskIndex = subtaskIndex;

		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		this.attemptNumber = attemptNumber;

		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");
		this.targetSlotNumber = targetSlotNumber;

		this.taskStateHandles = taskStateHandles;

		this.producedPartitions = Preconditions.checkNotNull(resultPartitionDeploymentDescriptors);
		this.inputGates = Preconditions.checkNotNull(inputGateDeploymentDescriptors);
	}

	/**
	 * Return the sub task's serialized job information.
	 *
	 * @return serialized job information
	 */
	public SerializedValue<JobInformation> getSerializedJobInformation() {
		return serializedJobInformation;
	}

	/**
	 * Return the sub task's serialized task information.
	 *
	 * @return serialized task information
	 */
	public SerializedValue<TaskInformation> getSerializedTaskInformation() {
		return serializedTaskInformation;
	}

	public ExecutionAttemptID getExecutionAttemptId() {
		return executionId;
	}

	/**
	 * Returns the task's index in the subtask group.
	 *
	 * @return the task's index in the subtask group
	 */
	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	/**
	 * Returns the attempt number of the subtask.
	 */
	public int getAttemptNumber() {
		return attemptNumber;
	}

	/**
	 * Gets the number of the slot into which the task is to be deployed.
	 *
	 * @return The number of the target slot.
	 */
	public int getTargetSlotNumber() {
		return targetSlotNumber;
	}

	public Collection<ResultPartitionDeploymentDescriptor> getProducedPartitions() {
		return producedPartitions;
	}

	public Collection<InputGateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	public TaskStateSnapshot getTaskStateHandles() {
		return taskStateHandles;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	@Override
	public String toString() {
		return String.format("TaskDeploymentDescriptor [execution id: %s, attempt: %d, " +
				"produced partitions: %s, input gates: %s]",
			executionId,
			attemptNumber,
			collectionToString(producedPartitions),
			collectionToString(inputGates));
	}

	private static String collectionToString(Iterable<?> collection) {
		final StringBuilder strBuilder = new StringBuilder();

		strBuilder.append("[");

		for (Object elem : collection) {
			strBuilder.append(elem);
		}

		strBuilder.append("]");

		return strBuilder.toString();
	}
}
