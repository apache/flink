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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 */
public final class TaskDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -3233562176034358530L;

	/** Serialized job information */
	private SerializedValue<JobInformation> serializedJobInformation;

	/** Serialized task information */
	private SerializedValue<TaskInformation> serializedTaskInformation;

	/**
	 * The ID referencing the job this task belongs to.
	 *
	 * <p>NOTE: this is redundant to the information stored in {@link #serializedJobInformation} but
	 * needed in order to restore offloaded data.</p>
	 */
	private final JobID jobId;

	/**
	 * The ID referencing the job vertex this task belongs to.
	 *
	 * <p>NOTE: this is redundant to the information stored in {@link #serializedTaskInformation} but
	 * needed in order to restore offloaded data.</p>
	 */
	private final JobVertexID jobVertexId;

	/** The ID referencing the attempt to execute the task. */
	private final ExecutionAttemptID executionId;

	/** The allocation ID of the slot in which the task shall be run */
	private final AllocationID allocationId;

	/** The task's index in the subtask group. */
	private final int subtaskIndex;

	/** Attempt number the task */
	private final int attemptNumber;

	/** The list of produced intermediate result partition deployment descriptors. */
	private final Collection<ResultPartitionDeploymentDescriptor> producedPartitions;

	/** The list of consumed intermediate result partitions. */
	private final Collection<InputGateDeploymentDescriptor> inputGates;

	/** Slot number to run the sub task in on the target machine */
	private final int targetSlotNumber;

	/** State handles for the sub task */
	private final TaskStateHandles taskStateHandles;

	public TaskDeploymentDescriptor(
			JobID jobId,
			JobVertexID jobVertexId,
			SerializedValue<JobInformation> serializedJobInformation,
			SerializedValue<TaskInformation> serializedTaskInformation,
			ExecutionAttemptID executionAttemptId,
			AllocationID allocationId,
			int subtaskIndex,
			int attemptNumber,
			int targetSlotNumber,
			TaskStateHandles taskStateHandles,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

		this.jobId = jobId;
		this.jobVertexId = jobVertexId;
		this.serializedJobInformation = serializedJobInformation;
		this.serializedTaskInformation = serializedTaskInformation;
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

	/**
	 * Returns the task's job ID.
	 *
	 * @return the job ID this task belongs to
	 */
	public JobID getJobId() {
		return jobId;
	}

	/**
	 * Returns the task's job vertex ID.
	 *
	 * @return the job vertex ID this task belongs to
	 */
	public JobVertexID getJobVertexId() {
		return jobVertexId;
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
	 * Returns the attempt number of the subtask
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

	public TaskStateHandles getTaskStateHandles() {
		return taskStateHandles;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	/**
	 * Loads externalized data from the BLOB store back to the object.
	 *
	 * @param blobLibCache
	 * 		the blob store to use (may be <tt>null</tt> if {@link #serializedJobInformation} and {@link
	 * 		#serializedTaskInformation} are non-<tt>null</tt>)
	 *
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * 		Class of a serialized object cannot be found.
	 */
	public void loadBigData(final LibraryCacheManager blobLibCache)
			throws IOException, ClassNotFoundException {

		// re-integrate offloaded job info and delete blob
		// here, if this fails, we need to throw the exception as there is no backup path anymore
		if (serializedJobInformation == null) {
			final String fileKey = ExecutionGraph.getOffloadedJobInfoFileName();
			final File dataFile = blobLibCache.getFile(jobId, fileKey);
			try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dataFile))) {
				serializedJobInformation = (SerializedValue<JobInformation>) ois.readObject();
				// NOTE: Do not delete the job info BLOB since it may be needed again during recovery.
				//       (it is deleted automatically on the BLOB server and cache when the job
				//       enters a terminal state)
			}
		}

		// re-integrate offloaded task info and delete blob
		if (serializedTaskInformation == null) {
			final String fileKey = ExecutionJobVertex.getOffloadedTaskInfoFileName(jobVertexId);
			final File dataFile = blobLibCache.getFile(jobId, fileKey);
			try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(dataFile))) {
				serializedTaskInformation = (SerializedValue<TaskInformation>) ois.readObject();
				// NOTE: Do not delete the task info BLOB since it may be needed again during recovery.
				//       (it is deleted automatically on the BLOB server and cache when the job
				//       enters a terminal state)
			}
		}

		// make sure that the serialized job and task information fields are filled
		Preconditions.checkNotNull(serializedJobInformation);
		Preconditions.checkNotNull(serializedTaskInformation);
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
