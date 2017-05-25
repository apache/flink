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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Describe the task and slot basic information to job manager reported by task manager.
 */
public class TaskSlotStatus implements Serializable {

	private static final long serialVersionUID = -7067814231108250951L;

	/** The execution attempt id of the parallel subtask */
	private final ExecutionAttemptID executionAttemptID;

	/** Represent the state of the parallel subtask */
	private final ExecutionState executionState;

	/** The execution attempt number of the parallel subtask */
	private final int attemptNumber;

	/** The execution creation timestamp of the parallel subtask */
	private final long createTimestamp;

	/** The vertex in the JobGraph whose code the task executes */
	private final JobVertexID jobVertexID;

	/** The number of the parallel subtask */
	private final int subtaskIndex;

	/** The array of result partition id of the parallel subtask */
	private final ResultPartitionID[] resultPartitionIDs;

	/** Represents the slot in which the execution of the parallel subtask is deployed */
	private final SlotOffer slotOffer;

	public TaskSlotStatus(
			ExecutionState executionState,
			int attemptNumber,
			long createTimestamp,
			JobVertexID jobVertexID,
			ExecutionAttemptID executionAttemptID,
			int subtaskIndex,
			ResultPartitionID[] resultPartitionIDs,
			SlotOffer slotOffer) {
		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		Preconditions.checkArgument(0 <= createTimestamp, "The create timestamp must be positive.");

		this.executionState = Preconditions.checkNotNull(executionState);
		this.attemptNumber = Preconditions.checkNotNull(attemptNumber);
		this.createTimestamp = createTimestamp;
		this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
		this.executionAttemptID = Preconditions.checkNotNull(executionAttemptID);
		this.subtaskIndex = subtaskIndex;
		this.resultPartitionIDs = Preconditions.checkNotNull(resultPartitionIDs);
		this.slotOffer = Preconditions.checkNotNull(slotOffer);
	}

	public ExecutionState getExecutionState() {
		return executionState;
	}

	public int getAttemptNumber() {
		return attemptNumber;
	}

	public long getCreateTimestamp() {
		return createTimestamp;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	public ExecutionAttemptID getExecutionAttemptID() {
		return executionAttemptID;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public SlotOffer getSlotOffer() {
		return slotOffer;
	}

	public ResultPartitionID[] getResultPartitionIDs() {
		return resultPartitionIDs;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskSlotStatus taskSlotStatus = (TaskSlotStatus) o;
		return executionAttemptID.equals(taskSlotStatus.getExecutionAttemptID());
	}

}
