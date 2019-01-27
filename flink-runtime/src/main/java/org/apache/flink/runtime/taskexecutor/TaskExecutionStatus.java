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

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TaskExecutionStatus implements Serializable {
	private static final long serialVersionUID = 1L;

	private final ExecutionState executionState;
	private final int attemptNumber;
	private final long createTimestamp;
	private final JobVertexID jobVertexID;
	private final ExecutionAttemptID executionAttemptID;
	private final int indexOfSubtask;
	private final ResultPartitionID[] resultPartitionIDs;
	private final boolean[] resultPartitionsConsumable;
	private final Map<OperatorID, List<InputSplit>> assignedInputSplits;
	private final SlotOffer boundSlot;

	public TaskExecutionStatus(
			ExecutionState executionState,
			int attemptNumber,
			long createTimestamp,
			JobVertexID jobVertexID,
			ExecutionAttemptID executionAttemptID,
			int indexOfSubtask,
			ResultPartitionID[] resultPartitionIDs,
			boolean[] resultPartitionsConsumable,
			Map<OperatorID, List<InputSplit>> assignedInputSplits,
			SlotOffer boundSlot) {
		this.executionState = executionState;
		this.attemptNumber = attemptNumber;
		this.createTimestamp = createTimestamp;
		this.jobVertexID = jobVertexID;
		this.executionAttemptID = executionAttemptID;
		this.indexOfSubtask = indexOfSubtask;
		this.resultPartitionIDs = resultPartitionIDs;
		this.resultPartitionsConsumable = resultPartitionsConsumable;
		this.assignedInputSplits = assignedInputSplits;
		this.boundSlot = boundSlot;
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

	public int getIndexOfSubtask() {
		return indexOfSubtask;
	}

	public SlotOffer getBoundSlot() {
		return boundSlot;
	}

	public ResultPartitionID[] getResultPartitionIDs() {
		return resultPartitionIDs;
	}

	public boolean[] getResultPartitionsConsumable() {
		return resultPartitionsConsumable;
	}

	public Map<OperatorID, List<InputSplit>> getAssignedInputSplits() {
		return assignedInputSplits;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskExecutionStatus taskExecutionStatus = (TaskExecutionStatus) o;
		return executionAttemptID.equals(taskExecutionStatus.getExecutionAttemptID());
	}
}
