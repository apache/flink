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

package org.apache.flink.runtime.jobmaster.message;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;

/**
 * Answer to a [[RequestPartitionState]] with the state of the respective partition.
 */
public class PartitionState implements Serializable {
	private static final long serialVersionUID = -3346485077312144516L;

	private final ExecutionAttemptID taskExecutionId;

	private final IntermediateDataSetID taskResultId;

	private final IntermediateResultPartitionID partitionId;

	private final ExecutionState state;

	public PartitionState(
		final ExecutionAttemptID taskExecutionId,
		final IntermediateDataSetID taskResultId,
		final IntermediateResultPartitionID partitionId,
		final ExecutionState state)
	{
		this.taskExecutionId = taskExecutionId;
		this.taskResultId = taskResultId;
		this.partitionId = partitionId;
		this.state = state;
	}

	public ExecutionAttemptID getTaskExecutionId() {
		return taskExecutionId;
	}

	public IntermediateDataSetID getTaskResultId() {
		return taskResultId;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ExecutionState getState() {
		return state;
	}
}
