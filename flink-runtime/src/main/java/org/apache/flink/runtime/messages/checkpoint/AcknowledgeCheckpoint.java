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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This message is sent from the {@link org.apache.flink.runtime.taskmanager.TaskManager} to the
 * {@link org.apache.flink.runtime.jobmanager.JobManager} to signal that the checkpoint of an
 * individual task is completed.
 * <p>
 * <p>This message may carry the handle to the task's chained operator state and the key group
 * state.
 */
public class AcknowledgeCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;


	private final SubtaskState subtaskState;

	private final CheckpointMetaData checkpointMetaData;

	// ------------------------------------------------------------------------

	public AcknowledgeCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			CheckpointMetaData checkpointMetaData) {
		this(job, taskExecutionId, checkpointMetaData, null);
	}

	public AcknowledgeCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			CheckpointMetaData checkpointMetaData,
			SubtaskState subtaskState) {

		super(job, taskExecutionId, checkpointMetaData.getCheckpointId());

		this.subtaskState = subtaskState;
		this.checkpointMetaData = checkpointMetaData;
		// these may be "-1", in case the values are unknown or not set
		checkArgument(checkpointMetaData.getSyncDurationMillis() >= -1);
		checkArgument(checkpointMetaData.getAsyncDurationMillis() >= -1);
		checkArgument(checkpointMetaData.getBytesBufferedInAlignment() >= -1);
		checkArgument(checkpointMetaData.getAlignmentDurationNanos() >= -1);
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	public SubtaskState getSubtaskState() {
		return subtaskState;
	}

	public long getSynchronousDurationMillis() {
		return checkpointMetaData.getSyncDurationMillis();
	}

	public long getAsynchronousDurationMillis() {
		return checkpointMetaData.getAsyncDurationMillis();
	}

	public long getBytesBufferedInAlignment() {
		return checkpointMetaData.getBytesBufferedInAlignment();
	}

	public long getAlignmentDurationNanos() {
		return checkpointMetaData.getAlignmentDurationNanos();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AcknowledgeCheckpoint)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		AcknowledgeCheckpoint that = (AcknowledgeCheckpoint) o;
		return subtaskState != null ?
				subtaskState.equals(that.subtaskState) : that.subtaskState == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (subtaskState != null ? subtaskState.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Checkpoint %d for (%s/%s) - state=%s",
				getCheckpointId(), getJob(), getTaskExecutionId(), subtaskState);
	}
}
