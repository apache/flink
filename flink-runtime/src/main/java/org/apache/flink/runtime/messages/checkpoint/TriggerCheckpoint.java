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

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/**
 * This message is sent from the {@link org.apache.flink.runtime.jobmanager.JobManager} to the
 * {@link org.apache.flink.runtime.taskmanager.TaskManager} to tell a certain task to trigger its
 * checkpoint.
 */
public class TriggerCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = 2094094662279578953L;
	
	/** The timestamp associated with the checkpoint */
	private final long timestamp;

	/** Options for how to perform the checkpoint. */
	private final CheckpointOptions checkpointOptions;

	public TriggerCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			long checkpointId,
			long timestamp,
			CheckpointOptions checkpointOptions) {

		super(job, taskExecutionId, checkpointId);
		this.timestamp = timestamp;
		this.checkpointOptions = checkNotNull(checkpointOptions);
	}

	// --------------------------------------------------------------------------------------------
	
	public long getTimestamp() {
		return timestamp;
	}

	public CheckpointOptions getCheckpointOptions() {
		return checkpointOptions;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return super.hashCode() + (int) (timestamp ^ (timestamp >>> 32));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof TriggerCheckpoint) {
			TriggerCheckpoint that = (TriggerCheckpoint) o;
			return this.timestamp == that.timestamp && super.equals(o);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return String.format("Trigger Checkpoint %d@%d for (%s/%s)", 
				getCheckpointId(), getTimestamp(), getJob(), getTaskExecutionId());
	}
}
