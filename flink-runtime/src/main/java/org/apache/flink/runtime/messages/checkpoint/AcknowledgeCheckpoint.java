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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.SerializedValue;

/**
 * This message is sent from the {@link org.apache.flink.runtime.taskmanager.TaskManager} to the
 * {@link org.apache.flink.runtime.jobmanager.JobManager} to signal that the checkpoint of an
 * individual task is completed.
 * 
 * This message may carry the handle to the task's state.
 */
public class AcknowledgeCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;
	
	private final SerializedValue<StateHandle<?>> state;

	public AcknowledgeCheckpoint(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		this(job, taskExecutionId, checkpointId, null);
	}

	public AcknowledgeCheckpoint(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId,
									SerializedValue<StateHandle<?>> state) {
		super(job, taskExecutionId, checkpointId);
		this.state = state;
	}

	public SerializedValue<StateHandle<?>> getState() {
		return state;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true ;
		}
		else if (o instanceof AcknowledgeCheckpoint) {
			AcknowledgeCheckpoint that = (AcknowledgeCheckpoint) o;
			return super.equals(o) && (this.state == null ? that.state == null :
					(that.state != null && this.state.equals(that.state)));
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Checkpoint %d for (%s/%s) - state=%s",
				getCheckpointId(), getJob(), getTaskExecutionId(), state);
	}
}
