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
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.List;

/**
 * This message is sent from the {@link org.apache.flink.runtime.taskmanager.TaskManager} to the
 * {@link org.apache.flink.runtime.jobmanager.JobManager} to signal that the checkpoint of an
 * individual task is completed.
 * 
 * <p>This message may carry the handle to the task's chained operator state and the key group
 * state.
 */
public class AcknowledgeCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;
	
	private final ChainedStateHandle<StreamStateHandle> stateHandle;

	private final List<KeyGroupsStateHandle> keyGroupsStateHandle;

	public AcknowledgeCheckpoint(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		this(job, taskExecutionId, checkpointId, null, null);
	}

	public AcknowledgeCheckpoint(
		JobID job,
		ExecutionAttemptID taskExecutionId,
		long checkpointId,
		ChainedStateHandle<StreamStateHandle> state,
		List<KeyGroupsStateHandle> keyGroupStateAndSizes) {

		super(job, taskExecutionId, checkpointId);
		this.stateHandle = state;
		this.keyGroupsStateHandle = keyGroupStateAndSizes;
	}

	public ChainedStateHandle<StreamStateHandle> getStateHandle() {
		return stateHandle;
	}

	public List<KeyGroupsStateHandle> getKeyGroupsStateHandle() {
		return keyGroupsStateHandle;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true ;
		}
		else if (o instanceof AcknowledgeCheckpoint) {
			AcknowledgeCheckpoint that = (AcknowledgeCheckpoint) o;
			return super.equals(o) &&
					(this.stateHandle == null ? that.stateHandle == null : (that.stateHandle != null && this.stateHandle.equals(that.stateHandle))) &&
					(this.keyGroupsStateHandle == null ? that.keyGroupsStateHandle == null : (that.keyGroupsStateHandle != null && this.keyGroupsStateHandle.equals(that.keyGroupsStateHandle)));

		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Checkpoint %d for (%s/%s) - state=%s keyGroupState=%s",
				getCheckpointId(), getJob(), getTaskExecutionId(), stateHandle, keyGroupsStateHandle);
	}
}
