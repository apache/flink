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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.util.Map;

/**
 * This message is sent from the {@link org.apache.flink.runtime.taskmanager.TaskManager} to the
 * {@link org.apache.flink.runtime.jobmanager.JobManager} to signal that the checkpoint of an
 * individual task is completed.
 * 
 * This message may carry the handle to the task's state and the key group state.
 */
public class AcknowledgeCheckpoint extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;
	
	private final SerializedValue<StateHandle<?>> state;

	/**
	 * The state size. This is an optimization in order to not deserialize the
	 * state handle at the checkpoint coordinator when gathering stats about
	 * the checkpoints.
	 */
	private final long stateSize;

	/**
	 * State handles and their sizes for the individual key groups assigned to the check-pointed
	 * task. The key groups are indexed by their key group index.
	 */
	private final Map<Integer, Tuple2<SerializedValue<StateHandle<?>>, Long>> keyGroupStateAndSizes;

	public AcknowledgeCheckpoint(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		this(job, taskExecutionId, checkpointId, null, 0, null);
	}

	public AcknowledgeCheckpoint(
		JobID job,
		ExecutionAttemptID taskExecutionId,
		long checkpointId,
		SerializedValue<StateHandle<?>> state,
		long stateSize,
		Map<Integer, Tuple2<SerializedValue<StateHandle<?>>, Long>> keyGroupStateAndSizes) {

		super(job, taskExecutionId, checkpointId);
		this.state = state;
		this.keyGroupStateAndSizes = keyGroupStateAndSizes;
		this.stateSize = stateSize;
	}

	public SerializedValue<StateHandle<?>> getState() {
		return state;
	}

	public Map<Integer, Tuple2<SerializedValue<StateHandle<?>>, Long>> getKeyGroupStateAndSizes() {
		return keyGroupStateAndSizes;
	}

	public long getStateSize() {
		return stateSize;
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
