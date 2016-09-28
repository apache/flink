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

import static org.apache.flink.util.Preconditions.checkArgument;

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

	/** The duration (in milliseconds) that the synchronous part of the checkpoint took */
	private final long synchronousDurationMillis;

	/** The duration (in milliseconds) that the asynchronous part of the checkpoint took */
	private final long asynchronousDurationMillis;

	/** The number of bytes that were buffered during the checkpoint alignment phase */
	private final long bytesBufferedInAlignment;

	/** The duration (in nanoseconds) that the alignment phase of the task's checkpoint took */
	private final long alignmentDurationNanos;

	// ------------------------------------------------------------------------

	public AcknowledgeCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			long checkpointId) {
		this(job, taskExecutionId, checkpointId, null, null);
	}

	public AcknowledgeCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			long checkpointId,
			ChainedStateHandle<StreamStateHandle> state,
			List<KeyGroupsStateHandle> keyGroupStateAndSizes) {
		this(job, taskExecutionId, checkpointId, state, keyGroupStateAndSizes, -1L, -1L, -1L, -1L);
	}

	public AcknowledgeCheckpoint(
			JobID job,
			ExecutionAttemptID taskExecutionId,
			long checkpointId,
			ChainedStateHandle<StreamStateHandle> state,
			List<KeyGroupsStateHandle> keyGroupStateAndSizes,
			long synchronousDurationMillis,
			long asynchronousDurationMillis,
			long bytesBufferedInAlignment,
			long alignmentDurationNanos) {

		super(job, taskExecutionId, checkpointId);

		// these may be null in cases where the operator has no state
		this.stateHandle = state;
		this.keyGroupsStateHandle = keyGroupStateAndSizes;

		// these may be "-1", in case the values are unknown or not set
		checkArgument(synchronousDurationMillis >= -1);
		checkArgument(asynchronousDurationMillis >= -1);
		checkArgument(bytesBufferedInAlignment >= -1);
		checkArgument(alignmentDurationNanos >= -1);

		this.synchronousDurationMillis = synchronousDurationMillis;
		this.asynchronousDurationMillis = asynchronousDurationMillis;
		this.bytesBufferedInAlignment = bytesBufferedInAlignment;
		this.alignmentDurationNanos = alignmentDurationNanos;
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	public ChainedStateHandle<StreamStateHandle> getStateHandle() {
		return stateHandle;
	}

	public List<KeyGroupsStateHandle> getKeyGroupsStateHandle() {
		return keyGroupsStateHandle;
	}

	public long getSynchronousDurationMillis() {
		return synchronousDurationMillis;
	}

	public long getAsynchronousDurationMillis() {
		return asynchronousDurationMillis;
	}

	public long getBytesBufferedInAlignment() {
		return bytesBufferedInAlignment;
	}

	public long getAlignmentDurationNanos() {
		return alignmentDurationNanos;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true ;
		}
		else if (o instanceof AcknowledgeCheckpoint) {
			AcknowledgeCheckpoint that = (AcknowledgeCheckpoint) o;
			return super.equals(o) &&
					(this.stateHandle == null ? that.stateHandle == null : 
							(that.stateHandle != null && this.stateHandle.equals(that.stateHandle))) &&
					(this.keyGroupsStateHandle == null ? that.keyGroupsStateHandle == null : 
							(that.keyGroupsStateHandle != null && this.keyGroupsStateHandle.equals(that.keyGroupsStateHandle)));
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
