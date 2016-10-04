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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Container for the chained state of one parallel subtask of an operator/task. This is part of the
 * {@link TaskState}.
 */
public class SubtaskState implements StateObject {

	private static final long serialVersionUID = -2394696997971923995L;

	/**
	 * The state of the parallel operator
	 */
	private final ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorState;
	private final ChainedStateHandle<OperatorStateHandle> operatorStateFromBackend;
	private final ChainedStateHandle<OperatorStateHandle> operatorStateFromStream;
	private final KeyGroupsStateHandle keyedStateFromBackend;
	private final KeyGroupsStateHandle keyedStateHandleFromStream;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	/**
	 * The duration of the checkpoint (ack timestamp - trigger timestamp).
	 */
	private long duration;

	public SubtaskState(
			ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorState,
			ChainedStateHandle<OperatorStateHandle> operatorStateFromBackend,
			ChainedStateHandle<OperatorStateHandle> operatorStateFromStream,
			KeyGroupsStateHandle keyedStateFromBackend,
			KeyGroupsStateHandle keyedStateHandleFromStream) {
		this(nonPartitionableOperatorState,
				operatorStateFromBackend,
				operatorStateFromStream,
				keyedStateFromBackend,
				keyedStateHandleFromStream,
				0L);
	}

	public SubtaskState(
			ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorState,
			ChainedStateHandle<OperatorStateHandle> operatorStateFromBackend,
			ChainedStateHandle<OperatorStateHandle> operatorStateFromStream,
			KeyGroupsStateHandle keyedStateFromBackend,
			KeyGroupsStateHandle keyedStateHandleFromStream,
			long duration) {

		this.nonPartitionableOperatorState = checkNotNull(nonPartitionableOperatorState, "State");
		this.operatorStateFromBackend = operatorStateFromBackend;
		this.operatorStateFromStream = operatorStateFromStream;
		this.keyedStateFromBackend = keyedStateFromBackend;
		this.keyedStateHandleFromStream = keyedStateHandleFromStream;
		this.duration = duration;
		try {
			long calculateStateSize = getSizeNullSafe(nonPartitionableOperatorState);
			calculateStateSize += getSizeNullSafe(operatorStateFromBackend);
			calculateStateSize += getSizeNullSafe(operatorStateFromStream);
			calculateStateSize += getSizeNullSafe(keyedStateFromBackend);
			calculateStateSize += getSizeNullSafe(keyedStateHandleFromStream);
			stateSize = calculateStateSize;
		} catch (Exception e) {
			throw new RuntimeException("Failed to get state size.", e);
		}
	}

	private static final long getSizeNullSafe(StateObject stateObject) throws Exception {
		return stateObject != null ? stateObject.getStateSize() : 0L;
	}

	// --------------------------------------------------------------------------------------------

	public ChainedStateHandle<StreamStateHandle> getNonPartitionableOperatorState() {
		return nonPartitionableOperatorState;
	}

	public ChainedStateHandle<OperatorStateHandle> getOperatorStateFromBackend() {
		return operatorStateFromBackend;
	}

	public ChainedStateHandle<OperatorStateHandle> getOperatorStateFromStream() {
		return operatorStateFromStream;
	}

	public KeyGroupsStateHandle getKeyedStateFromBackend() {
		return keyedStateFromBackend;
	}

	public KeyGroupsStateHandle getKeyedStateFromStream() {
		return keyedStateHandleFromStream;
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	public long getDuration() {
		return duration;
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(
				Arrays.asList(
						nonPartitionableOperatorState,
						operatorStateFromBackend,
						operatorStateFromStream,
						keyedStateFromBackend,
						keyedStateHandleFromStream));
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	// --------------------------------------------------------------------------------------------


	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SubtaskState that = (SubtaskState) o;

		if (stateSize != that.stateSize) {
			return false;
		}
		if (duration != that.duration) {
			return false;
		}
		if (nonPartitionableOperatorState != null ?
				!nonPartitionableOperatorState.equals(that.nonPartitionableOperatorState)
				: that.nonPartitionableOperatorState != null) {
			return false;
		}
		if (operatorStateFromBackend != null ?
				!operatorStateFromBackend.equals(that.operatorStateFromBackend)
				: that.operatorStateFromBackend != null) {
			return false;
		}
		if (operatorStateFromStream != null ?
				!operatorStateFromStream.equals(that.operatorStateFromStream)
				: that.operatorStateFromStream != null) {
			return false;
		}
		if (keyedStateFromBackend != null ?
				!keyedStateFromBackend.equals(that.keyedStateFromBackend)
				: that.keyedStateFromBackend != null) {
			return false;
		}
		return keyedStateHandleFromStream != null ?
				keyedStateHandleFromStream.equals(that.keyedStateHandleFromStream)
				: that.keyedStateHandleFromStream == null;

	}

	public boolean hasState() {
		return (null != nonPartitionableOperatorState && !nonPartitionableOperatorState.isEmpty())
				|| (null != operatorStateFromBackend && !operatorStateFromBackend.isEmpty())
				|| null != keyedStateFromBackend
				|| null != keyedStateHandleFromStream;
	}

	@Override
	public int hashCode() {
		int result = nonPartitionableOperatorState != null ? nonPartitionableOperatorState.hashCode() : 0;
		result = 31 * result + (operatorStateFromBackend != null ? operatorStateFromBackend.hashCode() : 0);
		result = 31 * result + (operatorStateFromStream != null ? operatorStateFromStream.hashCode() : 0);
		result = 31 * result + (keyedStateFromBackend != null ? keyedStateFromBackend.hashCode() : 0);
		result = 31 * result + (keyedStateHandleFromStream != null ? keyedStateHandleFromStream.hashCode() : 0);
		result = 31 * result + (int) (stateSize ^ (stateSize >>> 32));
		result = 31 * result + (int) (duration ^ (duration >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "SubtaskState{" +
				"chainedStateHandle=" + nonPartitionableOperatorState +
				", operatorStateFromBackend=" + operatorStateFromBackend +
				", operatorStateFromStream=" + operatorStateFromStream +
				", keyedStateFromBackend=" + keyedStateFromBackend +
				", keyedStateHandleFromStream=" + keyedStateHandleFromStream +
				", stateSize=" + stateSize +
				", duration=" + duration +
				'}';
	}
}
