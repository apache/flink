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

package org.apache.flink.migration.v0.runtime;

import org.apache.flink.migration.MigrationSerializedValue;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The state belonging to a subtask (or a parallel operator) in SavepointV0.
 * Each subtask contains a collection of operators whose states are typed
 * {@link StreamTaskStateV0}.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class SubtaskStateV0 implements Serializable {

	private static final long serialVersionUID = -2394696997971923995L;

	/** 
	 * The state of the parallel operator, which is {@link StreamTaskStateListV0}
	 * in the java serialization form.
	 */
	private final MigrationSerializedValue<StateHandleV0<?>> state;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	/** The duration of the acknowledged (ack timestamp - trigger timestamp). */
	private final long duration;
	
	public SubtaskStateV0(
			MigrationSerializedValue<StateHandleV0<?>> state,
			long stateSize,
			long duration) {

		this.state = checkNotNull(state, "State");
		// Sanity check and don't fail checkpoint because of this.
		this.stateSize = stateSize >= 0 ? stateSize : 0;

		this.duration = duration;
	}

	// --------------------------------------------------------------------------------------------
	
	public MigrationSerializedValue<StateHandleV0<?>> getState() {
		return state;
	}

	public long getStateSize() {
		return stateSize;
	}

	public long getDuration() {
		return duration;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof SubtaskStateV0) {
			SubtaskStateV0 that = (SubtaskStateV0) o;
			return this.state.equals(that.state) && stateSize == that.stateSize &&
				duration == that.duration;
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.stateSize ^ this.stateSize >>> 32) +
			31 * ((int) (this.duration ^ this.duration >>> 32) +
				31 * state.hashCode());
	}

	@Override
	public String toString() {
		return String.format("SubtaskState(Size: %d, Duration: %d, State: %s)", stateSize, duration, state);
	}
}
