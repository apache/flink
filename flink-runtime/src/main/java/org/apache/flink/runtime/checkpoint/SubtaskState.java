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

import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple bean to describe the state belonging to a parallel operator. It is part of the
 * {@link TaskState}.
 * 
 * The state itself is kept in serialized form, since the checkpoint coordinator itself
 * is never looking at it anyways and only sends it back out in case of a recovery.
 * Furthermore, the state may involve user-defined classes that are not accessible without
 * the respective classloader.
 */
public class SubtaskState implements Serializable {

	private static final long serialVersionUID = -2394696997971923995L;

	private static final Logger LOG = LoggerFactory.getLogger(SubtaskState.class);

	/** The state of the parallel operator */
	private final SerializedValue<StateHandle<?>> state;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	/** The duration of the acknowledged (ack timestamp - trigger timestamp). */
	private final long duration;
	
	public SubtaskState(
			SerializedValue<StateHandle<?>> state,
			long stateSize,
			long duration) {

		this.state = checkNotNull(state, "State");
		// Sanity check and don't fail checkpoint because of this.
		this.stateSize = stateSize >= 0 ? stateSize : 0;

		this.duration = duration;
	}

	// --------------------------------------------------------------------------------------------
	
	public SerializedValue<StateHandle<?>> getState() {
		return state;
	}

	public long getStateSize() {
		return stateSize;
	}

	public long getDuration() {
		return duration;
	}

	public void discard(ClassLoader userClassLoader) {
		try {
			state.deserializeValue(userClassLoader).discardState();
		} catch (Exception e) {
			LOG.warn("Failed to discard checkpoint state: " + this, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof SubtaskState) {
			SubtaskState that = (SubtaskState) o;
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
		return String.format("StateForTask(Size: %d, Duration: %d, State: %s)", stateSize, duration, state);
	}
}
