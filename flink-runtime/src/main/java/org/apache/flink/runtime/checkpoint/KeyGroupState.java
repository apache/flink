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

/**
 * Simple container class which contains the serialized state handle for a key group.
 *
 * The key group state handle is kept in serialized form because it can contain user code classes
 * which might not be available on the JobManager.
 */
public class KeyGroupState implements Serializable {
	private static final long serialVersionUID = -5926696455438467634L;

	private static final Logger LOG = LoggerFactory.getLogger(KeyGroupState.class);

	private final SerializedValue<StateHandle<?>> keyGroupState;

	private final long stateSize;

	private final long duration;

	public KeyGroupState(SerializedValue<StateHandle<?>> keyGroupState, long stateSize, long duration) {
		this.keyGroupState = keyGroupState;

		this.stateSize = stateSize;

		this.duration = duration;
	}

	public SerializedValue<StateHandle<?>> getKeyGroupState() {
		return keyGroupState;
	}

	public long getDuration() {
		return duration;
	}

	public long getStateSize() {
		return stateSize;
	}

	public void discard(ClassLoader classLoader) {
		try {
			keyGroupState.deserializeValue(classLoader).discardState();
		} catch (Exception e) {
			LOG.warn("Failed to discard checkpoint state: " + this, e);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof KeyGroupState) {
			KeyGroupState other = (KeyGroupState) obj;

			return keyGroupState.equals(other.keyGroupState) && stateSize == other.stateSize &&
				duration == other.duration;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.stateSize ^ this.stateSize >>> 32) +
			31 * ((int) (this.duration ^ this.duration >>> 32) +
				31 * keyGroupState.hashCode());
	}
}
