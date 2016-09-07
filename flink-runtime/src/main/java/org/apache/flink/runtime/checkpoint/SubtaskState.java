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
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Container for the chained state of one parallel subtask of an operator/task. This is part of the
 * {@link TaskState}.
 */
public class SubtaskState implements StateObject {

	private static final long serialVersionUID = -2394696997971923995L;

	private static final Logger LOG = LoggerFactory.getLogger(SubtaskState.class);

	/** The state of the parallel operator */
	private final ChainedStateHandle<StreamStateHandle> chainedStateHandle;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	/** The duration of the checkpoint (ack timestamp - trigger timestamp). */
	private final long duration;
	
	public SubtaskState(
			ChainedStateHandle<StreamStateHandle> chainedStateHandle,
			long duration) {

		this.chainedStateHandle = checkNotNull(chainedStateHandle, "State");
		this.duration = duration;
		try {
			stateSize = chainedStateHandle.getStateSize();
		} catch (Exception e) {
			throw new RuntimeException("Failed to get state size.", e);
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public ChainedStateHandle<StreamStateHandle> getChainedStateHandle() {
		return chainedStateHandle;
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
		chainedStateHandle.discardState();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		else if (o instanceof SubtaskState) {
			SubtaskState that = (SubtaskState) o;
			return this.chainedStateHandle.equals(that.chainedStateHandle) && stateSize == that.stateSize &&
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
				31 * chainedStateHandle.hashCode());
	}

	@Override
	public String toString() {
		return String.format("SubtaskState(Size: %d, Duration: %d, State: %s)", stateSize, duration, chainedStateHandle);
	}

	@Override
	public void close() throws IOException {
		chainedStateHandle.close();
	}


}
