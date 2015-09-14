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

package org.apache.flink.streaming.api.state;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;

public class EagerStateStore<S, C extends Serializable> implements PartitionedStateStore<S, C> {

	private StateCheckpointer<S, C> checkpointer;
	private final StateHandleProvider<Serializable> provider;

	private Map<Serializable, S> fetchedState;

	@SuppressWarnings("unchecked")
	public EagerStateStore(StateCheckpointer<S, C> checkpointer, StateHandleProvider<C> provider) {
		this.checkpointer = checkpointer;
		this.provider = (StateHandleProvider<Serializable>) provider;

		fetchedState = new HashMap<Serializable, S>();
	}

	@Override
	public S getStateForKey(Serializable key) throws IOException {
		return fetchedState.get(key);
	}

	@Override
	public void setStateForKey(Serializable key, S state) {
		fetchedState.put(key, state);
	}

	@Override
	public void removeStateForKey(Serializable key) {
		fetchedState.remove(key);
	}

	@Override
	public Map<Serializable, S> getPartitionedState() throws IOException {
		return fetchedState;
	}

	@Override
	public StateHandle<Serializable> snapshotStates(long checkpointId, long checkpointTimestamp) {
		// we map the values in the state-map using the state-checkpointer and store it as a checkpoint
		Map<Serializable, C> checkpoints = new HashMap<Serializable, C>();
		for (Entry<Serializable, S> stateEntry : fetchedState.entrySet()) {
			checkpoints.put(stateEntry.getKey(),
					checkpointer.snapshotState(stateEntry.getValue(), checkpointId, checkpointTimestamp));
		}
		return provider.createStateHandle((Serializable) checkpoints);
	}

	@Override
	public void restoreStates(StateHandle<Serializable> snapshot, ClassLoader userCodeClassLoader)
			throws Exception {

		@SuppressWarnings("unchecked")
		Map<Serializable, C> checkpoints = (Map<Serializable, C>) snapshot.getState(userCodeClassLoader);

		// we map the values back to the state from the checkpoints
		for (Entry<Serializable, C> snapshotEntry : checkpoints.entrySet()) {
			fetchedState.put(snapshotEntry.getKey(), (S) checkpointer.restoreState(snapshotEntry.getValue()));
		}
	}

	@Override
	public boolean containsKey(Serializable key) {
		return fetchedState.containsKey(key);
	}

	@Override
	public void setCheckPointer(StateCheckpointer<S, C> checkpointer) {
		this.checkpointer = checkpointer;
	}

	@Override
	public String toString() {
		return fetchedState.toString();
	}
}
