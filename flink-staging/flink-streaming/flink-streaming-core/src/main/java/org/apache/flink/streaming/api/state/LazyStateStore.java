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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.runtime.state.PartitionedStateStore;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;

/**
 * Implementation of the {@link PartitionedStateStore} interface for lazy
 * retrieval and snapshotting of the partitioned operator states. Lazy state
 * access considerably speeds up recovery and makes resource access smoother by
 * avoiding request congestion in the persistent storage layer.
 * 
 * <p>
 * The logic implemented here can also be used later to push unused state to the
 * persistent layer and also avoids re-snapshotting the unmodified states.
 * </p>
 * 
 * @param <S>
 *            Type of the operator states.
 * @param <C>
 *            Type of the state checkpoints.
 */
public class LazyStateStore<S, C extends Serializable> implements PartitionedStateStore<S, C> {

	protected StateCheckpointer<S, C> checkpointer;
	protected final StateHandleProvider<C> provider;

	private final Map<Serializable, StateHandle<C>> unfetchedState;
	private final Map<Serializable, S> fetchedState;

	public LazyStateStore(StateCheckpointer<S, C> checkpointer, StateHandleProvider<C> provider) {
		this.checkpointer = checkpointer;
		this.provider = provider;

		unfetchedState = new HashMap<Serializable, StateHandle<C>>();
		fetchedState = new HashMap<Serializable, S>();
	}

	@Override
	public S getStateForKey(Serializable key) throws Exception {
		S state = fetchedState.get(key);
		if (state != null) {
			return state;
		} else {
			StateHandle<C> handle = unfetchedState.get(key);
			if (handle != null) {
				state = checkpointer.restoreState(handle.getState());
				fetchedState.put(key, state);
				unfetchedState.remove(key);
				return state;
			} else {
				return null;
			}
		}
	}

	@Override
	public void setStateForKey(Serializable key, S state) {
		fetchedState.put(key, state);
		unfetchedState.remove(key);
	}

	@Override
	public Map<Serializable, S> getPartitionedState() throws Exception {
		for (Entry<Serializable, StateHandle<C>> handleEntry : unfetchedState.entrySet()) {
			fetchedState.put(handleEntry.getKey(),
					checkpointer.restoreState(handleEntry.getValue().getState()));
		}
		unfetchedState.clear();
		return fetchedState;
	}

	@Override
	public Map<Serializable, StateHandle<C>> snapshotStates(long checkpointId,
			long checkpointTimestamp) {
		for (Entry<Serializable, S> stateEntry : fetchedState.entrySet()) {
			unfetchedState.put(stateEntry.getKey(), provider.createStateHandle(checkpointer
					.snapshotState(stateEntry.getValue(), checkpointId, checkpointTimestamp)));
		}
		return unfetchedState;
	}

	@Override
	public void restoreStates(Map<Serializable, StateHandle<C>> snapshots) {
		unfetchedState.putAll(snapshots);
	}

	@Override
	public boolean containsKey(Serializable key) {
		return fetchedState.containsKey(key) || unfetchedState.containsKey(key);
	}

	@Override
	public void setCheckPointer(StateCheckpointer<S, C> checkpointer) {
		this.checkpointer = checkpointer;
	}

}
