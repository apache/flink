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

public class EagerStateStore<S, C extends Serializable> implements PartitionedStateStore<S, C> {

	protected StateCheckpointer<S, C> checkpointer;
	protected StateHandleProvider<C> provider;

	Map<Serializable, S> fetchedState;

	public EagerStateStore(StateCheckpointer<S, C> checkpointer, StateHandleProvider<C> provider) {
		this.checkpointer = checkpointer;
		this.provider = provider;

		fetchedState = new HashMap<Serializable, S>();
	}

	@Override
	public S getStateForKey(Serializable key) throws Exception {
		return fetchedState.get(key);
	}

	@Override
	public void setStateForKey(Serializable key, S state) {
		fetchedState.put(key, state);
	}

	@Override
	public Map<Serializable, S> getPartitionedState() throws Exception {
		return fetchedState;
	}

	@Override
	public Map<Serializable, StateHandle<C>> snapshotStates(long checkpointId,
			long checkpointTimestamp) {

		Map<Serializable, StateHandle<C>> handles = new HashMap<Serializable, StateHandle<C>>();

		for (Entry<Serializable, S> stateEntry : fetchedState.entrySet()) {
			handles.put(stateEntry.getKey(), provider.createStateHandle(checkpointer.snapshotState(
					stateEntry.getValue(), checkpointId, checkpointTimestamp)));
		}
		return handles;
	}

	@Override
	public void restoreStates(Map<Serializable, StateHandle<C>> snapshots) throws Exception {
		for (Entry<Serializable, StateHandle<C>> snapshotEntry : snapshots.entrySet()) {
			fetchedState.put(snapshotEntry.getKey(),
					checkpointer.restoreState(snapshotEntry.getValue().getState()));
		}
	}

	@Override
	public boolean containsKey(Serializable key) {
		return fetchedState.containsKey(key);
	}

}
