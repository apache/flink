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
import java.util.Map;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.shaded.com.google.common.collect.ImmutableMap;

/**
 * Implementation of the {@link OperatorState} interface for non-partitioned
 * user states. It provides methods for checkpointing and restoring operator
 * states upon failure using the provided {@link StateCheckpointer} and
 * {@link StateHandleProvider}.
 * 
 * @param <S>
 *            Type of the underlying {@link OperatorState}.
 * @param <C>
 *            Type of the state snapshot.
 */
public class StreamOperatorState<S, C extends Serializable> implements OperatorState<S> {

	protected static final Serializable DEFAULTKEY = -1;

	private S state;
	private StateCheckpointer<S, C> checkpointer;
	private final StateHandleProvider<C> provider;

	public StreamOperatorState(StateCheckpointer<S, C> checkpointer, StateHandleProvider<C> provider) {
		this.checkpointer = checkpointer;
		this.provider = provider;
	}
	
	@SuppressWarnings("unchecked")
	public StreamOperatorState(StateHandleProvider<C> provider) {
		this((StateCheckpointer<S, C>) new BasicCheckpointer(), provider);
	}

	@Override
	public S getState() {
		return state;
	}

	@Override
	public void updateState(S state) {
		this.state = state;
	}
	
	public void setDefaultState(S defaultState) {
		// reconsider this as it might cause issues when setting the state to null
		if (getState() == null) {
			updateState(defaultState);
		}
	}

	public StateCheckpointer<S, C> getCheckpointer() {
		return checkpointer;
	}
	
	public void setCheckpointer(StateCheckpointer<S, C> checkpointer) {
		this.checkpointer = checkpointer;
	}

	protected StateHandleProvider<C> getStateHandleProvider() {
		return provider;
	}

	public Map<Serializable, StateHandle<C>> snapshotState(long checkpointId,
			long checkpointTimestamp) throws Exception {
		return ImmutableMap.of(DEFAULTKEY, provider.createStateHandle(checkpointer.snapshotState(
				getState(), checkpointId, checkpointTimestamp)));

	}

	public void restoreState(Map<Serializable, StateHandle<C>> snapshots) throws Exception {
		updateState(checkpointer.restoreState(snapshots.get(DEFAULTKEY).getState()));
	}

	public Map<Serializable, S> getPartitionedState() throws Exception {
		return ImmutableMap.of(DEFAULTKEY, state);
	}

}
