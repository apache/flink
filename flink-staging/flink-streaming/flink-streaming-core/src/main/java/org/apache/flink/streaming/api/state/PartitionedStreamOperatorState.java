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
import java.util.Map;

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.PartitionedStateStore;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.InstantiationUtil;

/**
 * Implementation of the {@link OperatorState} interface for partitioned user
 * states. It provides methods for checkpointing and restoring partitioned
 * operator states upon failure.
 * 
 * @param <IN>
 *            Input type of the underlying {@link OneInputStreamOperator}
 * @param <S>
 *            Type of the underlying {@link OperatorState}.
 * @param <C>
 *            Type of the state snapshot.
 */
public class PartitionedStreamOperatorState<IN, S, C extends Serializable> extends
		StreamOperatorState<S, C> {

	// KeySelector for getting the state partition key for each input
	private final KeySelector<IN, Serializable> keySelector;

	private final PartitionedStateStore<S, C> stateStore;

	private byte[] defaultState;

	// The currently processed input, used to extract the appropriate key
	private IN currentInput;

	private ClassLoader cl;

	public PartitionedStreamOperatorState(StateCheckpointer<S, C> checkpointer,
			StateHandleProvider<C> provider, KeySelector<IN, Serializable> keySelector, ClassLoader cl) {
		super(checkpointer, provider);
		this.keySelector = keySelector;
		this.stateStore = new EagerStateStore<S, C>(checkpointer, provider);
		this.cl = cl;
	}

	@SuppressWarnings("unchecked")
	public PartitionedStreamOperatorState(StateHandleProvider<C> provider,
			KeySelector<IN, Serializable> keySelector, ClassLoader cl) {
		this((StateCheckpointer<S, C>) new BasicCheckpointer(), provider, keySelector, cl);
	}

	@SuppressWarnings("unchecked")
	@Override
	public S value() throws IOException {
		if (currentInput == null) {
			throw new IllegalStateException("Need a valid input for accessing the state.");
		} else {
			try {
				Serializable key = keySelector.getKey(currentInput);
				if (stateStore.containsKey(key)) {
					return stateStore.getStateForKey(key);
				} else {
					return checkpointer.restoreState((C) InstantiationUtil.deserializeObject(
							defaultState, cl));
				}
			} catch (Exception e) {
				throw new RuntimeException("User-defined key selector threw an exception.", e);
			}
		}
	}

	@Override
	public void update(S state) throws IOException {
		if (state == null) {
			throw new RuntimeException("Cannot set state to null.");
		}
		if (currentInput == null) {
			throw new IllegalStateException("Need a valid input for updating a state.");
		} else {
			try {
				stateStore.setStateForKey(keySelector.getKey(currentInput), state);
			} catch (Exception e) {
				throw new RuntimeException("User-defined key selector threw an exception.");
			}
		}
	}

	@Override
	public void setDefaultState(S defaultState) {
		try {
			this.defaultState = InstantiationUtil.serializeObject(checkpointer.snapshotState(
					defaultState, 0, 0));
		} catch (IOException e) {
			throw new RuntimeException("Default state must be serializable.");
		}
	}

	public void setCurrentInput(IN input) {
		currentInput = input;
	}

	@Override
	public Map<Serializable, StateHandle<C>> snapshotState(long checkpointId,
			long checkpointTimestamp) throws Exception {
		return stateStore.snapshotStates(checkpointId, checkpointTimestamp);
	}

	@Override
	public void restoreState(Map<Serializable, StateHandle<C>> snapshots) throws Exception {
		stateStore.restoreStates(snapshots);
	}

	@Override
	public Map<Serializable, S> getPartitionedState() throws Exception {
		return stateStore.getPartitionedState();
	}

	@Override
	public String toString() {
		return stateStore.toString();
	}

}
