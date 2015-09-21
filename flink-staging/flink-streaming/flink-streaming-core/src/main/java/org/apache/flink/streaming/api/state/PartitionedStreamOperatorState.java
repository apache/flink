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
public class PartitionedStreamOperatorState<IN, S, C extends Serializable> extends StreamOperatorState<S, C> {

	// KeySelector for getting the state partition key for each input
	private final KeySelector<IN, Serializable> keySelector;

	private final PartitionedStateStore<S, C> stateStore;

	private byte[] defaultState;

	// The currently processed input, used to extract the appropriate key
	private IN currentInput;

	private ClassLoader cl;
	private boolean restored = true;
	private StateHandle<Serializable> checkpoint = null;

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
			if (!restored) {
				// If the state is not restored yet, restore now
				restoreWithCheckpointer();
			}
			Serializable key;
			try {
				key = keySelector.getKey(currentInput);
			} catch (Exception e) {
				throw new RuntimeException("User-defined key selector threw an exception.", e);
			}
			if (stateStore.containsKey(key)) {
				return stateStore.getStateForKey(key);
			} else {
				try {
					return (S) checkpointer.restoreState((C) InstantiationUtil.deserializeObject(
							defaultState, cl));
				} catch (ClassNotFoundException e) {
					throw new RuntimeException("Could not deserialize default state value.", e);
				}
			}
		}
	}

	@Override
	public void update(S state) throws IOException {
		if (currentInput == null) {
			throw new IllegalStateException("Need a valid input for updating a state.");
		} else {
			if (!restored) {
				// If the state is not restored yet, restore now
				restoreWithCheckpointer();
			}
			Serializable key;
			try {
				key = keySelector.getKey(currentInput);
			} catch (Exception e) {
				throw new RuntimeException("User-defined key selector threw an exception.");
			}
			
			if (state == null) {
				// Remove state if set to null
				stateStore.removeStateForKey(key);
			} else {
				stateStore.setStateForKey(key, state);
			}
		}
	}

	@Override
	public void setDefaultState(S defaultState) {
		try {
			this.defaultState = InstantiationUtil.serializeObject(checkpointer.snapshotState(defaultState, 0, 0));
		} catch (IOException e) {
			throw new RuntimeException("Default state must be serializable.");
		}
	}

	public void setCurrentInput(IN input) {
		currentInput = input;
	}

	@Override
	public StateHandle<Serializable> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		// If the state is restored we take a snapshot, otherwise return the last checkpoint
		return restored ? stateStore.snapshotStates(checkpointId, checkpointTimestamp) : provider
				.createStateHandle(checkpoint.getState(cl));
	}
	
	@Override
	public void restoreState(StateHandle<Serializable> snapshot, ClassLoader userCodeClassLoader) throws Exception {
		// We store the snapshot for lazy restore
		checkpoint = snapshot;
		restored = false;
	}
	
	private void restoreWithCheckpointer() throws IOException {
		try {
			stateStore.restoreStates(checkpoint, cl);
		} catch (Exception e) {
			throw new IOException(e);
		}
		restored = true;
		checkpoint = null;
	}

	@Override
	public Map<Serializable, S> getPartitionedState() throws Exception {
		return stateStore.getPartitionedState();
	}
	
	@Override
	public void setCheckpointer(StateCheckpointer<S, C> checkpointer) {
		super.setCheckpointer(checkpointer);
		stateStore.setCheckPointer(checkpointer);
	}

	@Override
	public String toString() {
		return stateStore.toString();
	}

}
