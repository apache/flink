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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 */
public abstract class AbstractPartitionedStateBackend<KEY> implements PartitionedStateBackend<KEY> {
	
	protected final TypeSerializer<KEY> keySerializer;

	protected final ClassLoader classLoader;

	protected KEY currentKey;

	/** For efficient access in setCurrentKey() */
	private KvState<KEY, ?, ?, ?, ?>[] keyValueStates;

	/** So that we can give out state when the user uses the same key. */
	protected HashMap<String, KvState<KEY, ?, ?, ?, ?>> keyValueStatesByName;

	/** For caching the last accessed partitioned state */
	private String lastName;

	@SuppressWarnings("rawtypes")
	private KvState lastState;

	public AbstractPartitionedStateBackend(TypeSerializer<KEY> keySerializer, ClassLoader classLoader) {
		this.keySerializer = keySerializer;
		this.classLoader = classLoader;
	}

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	/**
	 * Disposes all state associated with the current job.
	 *
	 * @throws Exception Exceptions may occur during disposal of the state and should be forwarded.
	 */
	public abstract void disposeAllStateForCurrentJob() throws Exception;

	public void close() throws Exception {
		if (keyValueStates != null) {
			for (KvState<?, ?, ?, ?, ?> state : keyValueStates) {
				state.dispose();
			}
		}
	}

	/**
	 * Disposes only the state associated with the respective state descriptor.
	 *
	 * @param stateDescriptor StateDescriptor defining the state to be disposed
	 */
	public <S extends PartitionedState> void dispose(StateDescriptor<S, ?> stateDescriptor) {
		if (keyValueStatesByName.containsKey(stateDescriptor.getName())) {
			keyValueStatesByName.get(stateDescriptor.getName()).dispose();
		}
	}
	
	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	/**
	 * Creates and returns a new {@link ValueState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the value that the {@code ValueState} can store.
	 */
	protected abstract <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ListState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ReducingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link FoldingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> Type of the values folded into the state
	 * @param <ACC> Type of the value in the state	 *
	 */
	protected abstract <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception;


	/**
	 * Sets the current key that is used for partitioned state.
	 * @param currentKey The current key.
	 */
	public void setCurrentKey(KEY currentKey) {
		this.currentKey = currentKey;
		if (keyValueStates != null) {
			for (KvState kv : keyValueStates) {
				kv.setCurrentKey(currentKey);
			}
		}
	}

	public KEY getCurrentKey() {
		return currentKey;
	}

	/**
	 * Creates or retrieves a partitioned state backed by this state backend.
	 *
	 * @param stateDescriptor The state identifier for the state. This contains name
	 *                           and can create a default state value.

	 * @param <N> The type of the namespace.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <N, S extends PartitionedState> S getPartitionedState(
		final N namespace,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) throws Exception {

		if (keySerializer == null) {
			throw new RuntimeException("State key serializer has not been configured in the config. " +
					"This operation cannot use partitioned state.");
		}
		
		if (!stateDescriptor.isSerializerInitialized()) {
			stateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		if (keyValueStatesByName == null) {
			keyValueStatesByName = new HashMap<>();
		}

		if (lastName != null && lastName.equals(stateDescriptor.getName())) {
			lastState.setCurrentNamespace(namespace);
			return (S) lastState;
		}

		KvState<?, ?, ?, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
		if (previous != null) {
			lastState = previous;
			lastState.setCurrentNamespace(namespace);
			lastName = stateDescriptor.getName();
			return (S) previous;
		}

		// create a new blank key/value state
		S kvstate = stateDescriptor.bind(new StateBackend() {
			@Override
			public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
				return AbstractPartitionedStateBackend.this.createValueState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
				return AbstractPartitionedStateBackend.this.createListState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
				return AbstractPartitionedStateBackend.this.createReducingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
				return AbstractPartitionedStateBackend.this.createFoldingState(namespaceSerializer, stateDesc);
			}

		});

		keyValueStatesByName.put(stateDescriptor.getName(), (KvState) kvstate);
		keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);

		lastName = stateDescriptor.getName();
		lastState = (KvState<?, ?, ?, ?, ?>) kvstate;

		((KvState) kvstate).setCurrentKey(currentKey);
		((KvState) kvstate).setCurrentNamespace(namespace);

		return kvstate;
	}

	@SuppressWarnings("unchecked,rawtypes")
	public <N, S extends MergingState<?, ?>> void mergePartitionedStates(final N target, Collection<N> sources, final TypeSerializer<N> namespaceSerializer, final StateDescriptor<S, ?> stateDescriptor) throws Exception {
		if (stateDescriptor instanceof ReducingStateDescriptor) {
			ReducingStateDescriptor reducingStateDescriptor = (ReducingStateDescriptor) stateDescriptor;
			ReduceFunction reduceFn = reducingStateDescriptor.getReduceFunction();
			ReducingState state = (ReducingState) getPartitionedState(target, namespaceSerializer, stateDescriptor);
			KvState kvState = (KvState) state;
			Object result = null;
			for (N source: sources) {
				kvState.setCurrentNamespace(source);
				Object sourceValue = state.get();
				if (result == null) {
					result = state.get();
				} else if (sourceValue != null) {
					result = reduceFn.reduce(result, sourceValue);
				}
				state.clear();
			}
			kvState.setCurrentNamespace(target);
			if (result != null) {
				state.add(result);
			}
		} else if (stateDescriptor instanceof ListStateDescriptor) {
			ListState<Object> state = (ListState) getPartitionedState(target, namespaceSerializer, stateDescriptor);
			KvState kvState = (KvState) state;
			List<Object> result = new ArrayList<>();
			for (N source: sources) {
				kvState.setCurrentNamespace(source);
				Iterable<Object> sourceValue = state.get();
				if (sourceValue != null) {
					for (Object o : sourceValue) {
						result.add(o);
					}
				}
				state.clear();
			}
			kvState.setCurrentNamespace(target);
			for (Object o : result) {
				state.add(o);
			}
		} else {
			throw new RuntimeException("Cannot merge states for " + stateDescriptor);
		}
	}

	public PartitionedStateSnapshot snapshotPartitionedState(
		long checkpointId,
		long timestamp) throws Exception {

		if (keyValueStates != null) {
			PartitionedStateSnapshot partitionedStateSnapshot = new PartitionedStateSnapshot();

			for (Map.Entry<String, KvState<KEY, ?, ?, ?, ?>> entry : keyValueStatesByName.entrySet()) {
				KvStateSnapshot<KEY, ?, ?, ?, ?> snapshot = entry.getValue().snapshot(checkpointId, timestamp);
				partitionedStateSnapshot.put(entry.getKey(), snapshot);
			}

			return partitionedStateSnapshot;
		} else {
			return null;
		}
	}

	public void notifyCompletedCheckpoint(long checkpointId) throws Exception {
		// We check whether the KvStates require notifications
		if (keyValueStates != null) {
			for (KvState<?, ?, ?, ?, ?> kvstate : keyValueStates) {
				if (kvstate instanceof CheckpointListener) {
					((CheckpointListener) kvstate).notifyCheckpointComplete(checkpointId);
				}
			}
		}
	}

	/**
	 * Injects K/V state snapshots for lazy restore.
	 * @param partitionedStateSnapshot The assigned key groups and their associated states
	 */
	@SuppressWarnings("unchecked,rawtypes")
	public void restorePartitionedState(PartitionedStateSnapshot partitionedStateSnapshot, long recoveryTimestamp) throws Exception {
		if (keyValueStatesByName == null) {
			keyValueStatesByName = new HashMap<>();
		}

		for (Map.Entry<String, KvStateSnapshot<?, ?, ?, ?, ?>> state: partitionedStateSnapshot.entrySet()) {
			KvState kvState = ((KvStateSnapshot)state.getValue()).restoreState(this,
				keySerializer,
				classLoader,
				recoveryTimestamp);
			keyValueStatesByName.put(state.getKey(), kvState);
		}
		keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);
	}
}
