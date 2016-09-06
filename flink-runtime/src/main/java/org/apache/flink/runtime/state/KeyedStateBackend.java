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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RunnableFuture;

/**
 * A keyed state backend is responsible for managing keyed state. The state can be checkpointed
 * to streams using {@link #snapshot(long, long, CheckpointStreamFactory)}.
 *
 * @param <K> The key by which state is keyed.
 */
public abstract class KeyedStateBackend<K> {

	/** {@link TypeSerializer} for our key. */
	protected final TypeSerializer<K> keySerializer;

	/** The currently active key. */
	protected K currentKey;

	/** The key group of the currently active key */
	private int currentKeyGroup;

	/** So that we can give out state when the user uses the same key. */
	protected HashMap<String, KvState<?>> keyValueStatesByName;

	/** For caching the last accessed partitioned state */
	private String lastName;

	@SuppressWarnings("rawtypes")
	private KvState lastState;

	/** The number of key-groups aka max parallelism */
	protected final int numberOfKeyGroups;

	/** Range of key-groups for which this backend is responsible */
	protected final KeyGroupRange keyGroupRange;

	/** KvStateRegistry helper for this task */
	protected final TaskKvStateRegistry kvStateRegistry;

	public KeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange) {

		this.kvStateRegistry = Preconditions.checkNotNull(kvStateRegistry);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.numberOfKeyGroups = Preconditions.checkNotNull(numberOfKeyGroups);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
	}

	/**
	 * Closes the state backend, releasing all internal resources, but does not delete any persistent
	 * checkpoint data.
	 *
	 * @throws Exception Exceptions can be forwarded and will be logged by the system
	 */
	public void close() throws Exception {
		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterAll();
		}

		lastName = null;
		lastState = null;
		keyValueStatesByName = null;
	}

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
	 * @param newKey The new current key.
	 */
	public void setCurrentKey(K newKey) {
		this.currentKey = newKey;
		this.currentKeyGroup = KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups);
	}

	/**
	 * {@link TypeSerializer} for the state backend key type.
	 */
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Used by states to access the current key.
	 */
	public K getCurrentKey() {
		return currentKey;
	}

	public int getCurrentKeyGroupIndex() {
		return currentKeyGroup;
	}

	public int getNumberOfKeyGroups() {
		return numberOfKeyGroups;
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
	public <N, S extends State> S getPartitionedState(final N namespace, final TypeSerializer<N> namespaceSerializer, final StateDescriptor<S, ?> stateDescriptor) throws Exception {
		Preconditions.checkNotNull(namespace, "Namespace");
		Preconditions.checkNotNull(namespaceSerializer, "Namespace serializer");

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

		KvState<?> previous = keyValueStatesByName.get(stateDescriptor.getName());
		if (previous != null) {
			lastState = previous;
			lastState.setCurrentNamespace(namespace);
			lastName = stateDescriptor.getName();
			return (S) previous;
		}

		// create a new blank key/value state
		S state = stateDescriptor.bind(new StateBackend() {
			@Override
			public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
				return KeyedStateBackend.this.createValueState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
				return KeyedStateBackend.this.createListState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
				return KeyedStateBackend.this.createReducingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
				return KeyedStateBackend.this.createFoldingState(namespaceSerializer, stateDesc);
			}

		});

		KvState kvState = (KvState) state;

		keyValueStatesByName.put(stateDescriptor.getName(), kvState);

		lastName = stateDescriptor.getName();
		lastState = kvState;

		kvState.setCurrentNamespace(namespace);

		// Publish queryable state
		if (stateDescriptor.isQueryable()) {
			if (kvStateRegistry == null) {
				throw new IllegalStateException("State backend has not been initialized for job.");
			}

			String name = stateDescriptor.getQueryableStateName();
			// TODO: deal with key group indices here
			kvStateRegistry.registerKvState(0, name, kvState);
		}

		return state;
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

	/**
	 * Snapshots the keyed state by writing it to streams that are provided by a
	 * {@link CheckpointStreamFactory}.
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 *
	 * @return A future that will yield a {@link KeyGroupsStateHandle} with the index and
	 * written key group state stream.
	 */
	public abstract RunnableFuture<KeyGroupsStateHandle> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory) throws Exception;
}
