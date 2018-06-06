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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base implementation of KeyedStateBackend. The state can be checkpointed
 * to streams using {@link #snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
public abstract class AbstractKeyedStateBackend<K> implements
	KeyedStateBackend<K>,
	Snapshotable<SnapshotResult<KeyedStateHandle>, Collection<KeyedStateHandle>>,
	Closeable,
	CheckpointListener {

	/** {@link TypeSerializer} for our key. */
	protected final TypeSerializer<K> keySerializer;

	/** The currently active key. */
	protected K currentKey;

	/** The key group of the currently active key */
	private int currentKeyGroup;

	/** So that we can give out state when the user uses the same key. */
	protected final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

	/** For caching the last accessed partitioned state */
	private String lastName;

	@SuppressWarnings("rawtypes")
	private InternalKvState lastState;

	/** The number of key-groups aka max parallelism */
	protected final int numberOfKeyGroups;

	/** Range of key-groups for which this backend is responsible */
	protected final KeyGroupRange keyGroupRange;

	/** KvStateRegistry helper for this task */
	protected final TaskKvStateRegistry kvStateRegistry;

	/** Registry for all opened streams, so they can be closed if the task using this backend is closed */
	protected CloseableRegistry cancelStreamRegistry;

	protected final ClassLoader userCodeClassLoader;

	private final ExecutionConfig executionConfig;

	/** Decorates the input and output streams to write key-groups compressed. */
	protected final StreamCompressionDecorator keyGroupCompressionDecorator;

	public AbstractKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		ExecutionConfig executionConfig) {

		this.kvStateRegistry = kvStateRegistry;
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.numberOfKeyGroups = Preconditions.checkNotNull(numberOfKeyGroups);
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.cancelStreamRegistry = new CloseableRegistry();
		this.keyValueStatesByName = new HashMap<>();
		this.executionConfig = executionConfig;
		this.keyGroupCompressionDecorator = determineStreamCompression(executionConfig);
	}

	private StreamCompressionDecorator determineStreamCompression(ExecutionConfig executionConfig) {
		if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
			return SnappyStreamCompressionDecorator.INSTANCE;
		} else {
			return UncompressedStreamCompressionDecorator.INSTANCE;
		}
	}

	/**
	 * Closes the state backend, releasing all internal resources, but does not delete any persistent
	 * checkpoint data.
	 *
	 */
	@Override
	public void dispose() {

		IOUtils.closeQuietly(cancelStreamRegistry);

		if (kvStateRegistry != null) {
			kvStateRegistry.unregisterAll();
		}

		lastName = null;
		lastState = null;
		keyValueStatesByName.clear();
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
	protected abstract <N, T> InternalValueState<K, N, T> createValueState(
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ListState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> InternalListState<K, N, T> createListState(
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ReducingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> InternalReducingState<K, N, T> createReducingState(
			TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link AggregatingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T, ACC, R> InternalAggregatingState<K, N, T, ACC, R> createAggregatingState(
			TypeSerializer<N> namespaceSerializer,
			AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link FoldingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> Type of the values folded into the state
	 * @param <ACC> Type of the value in the state
	 *
	 * @deprecated will be removed in a future version
	 */
	@Deprecated
	protected abstract <N, T, ACC> InternalFoldingState<K, N, T, ACC> createFoldingState(
			TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link MapState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <UK> Type of the keys in the state
	 * @param <UV> Type of the values in the state	 *
	 */
	protected abstract <N, UK, UV> InternalMapState<K, N, UK, UV> createMapState(
			TypeSerializer<N> namespaceSerializer,
			MapStateDescriptor<UK, UV> stateDesc) throws Exception;

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public void setCurrentKey(K newKey) {
		this.currentKey = newKey;
		this.currentKeyGroup = KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups);
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public K getCurrentKey() {
		return currentKey;
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public int getCurrentKeyGroupIndex() {
		return currentKeyGroup;
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public int getNumberOfKeyGroups() {
		return numberOfKeyGroups;
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public <N, S extends State, T> void applyToAllKeys(
			final N namespace,
			final TypeSerializer<N> namespaceSerializer,
			final StateDescriptor<S, T> stateDescriptor,
			final KeyedStateFunction<K, S> function) throws Exception {

		try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

			final S state = getPartitionedState(
				namespace,
				namespaceSerializer,
				stateDescriptor);

			keyStream.forEach((K key) -> {
				setCurrentKey(key);
				try {
					function.process(key, state);
				} catch (Throwable e) {
					// we wrap the checked exception in an unchecked
					// one and catch it (and re-throw it) later.
					throw new RuntimeException(e);
				}
			});
		} catch (RuntimeException e) {
			throw e;
		}
	}

	/**
	 * @see KeyedStateBackend
	 */
	@Override
	public <N, S extends State, V> S getOrCreateKeyedState(
			final TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, V> stateDescriptor) throws Exception {

		checkNotNull(namespaceSerializer, "Namespace serializer");

		if (keySerializer == null) {
			throw new UnsupportedOperationException(
					"State key serializer has not been configured in the config. " +
					"This operation cannot use partitioned state.");
		}

		if (!stateDescriptor.isSerializerInitialized()) {
			stateDescriptor.initializeSerializerUnlessSet(executionConfig);
		}

		InternalKvState<K, ?, ?> existing = keyValueStatesByName.get(stateDescriptor.getName());
		if (existing != null) {
			@SuppressWarnings("unchecked")
			S typedState = (S) existing;
			return typedState;
		}

		// create a new blank key/value state
		S state = stateDescriptor.bind(new StateBinder() {
			@Override
			public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createValueState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createListState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createReducingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T, ACC, R> AggregatingState<T, R> createAggregatingState(
					AggregatingStateDescriptor<T, ACC, R> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createAggregatingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createFoldingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <UK, UV> MapState<UK, UV> createMapState(MapStateDescriptor<UK, UV> stateDesc) throws Exception {
				return AbstractKeyedStateBackend.this.createMapState(namespaceSerializer, stateDesc);
			}

		});

		@SuppressWarnings("unchecked")
		InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;
		keyValueStatesByName.put(stateDescriptor.getName(), kvState);

		// Publish queryable state
		if (stateDescriptor.isQueryable()) {
			if (kvStateRegistry == null) {
				throw new IllegalStateException("State backend has not been initialized for job.");
			}

			String name = stateDescriptor.getQueryableStateName();
			kvStateRegistry.registerKvState(keyGroupRange, name, kvState);
		}

		return state;
	}

	/**
	 * TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
	 *       This method should be removed for the sake of namespaces being lazily fetched from the keyed
	 *       state backend, or being set on the state directly.
	 * 
	 * @see KeyedStateBackend
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <N, S extends State> S getPartitionedState(
			final N namespace,
			final TypeSerializer<N> namespaceSerializer,
			final StateDescriptor<S, ?> stateDescriptor) throws Exception {

		checkNotNull(namespace, "Namespace");

		if (lastName != null && lastName.equals(stateDescriptor.getName())) {
			lastState.setCurrentNamespace(namespace);
			return (S) lastState;
		}

		InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
		if (previous != null) {
			lastState = previous;
			lastState.setCurrentNamespace(namespace);
			lastName = stateDescriptor.getName();
			return (S) previous;
		}

		final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
		final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

		lastName = stateDescriptor.getName();
		lastState = kvState;
		kvState.setCurrentNamespace(namespace);

		return state;
	}

	@Override
	public void close() throws IOException {
		cancelStreamRegistry.close();
	}

	@VisibleForTesting
	public boolean supportsAsynchronousSnapshots() {
		return false;
	}

	@VisibleForTesting
	public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
		return keyGroupCompressionDecorator;
	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@VisibleForTesting
	public abstract int numStateEntries();

}
