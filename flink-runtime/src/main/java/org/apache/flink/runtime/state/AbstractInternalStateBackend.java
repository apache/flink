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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedListStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateImpl;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateBinder;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateImpl;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateBinder;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateImpl;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base implementation for {@link InternalStateBackend}.
 */
public abstract class AbstractInternalStateBackend implements
		InternalStateBackend,
		Closeable,
		KeyedStateBinder,
		SubKeyedStateBinder,
		CheckpointListener {

	/**
	 * The total number of groups in all subtasks.
	 */
	private int numberOfGroups;

	/**
	 * The groups of the given scope in the backend.
	 */
	private KeyGroupRange keyGroupRange;

	/**
	 * The classloader for the user code in this operator.
	 */
	private ClassLoader userClassLoader;

	/** Registry for all opened streams, so they can be closed if the task using this backend is closed. */
	protected CloseableRegistry cancelStreamRegistry;

	/**
	 * StateRegistry helper for this task.
	 */
	protected TaskKvStateRegistry kvStateRegistry;

	/**
	 * Map of state names to their corresponding restored state meta info snapshot.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	protected final Map<String, StateMetaInfoSnapshot> restoredKvStateMetaInfos;

	/**
	 * Map of all state-storage.
	 */
	protected final Map<String, StateStorage> stateStorages;

	/**
	 * The keyed state using storages backend by the backend.
	 */
	protected final transient Map<String, KeyedState> keyedStates;

	/**
	 * The subKeyed state using storages backend by the backend.
	 */
	protected final transient Map<String, SubKeyedState> subKeyedStates;

	/**
	 * Map of state names to their registered state meta info.
	 */
	protected final Map<String, RegisteredStateMetaInfo> registeredStateMetaInfos;

	/**
	 * Decorates the input and output streams to write key-groups compressed.
	 */
	protected final StreamCompressionDecorator keyGroupCompressionDecorator;

	/**
	 * Subclasses should implement this method to release unused resources.
	 */
	protected void closeImpl() {}

	/**
	 * Creates the state storage described by the given keyed descriptor.
	 *
	 * @param stateMetaInfo The descriptor of the state storage to be created.
	 * @return The state storage described by the given descriptor.
	 */
	protected abstract StateStorage getOrCreateStateStorageForKeyedState(RegisteredStateMetaInfo stateMetaInfo);

	/**
	 * Creates the state storage described by the given sub-keyed descriptor.
	 *
	 * @param stateMetaInfo The descriptor of the state storage to be created.
	 * @return The state storage described by the given descriptor.
	 */
	protected abstract StateStorage getOrCreateStateStorageForSubKeyedState(RegisteredStateMetaInfo stateMetaInfo);

	//--------------------------------------------------------------------------

	protected AbstractInternalStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		TaskKvStateRegistry kvStateRegistry,
		ExecutionConfig executionConfig) {

		this.numberOfGroups = numberOfGroups;
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader);
		this.cancelStreamRegistry = new CloseableRegistry();
		this.kvStateRegistry = kvStateRegistry;

		this.restoredKvStateMetaInfos = new HashMap<>();
		this.stateStorages = new HashMap<>();

		this.keyedStates = new HashMap<>();
		this.subKeyedStates = new HashMap<>();
		this.registeredStateMetaInfos = new HashMap<>();
		this.keyGroupCompressionDecorator = determineStreamCompression(executionConfig);
	}

	private StreamCompressionDecorator determineStreamCompression(ExecutionConfig executionConfig) {
		if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
			return SnappyStreamCompressionDecorator.INSTANCE;
		} else {
			return UncompressedStreamCompressionDecorator.INSTANCE;
		}
	}

	@Override
	public int getNumGroups() {
		return numberOfGroups;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public Map<String, StateMetaInfoSnapshot> getRestoredKvStateMetaInfos() {
		return restoredKvStateMetaInfos;
	}

	@Override
	public ClassLoader getUserClassLoader() {
		return userClassLoader;
	}

	@Override
	public Map<String, KeyedState> getKeyedStates() {
		return keyedStates;
	}

	@Override
	public Map<String, SubKeyedState> getSubKeyedStates() {
		return subKeyedStates;
	}

	public Map<String, RegisteredStateMetaInfo> getRegisteredStateMetaInfos() {
		return registeredStateMetaInfos;
	}

	public TaskKvStateRegistry getKvStateRegistry() {
		return kvStateRegistry;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		// Nothing to do by default.
	}

	/**
	 * Returns the total number of state entries across all keys/namespaces.
	 */
	@VisibleForTesting
	public abstract int numStateEntries();


	//--------------------------------------------------------------------------

	@Override
	public void dispose() {
		closeImpl();

		IOUtils.closeQuietly(cancelStreamRegistry);

		keyedStates.clear();
		subKeyedStates.clear();
		registeredStateMetaInfos.clear();
		stateStorages.clear();
		restoredKvStateMetaInfos.clear();
	}

	@Override
	public void close() throws IOException {
		cancelStreamRegistry.close();
	}

	@Override
	public <K, V, S extends KeyedState<K, V>> S getKeyedState(
		KeyedStateDescriptor<K, V, S> keyedStateDescriptor
	) throws Exception {
		checkNotNull(keyedStateDescriptor);

		return keyedStateDescriptor.bind(this);
	}

	@Override
	public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
		SubKeyedStateDescriptor<K, N, V, S> stateDescriptor
	) throws Exception {
		checkNotNull(stateDescriptor);

		return stateDescriptor.bind(this);
	}

	@Override
	public <K, V> KeyedValueState<K, V> createKeyedValueState(KeyedValueStateDescriptor<K, V> keyedStateDescriptor) throws Exception {
		String stateName = keyedStateDescriptor.getName();
		KeyedValueState<K, V> keyedState = (KeyedValueState<K, V>) keyedStates.get(stateName);

		if (keyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(keyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForKeyedState(newStateMetaInfo);
			keyedState = new KeyedValueStateImpl<>(this, keyedStateDescriptor, stateStorage);
			keyedStates.put(stateName, keyedState);
		}

		return keyedState;
	}

	@Override
	public <K, E> KeyedListState<K, E> createKeyedListState(KeyedListStateDescriptor<K, E> keyedStateDescriptor) throws Exception {
		String stateName = keyedStateDescriptor.getName();
		KeyedListState<K, E> keyedState = (KeyedListState<K, E>) keyedStates.get(stateName);

		if (keyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(keyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForKeyedState(newStateMetaInfo);
			keyedState = new KeyedListStateImpl<>(this, keyedStateDescriptor, stateStorage);
			keyedStates.put(stateName, keyedState);
		}

		return keyedState;
	}

	@Override
	public <K, MK, MV> KeyedMapState<K, MK, MV> createKeyedMapState(KeyedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) throws Exception {
		String stateName = keyedStateDescriptor.getName();
		KeyedMapState<K, MK, MV> keyedState = (KeyedMapState<K, MK, MV>) keyedStates.get(stateName);

		if (keyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(keyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForKeyedState(newStateMetaInfo);
			keyedState = new KeyedMapStateImpl<>(this, keyedStateDescriptor, stateStorage);
			keyedStates.put(stateName, keyedState);
		}

		return keyedState;
	}

	@Override
	public <K, MK, MV> KeyedSortedMapState<K, MK, MV> createKeyedSortedMapState(KeyedSortedMapStateDescriptor<K, MK, MV> keyedStateDescriptor) throws Exception {
		String stateName = keyedStateDescriptor.getName();
		KeyedSortedMapState<K, MK, MV> keyedState = (KeyedSortedMapState<K, MK, MV>) keyedStates.get(stateName);

		if (keyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(keyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForKeyedState(newStateMetaInfo);
			keyedState = new KeyedSortedMapStateImpl<>(this, keyedStateDescriptor, stateStorage);
			keyedStates.put(stateName, keyedState);
		}

		return keyedState;
	}

	@Override
	public <K, N, V> SubKeyedValueState<K, N, V> createSubKeyedValueState(SubKeyedValueStateDescriptor<K, N, V> subKeyedStateDescriptor) throws Exception {
		String stateName = subKeyedStateDescriptor.getName();
		SubKeyedValueState<K, N, V> subKeyedState = (SubKeyedValueState<K, N, V>) subKeyedStates.get(stateName);

		if (subKeyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(subKeyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForSubKeyedState(newStateMetaInfo);
			subKeyedState = new SubKeyedValueStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
			subKeyedStates.put(stateName, subKeyedState);
		}

		return subKeyedState;
	}

	@Override
	public <K, N, E> SubKeyedListState<K, N, E> createSubKeyedListState(SubKeyedListStateDescriptor<K, N, E> subKeyedStateDescriptor) throws Exception {
		String stateName = subKeyedStateDescriptor.getName();
		SubKeyedListState<K, N, E> subKeyedState = (SubKeyedListState<K, N, E>) subKeyedStates.get(stateName);

		if (subKeyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(subKeyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForSubKeyedState(newStateMetaInfo);
			subKeyedState = new SubKeyedListStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
			subKeyedStates.put(stateName, subKeyedState);
		}

		return subKeyedState;
	}

	@Override
	public <K, N, MK, MV> SubKeyedMapState<K, N, MK, MV> createSubKeyedMapState(SubKeyedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) throws Exception {
		String stateName = subKeyedStateDescriptor.getName();
		SubKeyedMapState<K, N, MK, MV> subKeyedState = (SubKeyedMapState<K, N, MK, MV>) subKeyedStates.get(stateName);

		if (subKeyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(subKeyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForSubKeyedState(newStateMetaInfo);
			subKeyedState = new SubKeyedMapStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
			subKeyedStates.put(stateName, subKeyedState);
		}

		return subKeyedState;
	}

	@Override
	public <K, N, MK, MV> SubKeyedSortedMapState<K, N, MK, MV> createSubKeyedSortedMapState(SubKeyedSortedMapStateDescriptor<K, N, MK, MV> subKeyedStateDescriptor) throws Exception {
		String stateName = subKeyedStateDescriptor.getName();
		SubKeyedSortedMapState<K, N, MK, MV> subKeyedState = (SubKeyedSortedMapState<K, N, MK, MV>) subKeyedStates.get(stateName);

		if (subKeyedState == null) {
			RegisteredStateMetaInfo newStateMetaInfo = tryRegisterStateMetaInfo(subKeyedStateDescriptor);
			StateStorage stateStorage = getOrCreateStateStorageForSubKeyedState(newStateMetaInfo);
			subKeyedState = new SubKeyedSortedMapStateImpl<>(this, subKeyedStateDescriptor, stateStorage);
			subKeyedStates.put(stateName, subKeyedState);
		}

		return subKeyedState;
	}

	//--------------------------------------------------------------------------

	private RegisteredStateMetaInfo tryRegisterStateMetaInfo(KeyedStateDescriptor stateDescriptor) throws StateMigrationException {
		Preconditions.checkNotNull(stateDescriptor);

		String stateName = stateDescriptor.getName();
		RegisteredStateMetaInfo stateInfo = registeredStateMetaInfos.get(stateName);

		if (stateInfo != null) {
			StateMetaInfoSnapshot restoredStateMetaInfoSnapshot = restoredKvStateMetaInfos.get(stateName);
			Preconditions.checkState(
				restoredStateMetaInfoSnapshot != null,
				"Requested to check compatibility of a restored StateMetaInfoSnapshot," +
					" but its corresponding restored snapshot cannot be found.");

			stateInfo = RegisteredStateMetaInfo.resolveStateCompatibility(restoredStateMetaInfoSnapshot, stateDescriptor);
		} else {
			stateInfo = RegisteredStateMetaInfo.createKeyedStateMetaInfo(
				stateDescriptor.getStateType(), stateName, stateDescriptor.getKeySerializer(), stateDescriptor.getValueSerializer());
		}

		stateDescriptor.setKeySerializer(stateInfo.getKeySerializer());
		stateDescriptor.setValueSerializer(stateInfo.getValueSerializer());
		registeredStateMetaInfos.put(stateName, stateInfo);
		return stateInfo;
	}

	private RegisteredStateMetaInfo tryRegisterStateMetaInfo(SubKeyedStateDescriptor stateDescriptor) throws StateMigrationException {
		Preconditions.checkNotNull(stateDescriptor);

		String stateName = stateDescriptor.getName();
		RegisteredStateMetaInfo stateInfo = registeredStateMetaInfos.get(stateName);

		if (stateInfo != null) {
			StateMetaInfoSnapshot restoredStateMetaInfoSnapshot = restoredKvStateMetaInfos.get(stateName);
			Preconditions.checkState(
				restoredStateMetaInfoSnapshot != null,
				"Requested to check compatibility of a restored StateMetaInfoSnapshot," +
					" but its corresponding restored snapshot cannot be found.");

			stateInfo = RegisteredStateMetaInfo.resolveStateCompatibility(restoredStateMetaInfoSnapshot, stateDescriptor);
		} else {
			stateInfo = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(
				stateDescriptor.getStateType(), stateName, stateDescriptor.getKeySerializer(), stateDescriptor.getValueSerializer(), stateDescriptor.getNamespaceSerializer());
		}

		registeredStateMetaInfos.put(stateName, stateInfo);
		return stateInfo;
	}

	@Override
	public Map<String, StateStorage> getStateStorages() {
		return stateStorages;
	}

	public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
		return keyGroupCompressionDecorator;
	}
}
