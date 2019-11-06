/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key, as provided
 * through the {@link InternalKeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S>
	implements StateSnapshotRestore, Iterable<StateEntry<K, N, S>> {

	/**
	 * The key context view on the backend. This provides information, such as the currently active key.
	 */
	protected final InternalKeyContext<K> keyContext;

	/**
	 * Combined meta information such as name and serializers for this state.
	 */
	protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

	/**
	 * The serializer of the key.
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * The offset to the contiguous key groups.
	 */
	protected final int keyGroupOffset;

	/**
	 * Map for holding the actual state objects. The outer array represents the key-groups.
	 * All array positions will be initialized with an empty state map.
	 */
	protected final StateMap<K, N, S>[] keyGroupedStateMaps;

	/**
	 * @param keyContext    the key context provides the key scope for all put/get/delete operations.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 */
	public StateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.metaInfo = Preconditions.checkNotNull(metaInfo);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);

		this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();

		@SuppressWarnings("unchecked")
		StateMap<K, N, S>[] state = (StateMap<K, N, S>[]) new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
		this.keyGroupedStateMaps = state;
		for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
			this.keyGroupedStateMaps[i] = createStateMap();
		}
	}

	protected abstract StateMap<K, N, S> createStateMap();

	// Main interface methods of StateTable -------------------------------------------------------

	/**
	 * Returns whether this {@link StateTable} is empty.
	 *
	 * @return {@code true} if this {@link StateTable} has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the total number of entries in this {@link StateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link StateTable}.
	 */
	public int size() {
		int count = 0;
		for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
			count += stateMap.size();
		}
		return count;
	}

	/**
	 * Returns the state of the mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace. Not null.
	 * @return the states of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public S get(N namespace) {
		return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	/**
	 * Returns whether this table contains a mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public boolean containsKey(N namespace) {
		return containsKey(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	/**
	 * Maps the composite of active key and given namespace to the specified state.
	 *
	 * @param namespace the namespace. Not null.
	 * @param state     the state. Can be null.
	 */
	public void put(N namespace, S state) {
		put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	/**
	 * Removes the mapping for the composite of active key and given namespace. This method should be preferred
	 * over {@link #removeAndGetOld(N)} when the caller is not interested in the old state.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	public void remove(N namespace) {
		remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	/**
	 * Removes the mapping for the composite of active key and given namespace, returning the state that was
	 * found under the entry.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the state of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public S removeAndGetOld(N namespace) {
		return removeAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	/**
	 * Applies the given {@link StateTransformationFunction} to the state (1st input argument), using the given value as
	 * second input argument. The result of {@link StateTransformationFunction#apply(Object, Object)} is then stored as
	 * the new state. This function is basically an optimization for get-update-put pattern.
	 *
	 * @param namespace      the namespace. Not null.
	 * @param value          the value to use in transforming the state. Can be null.
	 * @param transformation the transformation function.
	 * @throws Exception if some exception happens in the transformation function.
	 */
	public <T> void transform(
			N namespace,
			T value,
			StateTransformationFunction<S, T> transformation) throws Exception {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroup = keyContext.getCurrentKeyGroupIndex();
		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
		stateMap.transform(key, namespace, value, transformation);
	}

	// For queryable state ------------------------------------------------------------------------

	/**
	 * Returns the state for the composite of active key and given namespace. This is typically used by
	 * queryable state.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @return the state of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public S get(K key, N namespace) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
		return get(key, keyGroup, namespace);
	}

	public Stream<K> getKeys(N namespace) {
		return Arrays.stream(keyGroupedStateMaps)
			.flatMap(stateMap -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0), false))
			.filter(entry -> entry.getNamespace().equals(namespace))
			.map(StateEntry::getKey);
	}

	public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
		return Arrays.stream(keyGroupedStateMaps)
			.flatMap(stateMap -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0), false))
			.map(entry -> Tuple2.of(entry.getKey(), entry.getNamespace()));
	}

	public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
	}

	// ------------------------------------------------------------------------

	private S get(K key, int keyGroupIndex, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

		if (stateMap == null) {
			return null;
		}

		return stateMap.get(key, namespace);
	}

	private boolean containsKey(K key, int keyGroupIndex, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

		return stateMap != null && stateMap.containsKey(key, namespace);
	}

	private void checkKeyNamespacePreconditions(K key, N namespace) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		Preconditions.checkNotNull(namespace, "Provided namespace is null.");
	}

	private void remove(K key, int keyGroupIndex, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
		stateMap.remove(key, namespace);
	}

	private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);

		return stateMap.removeAndGetOld(key, namespace);
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	/**
	 * Returns the internal data structure.
	 */
	@VisibleForTesting
	public StateMap<K, N, S>[] getState() {
		return keyGroupedStateMaps;
	}

	public int getKeyGroupOffset() {
		return keyGroupOffset;
	}

	@VisibleForTesting
	StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < keyGroupedStateMaps.length) {
			return keyGroupedStateMaps[pos];
		} else {
			return null;
		}
	}

	/**
	 * Translates a key-group id to the internal array offset.
	 */
	private int indexToOffset(int index) {
		return index - keyGroupOffset;
	}

	// Meta data setter / getter and toString -----------------------------------------------------

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// Snapshot / Restore -------------------------------------------------------------------------

	public void put(K key, int keyGroup, N namespace, S state) {
		checkKeyNamespacePreconditions(key, namespace);

		StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
		stateMap.put(key, namespace, state);
	}

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return Arrays.stream(keyGroupedStateMaps)
			.filter(Objects::nonNull)
			.flatMap(stateMap -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0), false))
			.iterator();
	}

	// For testing --------------------------------------------------------------------------------

	@VisibleForTesting
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
			count += stateMap.sizeOfNamespace(namespace);
		}

		return count;
	}

	@Nonnull
	@Override
	public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
		return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
	}

	// StateEntryIterator  ---------------------------------------------------------------------------------------------

	class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

		final int recommendedMaxNumberOfReturnedRecords;

		int keyGroupIndex;

		StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

		StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
			this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
			this.keyGroupIndex = 0;
			next();
		}

		private void next() {
			while (keyGroupIndex < keyGroupedStateMaps.length) {
				StateMap<K, N, S> stateMap = keyGroupedStateMaps[keyGroupIndex++];
				StateIncrementalVisitor<K, N, S> visitor =
					stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
				if (visitor.hasNext()) {
					stateIncrementalVisitor = visitor;
					return;
				}
			}
		}

		@Override
		public boolean hasNext() {
			while (stateIncrementalVisitor == null || !stateIncrementalVisitor.hasNext()) {
				if (keyGroupIndex == keyGroupedStateMaps.length) {
					return false;
				}
				StateIncrementalVisitor<K, N, S> visitor =
					keyGroupedStateMaps[keyGroupIndex++].getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
				if (visitor.hasNext()) {
					stateIncrementalVisitor = visitor;
					break;
				}
			}
			return true;
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (!hasNext()) {
				return null;
			}

			return stateIncrementalVisitor.nextEntries();
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			keyGroupedStateMaps[keyGroupIndex - 1].remove(stateEntry.getKey(), stateEntry.getNamespace());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			keyGroupedStateMaps[keyGroupIndex - 1].put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
		}
	}
}
