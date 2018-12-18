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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * This implementation of {@link StateTable} uses nested {@link HashMap} objects. It is also maintaining a partitioning
 * by key-group.
 *
 * <p>In contrast to {@link CopyOnWriteStateTable}, this implementation does not support asynchronous snapshots. However,
 * it might have a better memory footprint for some use-cases, e.g. it is naturally de-duplicating namespace objects.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
@Internal
public class NestedMapsStateTable<K, N, S> extends StateTable<K, N, S> {

	/**
	 * Map for holding the actual state objects. The outer array represents the key-groups. The nested maps provide
	 * an outer scope by namespace and an inner scope by key.
	 */
	private final Map<N, Map<K, S>>[] state;

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link NestedMapsStateTable} for the given key context and meta info.
	 *
	 * @param keyContext the key context.
	 * @param metaInfo the meta information for this state table.
	 */
	public NestedMapsStateTable(InternalKeyContext<K> keyContext, RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
		super(keyContext, metaInfo);
		this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();

		@SuppressWarnings("unchecked")
		Map<N, Map<K, S>>[] state = (Map<N, Map<K, S>>[]) new Map[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
		this.state = state;
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	/**
	 * Returns the internal data structure.
	 */
	@VisibleForTesting
	public Map<N, Map<K, S>>[] getState() {
		return state;
	}

	@VisibleForTesting
	Map<N, Map<K, S>> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < state.length) {
			return state[pos];
		} else {
			return null;
		}
	}

	/**
	 * Sets the given map for the given key-group.
	 */
	private void setMapForKeyGroup(int keyGroupId, Map<N, Map<K, S>> map) {
		try {
			state[indexToOffset(keyGroupId)] = map;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Key group index " + keyGroupId + " is out of range of key group " +
				"range [" + keyGroupOffset + ", " + (keyGroupOffset + state.length) + ").");
		}
	}

	/**
	 * Translates a key-group id to the internal array offset.
	 */
	private int indexToOffset(int index) {
		return index - keyGroupOffset;
	}

	// ------------------------------------------------------------------------

	@Override
	public int size() {
		int count = 0;
		for (Map<N, Map<K, S>> namespaceMap : state) {
			if (null != namespaceMap) {
				for (Map<K, S> keyMap : namespaceMap.values()) {
					if (null != keyMap) {
						count += keyMap.size();
					}
				}
			}
		}
		return count;
	}

	@Override
	public S get(N namespace) {
		return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public boolean containsKey(N namespace) {
		return containsKey(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public void put(N namespace, S state) {
		put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	@Override
	public S putAndGetOld(N namespace, S state) {
		return putAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	@Override
	public void remove(N namespace) {
		remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public S removeAndGetOld(N namespace) {
		return removeAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public S get(K key, N namespace) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
		return get(key, keyGroup, namespace);
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return Arrays.stream(state)
			.filter(Objects::nonNull)
			.map(namespaces -> namespaces.getOrDefault(namespace, Collections.emptyMap()))
			.flatMap(namespaceSate -> namespaceSate.keySet().stream());
	}

	// ------------------------------------------------------------------------

	private boolean containsKey(K key, int keyGroupIndex, N namespace) {

		checkKeyNamespacePreconditions(key, namespace);

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return false;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		return keyedMap != null && keyedMap.containsKey(key);
	}

	S get(K key, int keyGroupIndex, N namespace) {

		checkKeyNamespacePreconditions(key, namespace);

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	@Override
	public void put(K key, int keyGroupIndex, N namespace, S value) {
		putAndGetOld(key, keyGroupIndex, namespace, value);
	}

	private S putAndGetOld(K key, int keyGroupIndex, N namespace, S value) {

		checkKeyNamespacePreconditions(key, namespace);

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, namespaceMap);
		}

		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> new HashMap<>());

		return keyedMap.put(key, value);
	}

	private void remove(K key, int keyGroupIndex, N namespace) {
		removeAndGetOld(key, keyGroupIndex, namespace);
	}

	private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {

		checkKeyNamespacePreconditions(key, namespace);

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		S removed = keyedMap.remove(key);

		if (keyedMap.isEmpty()) {
			namespaceMap.remove(namespace);
		}

		return removed;
	}

	private void checkKeyNamespacePreconditions(K key, N namespace) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		Preconditions.checkNotNull(namespace, "Provided namespace is null.");
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (Map<N, Map<K, S>> namespaceMap : state) {
			if (null != namespaceMap) {
				Map<K, S> keyMap = namespaceMap.get(namespace);
				count += keyMap != null ? keyMap.size() : 0;
			}
		}

		return count;
	}

	@Override
	public <T> void transform(N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		final K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);
		final int keyGroupIndex = keyContext.getCurrentKeyGroupIndex();

		Map<N, Map<K, S>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, namespaceMap);
		}

		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> new HashMap<>());
		keyedMap.put(key, transformation.apply(keyedMap.get(key), value));
	}

	// snapshots ---------------------------------------------------------------------------------------------------

	private static <K, N, S> int countMappingsInKeyGroup(final Map<N, Map<K, S>> keyGroupMap) {
		int count = 0;
		for (Map<K, S> namespaceMap : keyGroupMap.values()) {
			count += namespaceMap.size();
		}

		return count;
	}

	@Nonnull
	@Override
	public NestedMapsStateTableSnapshot<K, N, S> stateSnapshot() {
		return new NestedMapsStateTableSnapshot<>(this, metaInfo.getSnapshotTransformer());
	}

	/**
	 * This class encapsulates the snapshot logic.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class NestedMapsStateTableSnapshot<K, N, S>
			extends AbstractStateTableSnapshot<K, N, S, NestedMapsStateTable<K, N, S>>
			implements StateSnapshot.StateKeyGroupWriter {
		private final TypeSerializer<K> keySerializer;
		private final TypeSerializer<N> namespaceSerializer;
		private final TypeSerializer<S> stateSerializer;
		private final StateSnapshotTransformer<S> snapshotFilter;

		NestedMapsStateTableSnapshot(NestedMapsStateTable<K, N, S> owningTable, StateSnapshotTransformer<S> snapshotFilter) {
			super(owningTable);
			this.snapshotFilter = snapshotFilter;
			this.keySerializer = owningStateTable.keyContext.getKeySerializer();
			this.namespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer();
			this.stateSerializer = owningStateTable.metaInfo.getStateSerializer();
		}

		@Nonnull
		@Override
		public StateKeyGroupWriter getKeyGroupWriter() {
			return this;
		}

		@Nonnull
		@Override
		public StateMetaInfoSnapshot getMetaInfoSnapshot() {
			return owningStateTable.metaInfo.snapshot();
		}

		/**
		 * Implementation note: we currently chose the same format between {@link NestedMapsStateTable} and
		 * {@link CopyOnWriteStateTable}.
		 *
		 * <p>{@link NestedMapsStateTable} could naturally support a kind of
		 * prefix-compressed format (grouping by namespace, writing the namespace only once per group instead for each
		 * mapping). We might implement support for different formats later (tailored towards different state table
		 * implementations).
		 */
		@Override
		public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId) throws IOException {
			final Map<N, Map<K, S>> keyGroupMap = owningStateTable.getMapForKeyGroup(keyGroupId);
			if (null != keyGroupMap) {
				Map<N, Map<K, S>> filteredMappings = filterMappingsInKeyGroupIfNeeded(keyGroupMap);
				dov.writeInt(countMappingsInKeyGroup(filteredMappings));
				for (Map.Entry<N, Map<K, S>> namespaceEntry : filteredMappings.entrySet()) {
					final N namespace = namespaceEntry.getKey();
					final Map<K, S> namespaceMap = namespaceEntry.getValue();
					for (Map.Entry<K, S> keyEntry : namespaceMap.entrySet()) {
						writeElement(namespace, keyEntry, dov);
					}
				}
			} else {
				dov.writeInt(0);
			}
		}

		private void writeElement(N namespace, Map.Entry<K, S> keyEntry, DataOutputView dov) throws IOException {
			namespaceSerializer.serialize(namespace, dov);
			keySerializer.serialize(keyEntry.getKey(), dov);
			stateSerializer.serialize(keyEntry.getValue(), dov);
		}

		private Map<N, Map<K, S>> filterMappingsInKeyGroupIfNeeded(final Map<N, Map<K, S>> keyGroupMap) {
			return snapshotFilter == null ?
				keyGroupMap : filterMappingsInKeyGroup(keyGroupMap);
		}

		private Map<N, Map<K, S>> filterMappingsInKeyGroup(final Map<N, Map<K, S>> keyGroupMap) {
			Map<N, Map<K, S>> filtered = new HashMap<>();
			for (Map.Entry<N, Map<K, S>> namespaceEntry : keyGroupMap.entrySet()) {
				N namespace = namespaceEntry.getKey();
				Map<K, S> filteredNamespaceMap = filtered.computeIfAbsent(namespace, n -> new HashMap<>());
				for (Map.Entry<K, S> keyEntry : namespaceEntry.getValue().entrySet()) {
					K key = keyEntry.getKey();
					S transformedvalue = snapshotFilter.filterOrTransform(keyEntry.getValue());
					if (transformedvalue != null) {
						filteredNamespaceMap.put(key, transformedvalue);
					}
				}
			}
			return filtered;
		}
	}
}
