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

package org.apache.flink.runtime.state.heap.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
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

	private static final Logger LOG = LoggerFactory.getLogger(NestedMapsStateTable.class);

	/**
	 * Map for holding the actual state objects. The outer array represents the key-groups.
	 * If we have namespace, a nested map will be created which provides an outer scope by
	 * key and an inner scope by namespace.
	 */
	private final Map[] state;

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	/**
	 * Max parallelism.
	 */
	private final int maxParallelism;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@link NestedMapsStateTable} for the given key context and meta info.
	 */
	public NestedMapsStateTable(
		AbstractInternalStateBackend internalStateBackend,
		RegisteredStateMetaInfo stateMetaInfo,
		boolean usingNamespace
	) {
		super(internalStateBackend, stateMetaInfo, usingNamespace);

		KeyGroupRange groups = internalStateBackend.getKeyGroupRange();
		this.keyGroupOffset = groups.getStartKeyGroup();
		this.maxParallelism = internalStateBackend.getNumGroups();
		int numberOfKeyGroups = groups.getNumberOfKeyGroups();

		@SuppressWarnings("unchecked")
		Map[] state = new Map[numberOfKeyGroups];
		this.state = state;
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	/**
	 * Returns the internal data structure.
	 */
	@VisibleForTesting
	public Map[] getState() {
		return state;
	}

	@VisibleForTesting
	Map getMapForKeyGroup(int keyGroupIndex) {
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
	private void setMapForKeyGroup(int keyGroupId, Map map) {
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
		if (usingNamespace) {
			for (Map<K, Map<N, S>> keyMap : state) {
				if (null != keyMap) {
					for (Map<N, S> namspaceMap : keyMap.values()) {
						if (null != namspaceMap) {
							count += namspaceMap.size();
						}
					}
				}
			}
		} else {
			for (Map<K, S> keyMap : state) {
				if (null != keyMap) {
					count += keyMap.size();
				}
			}
		}
		return count;
	}

	@Override
	public S get(K key, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);
		Object lastKey = key;

		if (map == null) {
			return null;
		}

		if (usingNamespace) {
			map = (Map) map.get(key);
			lastKey = namespace;
		}

		if (map == null) {
			return null;
		}

		return (S) map.get(lastKey);
	}

	@Override
	public void put(K key, N namespace, S state) {
		putAndGetOld(key, namespace, state);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);
		Object lastKey = key;

		if (map == null) {
			map = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, map);
		}

		if (usingNamespace) {
			Map subMap = (Map) map.get(key);
			if (subMap == null) {
				subMap = new HashMap();
				map.put(key, subMap);
			}
			map = subMap;
			lastKey = namespace;
		}

		return (S) map.put(lastKey, state);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);
		Object lastKey = key;

		if (map == null) {
			return false;
		}

		if (usingNamespace) {
			map = (Map) map.get(key);
			lastKey = namespace;
		}

		return map != null && map.containsKey(lastKey);
	}

	@Override
	public boolean remove(K key, N namespace) {
		return removeAndGetOld(key, namespace) != null;
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);
		Map firstMap = map;
		Object lastKey = key;

		if (map == null) {
			return null;
		}

		if (usingNamespace) {
			map = (Map) map.get(key);
			lastKey = namespace;
		}

		if (map == null) {
			return null;
		}

		S removed = (S) map.remove(lastKey);

		if (usingNamespace && map.isEmpty()) {
			firstMap.remove(key);
		}

		return removed;
	}

	@Override
	public Map<N, S> getAll(K key) {
		if (!usingNamespace) {
			throw new UnsupportedOperationException("This method should be called with namespace supported");
		}

		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);

		if (map == null) {
			return Collections.emptyMap();
		}

		Map<N, S> result = (Map<N, S>) map.get(key);
		return result == null ? Collections.emptyMap() : result;
	}

	@Override
	public void removeAll(K key) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);

		if (map != null) {
			map.remove(key);
		}
	}

	@Override
	public void removeAll() {
		for (int i = 0; i < state.length; i++) {
			Map map = state[i];
			if (map != null) {
				map.clear();
			}
		}
	}

	// ------------------------------------------------------------------------

	private int getKeyGroupIndex(K key) {
		return KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
	}

	private void checkKeyNamespacePreconditions(K key, N namespace) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		if (usingNamespace) {
			Preconditions.checkNotNull(namespace, "Provided namespace is null.");
		}
	}

	@Override
	public <T> void transform(K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		checkKeyNamespacePreconditions(key, namespace);

		final int keyGroupIndex = getKeyGroupIndex(key);
		Map map = getMapForKeyGroup(keyGroupIndex);
		Object lastKey = key;

		if (map == null) {
			map = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, map);
		}

		if (usingNamespace) {
			Map subMap = (Map) map.get(key);
			if (subMap == null) {
				subMap = new HashMap<>();
				map.put(key, subMap);
			}
			map = subMap;
			lastKey = namespace;
		}

		map.put(lastKey, transformation.apply((S) map.get(lastKey), value));
	}

	// Iteration  ------------------------------------------------------------------------------------------------------

	@Override
	public Stream<K> getKeys(N namespace) {
		if (usingNamespace) {
			Set<K> keys = new HashSet<>();
			for (Map<K, Map<N, S>> map : state) {
				if (map != null) {
					for (K key : map.keySet()) {
						if (map.getOrDefault(key, Collections.emptyMap()).containsKey(namespace)) {
							keys.add(key);
						}
					}
				}
			}
			return keys.stream();
		} else {
			return Arrays.stream(state)
				.filter(Objects::nonNull)
				.flatMap(namespaceSate -> namespaceSate.keySet().stream());
		}
	}

	@Override
	public Iterator<Map.Entry<K, S>> entryIterator() {
		if (usingNamespace) {
			throw new UnsupportedOperationException("This method should be called with no namespace");
		}

		return new MultipleMapIterator<>((Map<K, S>[]) state);
	}

	@Override
	public Iterator<N> namespaceIterator(K key) {
		if (!usingNamespace) {
			throw new UnsupportedOperationException("This method should be called with namespace supported");
		}

		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		int keyGroupIndex = getKeyGroupIndex(key);
		Map keyMap = getMapForKeyGroup(keyGroupIndex);

		if (keyMap == null) {
			return Collections.emptyIterator();
		}

		Map namespaceMap = (Map) keyMap.get(key);

		return namespaceMap == null ? Collections.emptyIterator() : namespaceMap.keySet().iterator();
	}

	// snapshots ---------------------------------------------------------------------------------------------------

	private static int countMappingsInKeyGroupWithNamespace(final Map<?, Map<?, ?>> keyGroupMap) {
		int count = 0;
		for (Map namespaceMap : keyGroupMap.values()) {
			count += namespaceMap.size();
		}

		return count;
	}

	private static int countMappingsInKeyGroup(final Map keyGroupMap) {
		return keyGroupMap.size();
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		Preconditions.checkState(isUsingNamespace());
		Preconditions.checkNotNull(namespace);

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
	public NestedMapsStateTableSnapshot<K, N, S> createSnapshot() {
		return new NestedMapsStateTableSnapshot<>(this);
	}

	/**
	 * This class encapsulates the snapshot logic.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class NestedMapsStateTableSnapshot<K, N, S>
		extends AbstractStateTableSnapshot<K, N, S, NestedMapsStateTable<K, N, S>> {

		NestedMapsStateTableSnapshot(NestedMapsStateTable<K, N, S> owningTable) {
			super(owningTable);
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
		public int writeMappingsInKeyGroup(DataOutputView dov, int keyGroupId) throws IOException {
			final Map keyGroupMap = owningStateTable.getMapForKeyGroup(keyGroupId);

			if (keyGroupMap == null) {
				dov.writeInt(0);
				return 0;
			}

			TypeSerializer<K> keySerializer = owningStateTable.getKeySerializer();
			TypeSerializer<N> namespaceSerializer = owningStateTable.getNamespaceSerializer();
			TypeSerializer<S> stateSerializer = owningStateTable.getStateSerializer();
			boolean usingNamespace = owningStateTable.isUsingNamespace();

			int countMappings = usingNamespace ? countMappingsInKeyGroupWithNamespace(keyGroupMap) : countMappingsInKeyGroup(keyGroupMap);
			dov.writeInt(countMappings);

			if (usingNamespace) {
				for (Map.Entry<K, Map<N, S>> entry : ((Map<K, Map<N, S>>) keyGroupMap).entrySet()) {
					final K key = entry.getKey();
					final Map<N, S> namespaceMap = entry.getValue();

					for (Map.Entry<N, S> namespaceEntry : namespaceMap.entrySet()) {
						keySerializer.serialize(key, dov);
						namespaceSerializer.serialize(namespaceEntry.getKey(), dov);
						stateSerializer.serialize(namespaceEntry.getValue(), dov);
					}
				}
			} else {
				for (Map.Entry<K, S> entry : ((Map<K, S>) keyGroupMap).entrySet()) {
					keySerializer.serialize(entry.getKey(), dov);
					stateSerializer.serialize(entry.getValue(), dov);
				}
			}

			return countMappings;
		}
	}

	private static class MultipleMapIterator<K, S> implements Iterator<Map.Entry<K, S>> {

		private final Map<K, S>[] state;

		private int nextIndex;

		private Iterator<Map.Entry<K, S>> currentIterator;

		public MultipleMapIterator(Map<K, S>[] state) {
			this.state = Preconditions.checkNotNull(state);
			this.nextIndex = 0;
			this.currentIterator = Collections.emptyIterator();
		}

		@Override
		public boolean hasNext() {
			if (currentIterator.hasNext()) {
				return true;
			}

			Map<K, S>[] localState = state;
			int localNextIndex = nextIndex;
			int len = localState.length;
			while (localNextIndex < len) {
				Map<K, S> map = localState[localNextIndex];
				if (map != null && !map.isEmpty()) {
					currentIterator = map.entrySet().iterator();
					nextIndex = localNextIndex + 1;
					return true;
				}
				localNextIndex++;
			}

			nextIndex = localNextIndex;

			return false;
		}

		@Override
		public Map.Entry<K, S> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			return currentIterator.next();
		}
	}
}
