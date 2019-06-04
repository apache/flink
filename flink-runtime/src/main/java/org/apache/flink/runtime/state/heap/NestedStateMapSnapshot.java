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

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class represents the snapshot of a {@link NestedStateMap}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class NestedStateMapSnapshot<K, N, S>
	extends StateMapSnapshot<K, N, S, NestedStateMap<K, N, S>> {

	/**
	 * Creates a new {@link NestedStateMapSnapshot}.
	 *
	 * @param owningStateMap the {@link NestedStateMap} for which this object represents a snapshot.
	 */
	public NestedStateMapSnapshot(NestedStateMap<K, N, S> owningStateMap) {
		super(owningStateMap);

	}

	@Override
	public StateMapSnapshotIterator<K, N, S> getStateMapSnapshotIterator(
		StateSnapshotTransformer<S> stateSnapshotTransformer) {
		NestedStateMap<K, N, S> stateMap = owningStateMap;
		Map<N, Map<K, S>> mappings = filterMappingsIfNeeded(stateMap.getNamespaceMap(), stateSnapshotTransformer);
		int numberOfEntries = countMappingsInKeyGroup(mappings);
		return new SnapshotIterator<>(mappings, numberOfEntries);
	}

	private Map<N, Map<K, S>> filterMappingsIfNeeded(
		final Map<N, Map<K, S>> keyGroupMap,
		StateSnapshotTransformer<S> stateSnapshotTransformer) {
		if (stateSnapshotTransformer == null) {
			return keyGroupMap;
		}

		Map<N, Map<K, S>> filtered = new HashMap<>();
		for (Map.Entry<N, Map<K, S>> namespaceEntry : keyGroupMap.entrySet()) {
			N namespace = namespaceEntry.getKey();
			Map<K, S> filteredNamespaceMap = filtered.computeIfAbsent(namespace, n -> new HashMap<>());
			for (Map.Entry<K, S> keyEntry : namespaceEntry.getValue().entrySet()) {
				K key = keyEntry.getKey();
				S transformedvalue = stateSnapshotTransformer.filterOrTransform(keyEntry.getValue());
				if (transformedvalue != null) {
					filteredNamespaceMap.put(key, transformedvalue);
				}
			}
			if (filteredNamespaceMap.isEmpty()) {
				filtered.remove(namespace);
			}
		}

		return filtered;
	}

	private static <K, N, S> int countMappingsInKeyGroup(final Map<N, Map<K, S>> keyGroupMap) {
		int count = 0;
		for (Map<K, S> namespaceMap : keyGroupMap.values()) {
			count += namespaceMap.size();
		}

		return count;
	}

	class SnapshotIterator<K, N, S> implements StateMapSnapshotIterator<K, N, S> {

		private final int numberOfEntries;
		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;
		private Map.Entry<N, Map<K, S>> namespace;
		private Iterator<Map.Entry<K, S>> keyValueIterator;
		private final ReusedStateEntry<K, N, S> reusedStateEntry;

		SnapshotIterator(Map<N, Map<K, S>> mappings, int numberOfEntries) {
			this.numberOfEntries = numberOfEntries;
			this.namespaceIterator = mappings.entrySet().iterator();
			this.namespace = null;
			this.keyValueIterator = Collections.emptyIterator();
			this.reusedStateEntry = new ReusedStateEntry<>(null, null, null);
		}

		@Override
		public int size() {
			return numberOfEntries;
		}

		@Override
		public boolean hasNext() {
			return keyValueIterator.hasNext() || namespaceIterator.hasNext();
		}

		@Override
		public StateEntry<K, N, S> next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			if (!keyValueIterator.hasNext()) {
				namespace = namespaceIterator.next();
				keyValueIterator = namespace.getValue().entrySet().iterator();
			}

			Map.Entry<K, S> entry = keyValueIterator.next();

			reusedStateEntry.reuse(entry.getKey(), namespace.getKey(), entry.getValue());
			return reusedStateEntry;
		}
	}

	class ReusedStateEntry<K, N, S> implements StateEntry<K, N, S> {
		private K key;
		private N namespace;
		private S value;

		public ReusedStateEntry(K key, N namespace, S value) {
			reuse(key, namespace, value);
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public N getNamespace() {
			return namespace;
		}

		@Override
		public S getState() {
			return value;
		}

		public void reuse(K key, N namespace, S value) {
			this.key = key;
			this.namespace = namespace;
			this.value = value;
		}
	}
}
