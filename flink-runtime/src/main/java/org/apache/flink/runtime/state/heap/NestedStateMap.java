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
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * This implementation of {@link StateMap} uses nested {@link HashMap} objects.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of value.
 */
public class NestedStateMap<K, N, S> extends StateMap<K, N, S> {

	/**
	 * Map for holding the actual state objects. The nested map provide
	 * an outer scope by namespace and an inner scope by key.
	 */
	private final Map<N, Map<K, S>> namespaceMap;

	/**
	 * Constructs a new {@code NestedStateMap}.
	 */
	public NestedStateMap() {
		this.namespaceMap = new HashMap<>();
	}

	// Public API from StateMap ------------------------------------------------------------------------------

	@Override
	public int size() {
		int count = 0;
		for (Map<K, S> keyMap : namespaceMap.values()) {
			if (null != keyMap) {
				count += keyMap.size();
			}
		}

		return count;
	}

	@Override
	public S get(K key, N namespace) {
		Map<K, S> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		Map<K, S> keyedMap = namespaceMap.get(namespace);

		return keyedMap != null && keyedMap.containsKey(key);
	}

	@Override
	public void put(K key, N namespace, S state) {
		putAndGetOld(key, namespace, state);
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> new HashMap<>());

		return keyedMap.put(key, state);
	}

	@Override
	public void remove(K key, N namespace) {
		removeAndGetOld(key, namespace);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {
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

	@Override
	public <T> void transform(
		K key, N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		Map<K, S> keyedMap = namespaceMap.computeIfAbsent(namespace, k -> new HashMap<>());
		keyedMap.put(key, transformation.apply(keyedMap.get(key), value));
	}

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return namespaceMap.getOrDefault(namespace, Collections.emptyMap()).keySet().stream();

	}

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
		int recommendedMaxNumberOfReturnedRecords) {
		return new StateEntryVisitor();
	}

	@Override
	public int sizeOfNamespace(Object namespace) {
		Map<K, S> keyMap = namespaceMap.get(namespace);
		return keyMap != null ? keyMap.size() : 0;
	}

	@Nonnull
	@Override
	public StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateSnapshot() {
		return new NestedStateMapSnapshot<>(this);
	}

	public Map<N, Map<K, S>> getNamespaceMap() {
		return namespaceMap;
	}

	/**
	 * Iterator over state entries in a {@link NestedStateMap}.
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {
		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;
		private Map.Entry<N, Map<K, S>> namespace;
		private Iterator<Map.Entry<K, S>> keyValueIterator;

		StateEntryIterator() {
			namespaceIterator = namespaceMap.entrySet().iterator();
			namespace = null;
			keyValueIterator = Collections.emptyIterator();
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

			return new StateEntry.SimpleStateEntry<>(
				entry.getKey(), namespace.getKey(), entry.getValue());
		}
	}


	/**
	 * Incremental visitor over state entries in a {@link NestedStateMap}.
	 *
	 * <p>The iterator keeps a snapshotted copy of key/namespace sets, available at the beginning of iteration.
	 * While further iterating the copy, the iterator returns the actual state value from primary maps
	 * if exists at that moment.
	 *
	 * <p>Note: Usage of this iterator can have a heap memory consumption impact.
	 */
	class StateEntryVisitor implements InternalKvState.StateIncrementalVisitor<K, N, S>, Iterator<StateEntry<K, N, S>> {
		private Iterator<Map.Entry<N, Map<K, S>>> namespaceIterator;
		private Map.Entry<N, Map<K, S>> namespace;
		private Iterator<Map.Entry<K, S>> keyValueIterator;
		private StateEntry<K, N, S> nextEntry;
		private StateEntry<K, N, S> lastReturnedEntry;

		StateEntryVisitor() {
			namespaceIterator = new HashSet<>(namespaceMap.entrySet()).iterator();
			namespace = null;
			keyValueIterator = null;
			nextKeyIterator();
		}

		@Override
		public boolean hasNext() {
			nextKeyIterator();
			return keyIteratorHasNext();
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			StateEntry<K, N, S> nextEntry = next();
			return nextEntry == null ? Collections.emptyList() : Collections.singletonList(nextEntry);
		}

		@Override
		public StateEntry<K, N, S> next() {
			StateEntry<K, N, S> next = null;
			if (hasNext()) {
				next = nextEntry;
			}
			nextEntry = null;
			lastReturnedEntry = next;
			return next;
		}

		private void nextKeyIterator() {
			while (!keyIteratorHasNext()) {
				if (namespaceIteratorHasNext()) {
					namespace = namespaceIterator.next();
					keyValueIterator = new HashSet<>(namespace.getValue().entrySet()).iterator();
				} else {
					break;
				}
			}
		}

		private boolean keyIteratorHasNext() {
			while (nextEntry == null && keyValueIterator != null && keyValueIterator.hasNext()) {
				Map.Entry<K, S> next = keyValueIterator.next();
				Map<K, S> ns = namespaceMap.getOrDefault(namespace.getKey(), null);
				S upToDateValue = ns == null ? null : ns.getOrDefault(next.getKey(), null);
				if (upToDateValue != null) {
					nextEntry = new StateEntry.SimpleStateEntry<>(next.getKey(), namespace.getKey(), upToDateValue);
				}
			}
			return nextEntry != null;
		}

		private boolean namespaceIteratorHasNext() {
			return namespaceIterator.hasNext();
		}

		@Override
		public void remove() {
			remove(lastReturnedEntry);
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			namespaceMap.get(stateEntry.getNamespace()).remove(stateEntry.getKey());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			namespaceMap.get(stateEntry.getNamespace()).put(stateEntry.getKey(), newValue);
		}
	}
}
