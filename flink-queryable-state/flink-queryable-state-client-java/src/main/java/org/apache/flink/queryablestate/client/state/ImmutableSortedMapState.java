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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A read-only {@link SortedMapState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and
 * providing an {@link MapStateDescriptor}.
 */
@PublicEvolving
public class ImmutableSortedMapState<K, V> extends ImmutableState implements SortedMapState<K, V> {
	private final SortedMap<K, V> state;

	private ImmutableSortedMapState(SortedMap<K, V> maps) {
		state = Preconditions.checkNotNull(maps);
	}

	@Override
	public V get(K key) {
		return state.get(key);
	}

	@Override
	public void put(K key, V value) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void remove(K key) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public boolean contains(K key) {
		return state.containsKey(key);
	}

	@Override
	public Iterable<Map.Entry<K, V>> entries() {
		return Collections.unmodifiableSet(state.entrySet());
	}

	@Override
	public Iterable<K> keys() {
		return Collections.unmodifiableSet(state.keySet());
	}

	@Override
	public Iterable<V> values() {
		return Collections.unmodifiableCollection(state.values());
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator()  {
		return Collections.unmodifiableSet(state.entrySet()).iterator();
	}

	@Override
	public Map.Entry<K, V> firstEntry() {
		return new Map.Entry<K, V>() {
			@Override
			public K getKey() {
				return state.firstKey();
			}

			@Override
			public V getValue() {
				return state.get(state.firstKey());
			}

			@Override
			public V setValue(V value) {
				return state.put(state.firstKey(), value);
			}
		};
	}

	@Override
	public Map.Entry<K, V> lastEntry() {
		return new Map.Entry<K, V>() {
			@Override
			public K getKey() {
				return state.lastKey();
			}

			@Override
			public V getValue() {
				return state.get(state.lastKey());
			}

			@Override
			public V setValue(V value) {
				return state.put(state.lastKey(), value);
			}
		};
	}

	@Override
	public Iterator<Map.Entry<K, V>> headIterator(K endKey) {
		return Collections.unmodifiableSet(state.headMap(endKey).entrySet()).iterator();
	}

	@Override
	public Iterator<Map.Entry<K, V>> tailIterator(K startKey) {
		return Collections.unmodifiableSet(state.tailMap(startKey).entrySet()).iterator();
	}

	@Override
	public Iterator<Map.Entry<K, V>> subIterator(K startKey, K endKey) {
		return Collections.unmodifiableSet(state.subMap(startKey, endKey).entrySet()).iterator();
	}

	@Override
	public void putAll(Map<K, V> map) {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@Override
	public void clear() {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	public static <K, V> ImmutableSortedMapState<K, V> createState(
		final SortedMapStateDescriptor<K, V> stateDescriptor,
		byte[] serializedState
		) throws IOException {
		final Map<K, V> deserializeMap = KvStateSerializer.deserializeMap(
			serializedState,
			stateDescriptor.getKeySerializer(),
			stateDescriptor.getValueSerializer());
		final SortedMap<K, V> state = new TreeMap<>(deserializeMap);
		return new ImmutableSortedMapState<>(state);
	}
}
