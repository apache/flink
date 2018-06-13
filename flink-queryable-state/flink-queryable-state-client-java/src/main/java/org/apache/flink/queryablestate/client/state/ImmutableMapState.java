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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A read-only {@link MapState} that does not allow for modifications.
 *
 * <p>This is the result returned when querying Flink's keyed state using the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client} and
 * providing an {@link MapStateDescriptor}.
 */
public final class ImmutableMapState<K, V> extends ImmutableState implements MapState<K, V> {

	private final Map<K, V> state;

	private ImmutableMapState(final Map<K, V> mapState) {
		this.state = Preconditions.checkNotNull(mapState);
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
	public void putAll(Map<K, V> map) {
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

	/**
	 * Returns all the mappings in the state in a {@link Collections#unmodifiableSet(Set)}.
	 *
	 * @return A read-only iterable view of all the key-value pairs in the state.
	 */
	@Override
	public Iterable<Map.Entry<K, V>> entries() {
		return Collections.unmodifiableSet(state.entrySet());
	}

	/**
	 * Returns all the keys in the state in a {@link Collections#unmodifiableSet(Set)}.
	 *
	 * @return A read-only iterable view of all the keys in the state.
	 */
	@Override
	public Iterable<K> keys() {
		return Collections.unmodifiableSet(state.keySet());
	}

	/**
	 * Returns all the values in the state in a {@link Collections#unmodifiableCollection(Collection)}.
	 *
	 * @return A read-only iterable view of all the values in the state.
	 */
	@Override
	public Iterable<V> values() {
		return Collections.unmodifiableCollection(state.values());
	}

	/**
	 * Iterates over all the mappings in the state. The iterator cannot
	 * remove elements.
	 *
	 * @return A read-only iterator over all the mappings in the state.
	 */
	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return Collections.unmodifiableSet(state.entrySet()).iterator();
	}

	@Override
	public void clear() {
		throw MODIFICATION_ATTEMPT_ERROR;
	}

	@SuppressWarnings("unchecked")
	public static <K, V, T, S extends State> S createState(
		StateDescriptor<S, T> stateDescriptor,
		byte[] serializedState) throws IOException {
		MapStateDescriptor<K, V> mapStateDescriptor = (MapStateDescriptor<K, V>) stateDescriptor;
		final Map<K, V> state = KvStateSerializer.deserializeMap(
			serializedState,
			mapStateDescriptor.getKeySerializer(),
			mapStateDescriptor.getValueSerializer());
		return (S) new ImmutableMapState<>(state);
	}
}
