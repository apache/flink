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

import org.apache.flink.api.common.state.MapState;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple wrapper map state that exposes empty state properly as an empty map.
 * 
 * @param <K> The type of keys in the map state.
 * @param <V> The type of values in the map state.
 */
class UserFacingMapState<K, V> implements MapState<K, V> {

	private final MapState<K, V> originalState;

	private final Map<K, V> emptyState = Collections.<K, V>emptyMap();

	UserFacingMapState(MapState<K, V> originalState) {
		this.originalState = originalState;
	}

	// ------------------------------------------------------------------------

	@Override
	public Iterable<Map.Entry<K, V>> get() throws Exception {
		Iterable<Map.Entry<K, V>> original = originalState.get();
		return original != null ? original : emptyState.entrySet();
	}

	@Override
	public void add(Map<K, V> value) throws Exception {
		originalState.add(value);
	}

	@Override
	public void clear() {
		originalState.clear();
	}

	@Override
	public V get(K key) throws IOException {
		return originalState.get(key);
	}

	@Override
	public void put(K key, V value) throws IOException {
		originalState.put(key, value);
	}

	@Override
	public void remove(K key) throws IOException {
		originalState.remove(key);
	}

	@Override
	public boolean contains(K key) throws IOException {
		return originalState.contains(key);
	}

	@Override
	public int size() throws IOException {
		return originalState.size();
	}

	@Override
	public Iterable<K> keys() {
		return originalState.keys();
	}

	@Override
	public Iterable<V> values() {
		return originalState.values();
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return originalState.iterator();
	}
}
