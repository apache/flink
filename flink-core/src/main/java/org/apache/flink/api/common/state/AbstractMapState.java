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

package org.apache.flink.api.common.state;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Base interface for {@link State} whose values are a collection of key-value
 * pairs.
 *
 * @param <K> Type of the keys in the state.
 * @param <V> Type of the values in the state.
 * @param <M> Type of the map in the state.
 */
public interface AbstractMapState<K, V, M extends Map<K, V>> extends State {

	/**
	 * Returns true if there does not exist any mapping in the map state.
	 *
	 * @return True if there does not exist any mapping in the map state.
	 */
	boolean isEmpty();

	/**
	 * Returns true if there exists the mapping for the given key.
	 *
	 * @param key The key of the mapping to be tested.
	 * @return True if there exists a mapping whose key equals to the given key.
	 */
	boolean contains(K key);

	/**
	 * Returns the current value associated with the given key.
	 *
	 * @param key The key of the mapping.
	 * @return The value of the mapping with the given key.
	 */
	V get(K key);

	/**
	 * Returns the current value associated with the given key, or
	 * {@code defaultValue} if the key does not exist in the state.
	 *
	 * @param key The key whose associated value is to be retrieved.
	 * @param defaultValue The default value associated with the key.
	 * @return The current value associated with the given key, or
	 *         {@code defaultValue} if the key does not exist in the state.
	 */
	V getOrDefault(K key, V defaultValue);

	/**
	 * Returns all the mappings with the given keys in the state.
	 *
	 * @param keys The keys of the mappings to be retrieved.
	 * @return The mappings with the given keys in the state.
	 */
	Map<K, V> getAll(Collection<? extends K> keys);

	/**
	 * Associates a new value with the given key.
	 *
	 * @param key The key of the mapping.
	 * @param value The new value of the mapping.
	 */
	void put(K key, V value);

	/**
	 * Copies all of the mappings from the given map into the state. The
	 * addition of the mappings is atomic, i.e., exceptions will be thrown if
	 * some of them fail to be added into the map state.
	 *
	 * @param map The mappings to be stored in the map state.
	 */
	void putAll(Map<K, V> map);

	/**
	 * Deletes the mapping of the given key.
	 *
	 * @param key The key of the mapping
	 */
	void remove(K key);

	/**
	 * Removes all the mappings with given keys from the state. The removal
	 * of the mappings is atomic, i.e., exceptions will be thrown if some of
	 * them fail to be removed from the map state.
	 */
	void removeAll(Collection<? extends K> keys);

	/**
	 * Returns an iterable view of the mappings in the state.
	 *
	 * @return An iterable view of the mappings in the state.
	 */
	Iterable<Map.Entry<K, V>> entries();

	/**
	 * Returns an iterable view of the keys in the state.
	 *
	 * @return An iterable view of the keys in the state.
	 */
	Iterable<K> keys();

	/**
	 * Returns an iterable view of the values in the state.
	 *
	 * @return An iterable view of the values in the state.
	 */
	Iterable<V> values();

	/**
	 * Returns the value of the state.
	 *
	 * @return The value of the state.
	 */
	M value();

	/**
	 * Returns an iterator over all the mappings in the state. The iterator is
	 * backed by the state, so changes to the iterator are reflected in the
	 * state, and vice-versa.
	 *
	 * @return An iterator over all the mappings in the state.
	 */
	Iterator<Map.Entry<K, V>> iterator();
}
