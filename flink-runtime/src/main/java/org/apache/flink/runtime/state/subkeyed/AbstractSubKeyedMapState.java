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

package org.apache.flink.runtime.state.subkeyed;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * The base interface for {@link SubKeyedState}s whose values are a collection of
 * key-value pairs.
 *
 * @param <K> The type of the keys in the state.
 * @param <N> The type of the namespaces in the state.
 * @param <MK> The type of the keys in the mappings.
 * @param <MV> The type of the values in the mappings.
 * @param <M> The type of the maps under keys
 */
interface AbstractSubKeyedMapState<K, N, MK, MV, M extends Map<MK, MV>> extends SubKeyedState<K, N, M> {
	/**
	 * Returns true if there exists a mapping for the mapping key under the
	 * given key and namespace.
	 *
	 * @param key The key under which the mapping's presence is to be tested.
	 * @param namespace The namespace of the mapping to be tested.
	 * @param mapKey The key of the mapping to be tested.
	 * @return True if there exists a mapping for the mapping key under the
	 *         given key and namespace.
	 */
	boolean contains(K key, N namespace, MK mapKey);

	/**
	 * Returns the current value associated with the mapping key under the
	 * given key and namespace.
	 *
	 * @param key The key under which the mapping is to be retrieved.
	 * @param namespace The namespace of the mapping to be retrieved.
	 * @param mapKey The mapping key of the mapping to be retrieved.
	 * @return The value associated with the mapping key under the given key and
	 *         namespace.
	 */
	MV get(K key, N namespace, MK mapKey);

	/**
	 * Returns the current value associated with the mapping key under the
	 * given key and namespace, or {@code defaultMapValue} if the mapping key
	 * does not exist under the given key and namespace.
	 *
	 * @param key The key under which the mapping is to be retrieved.
	 * @param namespace The namespace under which the mapping to be retrieved.
	 * @param mapKey The mapping key of the mapping to be retrieved.
	 * @param defaultMapValue The default value associated with the mapping key.
	 * @return The value associated with the mapping key under the given key and
	 *         namespace, or {@code defaultMapValue} if the mapping key does not
	 *         exist under the given key and namespace.
	 */
	MV getOrDefault(K key, N namespace, MK mapKey, MV defaultMapValue);

	/**
	 * Returns the values associated with the mapping keys under the given key
	 * and namespace.
	 *
	 * @param key The key under which the mappings are to be retrieved.
	 * @param namespace The namespace of the mappings to be retrieved.
	 * @param mapKeys The mapping keys of the mappings to be retrieved.
	 * @return The values associated with the mapping keys under the given key
	 *         and namespace.
	 */
	M getAll(K key, N namespace, Collection<? extends MK> mapKeys);

	/**
	 * Associates a new value with the mapping key under the given key and
	 * namespace. If the mapping already exists under the given key and
	 * namespace, the mapping's value will be replaced with the given value.
	 *
	 * @param key The key under which the mapping is to be added.
	 * @param namespace The namespace of the mapping to be added.
	 * @param mapKey The mapping key with which the given value is to be
	 *               associated.
	 * @param mapValue The value to be associated with the mapping key.
	 */
	void add(K key, N namespace, MK mapKey, MV mapValue);

	/**
	 * Adds all the mappings in the given map into the map under the given
	 * key and namespace. The addition of the mappings is atomic, i.e.,
	 * exceptions will be thrown if some of the mappings fail to be added.
	 *
	 * @param key The key under which the mappings are to be added.
	 * @param namespace The namespace of the mappings to be added.
	 * @param mappings The mappings to be added.
	 */
	void addAll(K key, N namespace, Map<? extends MK, ? extends MV> mappings);

	/**
	 * Removes the mapping with the given mapping key from the map under the given
	 * key and namespace, if it is present.
	 *
	 * @param key The key under which the mapping is to be removed.
	 * @param namespace The namespace of the mapping to be removed.
	 * @param mapKey The mapping key of the mapping to be removed.
	 */
	void remove(K key, N namespace, MK mapKey);

	/**
	 * Removes the mappings with the given mapping keys from the map under the
	 * given key and namespace. The removal of the mappings is atomic, i.e.,
	 * exceptions will be thrown if some of them fail to be removed from the
	 * state.
	 *
	 * @param key The key under which the mappings are to be removed.
	 * @param namespace The namespace of the mappings to be removed.
	 * @param mapKeys The mapping keys of the mappings to be removed.
	 */
	void removeAll(K key, N namespace, Collection<? extends MK> mapKeys);

	/**
	 * Returns an iterator over all the mappings under the given key
	 * and namespace. The iterator is backed by the state, so changes to the
	 * iterator are reflected in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @return An iterator over all the mappings in the state.
	 */
	Iterator<Map.Entry<MK, MV>> iterator(K key, N namespace);

	/**
	 * Returns an iterator over all the mapping under the given key.
	 * The iterator is backed by the state, so changes to the iterator
	 * are reflected in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @param namespace The namespace of the mappings are to be iterated.
	 * @return An iterable over all the mapping in the state, null if there are
	 *         no values in the state.
	 */
	Iterable<Map.Entry<MK, MV>> entries(K key, N namespace);

	/**
	 * Returns an iterator over all the keys under the given key.
	 * The iterator is backed by the state, so changes to the iterator
	 * are reflected in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @param namespace The namespace of the mappings are to be iterated.
	 * @return An iterator over all the keys in the sate, null if there are
	 *         no values in the state.
	 */
	Iterable<MK> keys(K key, N namespace);

	/**
	 * Returns an iterator over all the values under the given key.
	 * The iterator is backed by the state. so changes to the iterator
	 * are reflected in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @param namespace The namespace of the mappings are to be iterated.
	 * @return An iterator over all the values in the state, null if there are
	 *         no values in the state.
	 */
	Iterable<MV> values(K key, N namespace);
}
