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

package org.apache.flink.runtime.state.keyed;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * The base interface for {@link KeyedState}s whose values are a collection of
 * key-value pairs.
 *
 * @param <K> The type of the keys
 * @param <MK> The type of the keys in the mappings.
 * @param <MV> The type of the values in the mappings.
 * @param <M> The type of the maps under keys
 */
public interface AbstractKeyedMapState<K, MK, MV, M extends Map<MK, MV>> extends KeyedState<K, M> {
	/**
	 * Returns true if there exists a mapping for the mapping key under the
	 * given key.
	 *
	 * @param key The key of the mapping
	 * @return True if there exists a mapping for the mapping key under the
	 *         given key.
	 */
	boolean contains(K key, MK mapKey);

	/**
	 * Returns the current value associated with the mapping key under the
	 * given key.
	 *
	 * @param key The key under which the mapping is to be retrieved.
	 * @param mapKey The mapping key whose value is to be retrieved.
	 * @return The value associated with the mapping key under the given key.
	 */
	MV get(K key, MK mapKey);

	/**
	 * Returns the current value associated with the mapping key under the given
	 * key, or {@code defaultMapValue} if the mapping key does not exist under
	 * the given key.
	 *
	 * @param key The key under which the mapping is to be retrieved.
	 * @param mapKey The mapping key whose value is to be retrieved.
	 * @param defaultMapValue The default value associated with the mapping key
	 *                        under the given key.
	 * @return The value associated with the mapping key under the given key, or
	 *         {@code defaultMapValue} if the mapping key does not exist under
	 *         the given key.
	 */
	MV getOrDefault(K key, MK mapKey, MV defaultMapValue);

	/**
	 * Returns the values associated with the mapping keys under the given key.
	 *
	 * @param key The key under which the mappings are to be retrieved.
	 * @param mapKeys The mapping keys whose values are to be retrieved.
	 * @return The values associated with the mapping keys under the given key.
	 */
	M getAll(K key, Collection<? extends MK> mapKeys);

	/**
	 * Returns the values associated with the mapping keys under corresponding
	 * keys.
	 *
	 * @param mapKeys A map containing the keys and the mapping keys to be
	 *                retrieved.
	 * @return The values associated with the mapping keys under corresponding
	 *         keys.
	 */
	Map<K, M> getAll(Map<K, ? extends Collection<? extends MK>> mapKeys);

	/**
	 * Associates a new value with the mapping key under the given key. If
	 * the mapping already exists under the given key, the mapping's value will
	 * be replaced with the given value.
	 *
	 * @param key The key under which the mapping is to be added.
	 * @param mapKey The mapping key with which the given value is to be
	 *               associated.
	 * @param mapValue The value to be associated with the mapping key.
	 */
	void add(K key, MK mapKey, MV mapValue);

	/**
	 * Adds all the mappings in the given map into the map under the given
	 * key. The addition of the mappings is atomic, i.e., exceptions will be
	 * thrown if some of them fail to be added into the state.
	 *
	 * @param key The key under which the mappings are to be added.
	 * @param mappings The mappings to be added into the state
	 */
	void addAll(K key, Map<? extends MK, ? extends MV> mappings);

	/**
	 * Adds all the mappings in the given map into the maps under corresponding
	 * keys. The addition of the mappings is atomic, i.e., exceptions will be
	 * thrown if some of them fail to be added in to the state.
	 *
	 * @param mappings The mappings to be added into the state.
	 */
	void addAll(Map<? extends K, ? extends Map<? extends MK, ? extends MV>> mappings);

	/**
	 * Removes the mapping with the mapping key from the map under the given
	 * key, if it is present.
	 *
	 * @param key The key under which the mapping is to be removed.
	 * @param mapKey The mapping key whose mapping is to be removed.
	 */
	void remove(K key, MK mapKey);

	/**
	 * Removes all the mappings with mapping keys in the given collection from
	 * the map under the given key. The removal of the mappings is atomic, i.e.,
	 * exceptions will be thrown if some of them fail to be removed from the
	 * state.
	 *
	 * @param key The key under which the mappings are to be removed.
	 * @param mapKeys The mapping keys whose mappings are to be removed.
	 */
	void removeAll(K key, Collection<? extends MK> mapKeys);

	/**
	 * Removes all the mappings with mapping keys in the given map from the maps
	 * under corresponding keys. The removal of the mappings is atomic, i.e.,
	 * exceptions will be thrown if some of them fail to be removed from the
	 * state.
	 *
	 * @param mapKeys The mapping keys whose mappings are to be removed.
	 */
	void removeAll(Map<? extends K, ? extends Collection<? extends MK>> mapKeys);

	/**
	 * Returns an iterator over all the mappings under the given key. The
	 * iterator is backed by the state, so changes to the iterator are reflected
	 * in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @return An iterator over all the mappings in the state.
	 */
	Iterator<Map.Entry<MK, MV>> iterator(K key);

	/**
	 * Returns an iterable over all the mappings under the given key. The
	 * iterable is backend by the state, so changes to the iterable are reflected
	 * in the state, and vice-versa.
	 *
	 * @param key The key whose mappings are to be iterated.
	 * @return An iterable over all the mappings in the state.
	 */
	Iterable<Map.Entry<MK, MV>> entries(K key);

	/**
	 * Returns an iterable over all the mappings' keys under the given key. The
	 * iterable is backed by the state, so changes to the iterable are reflected
	 * in the state, and vice-versa.
	 *
	 * @param key The key whose mappings' keys are to be iterated.
	 * @return An iterable over all the mappings' keys in the state.
	 */
	Iterable<MK> mapKeys(K key);

	/**
	 * Returns an iterable over all the mappings' values under the given key. The
	 * iterable is backed by the state, so change to the iterable are reflected
	 * in the state, and vice-versa.
	 *
	 * @param key The key whose mappings' values are to be iterated.
	 * @return An iterable over all the mappings' values in the state.
	 */
	Iterable<MV> mapValues(K key);
}
