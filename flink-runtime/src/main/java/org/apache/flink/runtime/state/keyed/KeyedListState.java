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
import java.util.List;
import java.util.Map;

/**
 * The interface for {@link KeyedState} whose values are lists of elements.
 *
 * @param <K> The type of the keys in the list state.
 * @param <E> The type of the elements in the list state.
 */
public interface KeyedListState<K, E> extends KeyedState<K, List<E>> {

	/**
	 * Adds a new element into the list under the given key.
	 *
	 * @param key The key whose list is to be updated.
	 * @param element The element to be added.
	 */
	void add(K key, E element);

	/**
	 * Adds all the elements in given list into the list under the given key.
	 * The addition of the elements is atomic, i.e., exceptions will be thrown
	 * if some of them fail to be added into the state.
	 *
	 * @param key The key whose list is to be updated.
	 * @param elements The elements to be added into the state.
	 */
	void addAll(K key, Collection<? extends E> elements);

	/**
	 * Adds all the elements in the given map into the lists under corresponding
	 * keys. The addition of the elements is atomic, i.e., exceptions will be
	 * thrown if some of them fail to be added into the state.
	 *
	 * @param elements The elements to be added into the state.
	 */
	void addAll(Map<? extends K, ? extends Collection<? extends E>> elements);

	/**
	 * Set the value of the given key to {@code Arrays.asList(element)}.
	 *
	 * @param key The key whose list is to be updated.
	 * @param element The elements to be set to the state.
	 */
	void put(K key, E element);

	/**
	 * Set the value of the given key to elements.
	 *
	 * @param key The key whose list is to be updated.
	 * @param elements The elements to be set to the state.
	 */
	void putAll(K key, Collection<? extends E> elements);

	/**
	 * Set all the elements in the given map into the lists under corresponding
	 * keys. The addition of the elements is atomic, i.e., exceptions will be
	 * throw if some of them fail to be added into the state.
	 *
	 * @param elements The elements to be added into the state.
	 */
	void putAll(Map<? extends K, ? extends Collection<? extends E>> elements);

	/**
	 * Removes one occurrence of the given element from the list under the given
	 * key, if it is present.
	 *
	 * @param key The key whose list is to be updated.
	 * @param element The element to be removed from the state.
	 * @return True if the element to be removed exists under the key.
	 */
	boolean remove(K key, E element);

	/**
	 * Removes all the elements in the given list from the list under the given
	 * key. If an element appears more than once in the list, all its
	 * occurrences will be removed. The removal of the elements is atomic, i.e.,
	 * exceptions will be thrown if some of them fail to be removed from the
	 * state.
	 *
	 * @param key The key whose list is to be updated.
	 * @param elements The element to be removed from the state.
	 * @return True if this state changed as a result of the call.
	 */
	boolean removeAll(K key, Collection<? extends E> elements);

	/**
	 * Removes all the elements in the given map from the lists under
	 * corresponding keys. If an element appears more than once in its
	 * corresponding key, all its occurrences will be removed. The removal of
	 * the elements is atomic, i.e., exceptions will be thrown if some of them
	 * fail to be removed from the state.
	 *
	 * @param elements The elements to be removed from the state.
	 * @return True if this state changed as a result of the call.
	 */
	boolean removeAll(Map<? extends K, ? extends Collection<? extends E>> elements);

	/**
	 * Retrieves and removes the head of the list under the given key,
	 * or returns {@code null} if the list is empty.
	 *
	 * @param key The key under which the elements are to be retrieved.
	 * @return The head of the list under the given key.
	 */
	E poll(K key);

	/**
	 * Retrieves, but does not remove, the head of the list under the given key,
	 * or returns {@code null} if the list is empty.
	 *
	 * @param key The key under which the elements are to be retrieved.
	 * @return The head of the list under the given key.
	 */
	E peek(K key);
}
