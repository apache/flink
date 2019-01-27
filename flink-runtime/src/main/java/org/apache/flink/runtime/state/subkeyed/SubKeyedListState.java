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
import java.util.List;

/**
 * The interface for {@link SubKeyedState} whose values are lists of elements.
 *
 * @param <K> The type of the keys in the list state.
 * @param <N> The type of the namespaces in the list state.
 * @param <E> The type of the elements in the list state.
 */
public interface SubKeyedListState<K, N, E> extends SubKeyedState<K, N, List<E>> {

	/**
	 * Adds a new element into the list under the given key and namespace.
	 *
	 * @param key The key under which the list is to be updated.
	 * @param namespace The namespace of the list to be updated.
	 * @param element The element to be added.
	 */
	void add(K key, N namespace, E element);

	/**
	 * Adds all given elements into the list under the given key and namespace.
	 *
	 * @param key The key under which the list is to be updated.
	 * @param namespace The namespace of the list to be updated.
	 * @param elements The elements to be added.
	 */
	void addAll(K key, N namespace, Collection<? extends E> elements);

	/**
	 * Set the value under the given key and namesapce to element.
	 *
	 * @param key The key under which the list is to be updated.
	 * @param namespace The namespace of the list to be updated.
	 * @param element The element to be set.
	 */
	void put(K key, N namespace, E element);

	/**
	 * Sets the state under the given key and namespace to elements as list style.
	 *
	 * @param key The key under which the list is to be updated.
	 * @param namespace The namespace of the list to be updated.
	 * @param elements The elements to be set.
	 */
	void putAll(K key, N namespace, Collection<? extends E> elements);

	/**
	 * Removes one occurrence of the given element from the list under the given
	 * key and namespace, if it is present.
	 *
	 * @param key The key under which the list is to be updated.
	 * @param namespace The namespace of the list to be updated.
	 * @param element The element to be removed.
	 * @return True if the element to be removed exists
	 * 		   under the specified key and namespace.
	 */
	boolean remove(K key, N namespace, E element);

	/**
	 * Removes all the elements in the given list from the list under the given
	 * key and namespace. If an element appears more than once in the list, all
	 * its occurrences will be removed. The removal of the elements is atomic,
	 * i.e., exceptions will be thrown if some of them fail to be removed from
	 * the state.
	 *
	 * @param key The key under which the given namespace is to be updated.
	 * @param namespace The namespace whose list is to be updated.
	 * @param elements The elements to be removed.
	 * @return True if this state changed as a result of the call.
	 */
	boolean removeAll(K key, N namespace, Collection<? extends E> elements);

	/**
	 * Retrieves and removes the head of the list under the given key,
	 * or returns {@code null} if the list is empty.
	 *
	 * @param key The key under which the elements are to be retrieved.
	 * @param namespace The namespace of the list to be retrieved.
	 * @return The head of the list under the given key.
	 */
	E poll(K key, N namespace);

	/**
	 * Retrieves, but does not remove, the head of the list under the given key,
	 * or returns {@code null} if the list is empty.
	 *
	 * @param key The key under which the elements are to be retrieved.
	 * @param namespace The namespace of the list to be retrieved.
	 * @return The head of the list under the given key.
	 */
	E peek(K key, N namespace);
}
