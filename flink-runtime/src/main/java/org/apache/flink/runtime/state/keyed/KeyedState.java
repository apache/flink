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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateStorage;

import java.util.Collection;
import java.util.Map;

/**
 * Base interface for all keyed states.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the values in the state.
 */
public interface KeyedState<K, V> {

	/**
	 * Returns the descriptor of the state.
	 *
	 * @return The descriptor of the state.
	 */
	KeyedStateDescriptor getDescriptor();

	/**
	 * Returns true if the state contains a pair whose key is equal to the given
	 * object.
	 *
	 * @param key The key whose presence in the state to be tested.
	 * @return True if the state contains the pair for the given key.
	 */
	boolean contains(K key);

	/**
	 * Returns the value associated with the key in the state.
	 *
	 * @param key The key whose value is to be retrieved.
	 * @return The value associated with the key in the state.
	 */
	V get(K key);

	/**
	 * Returns the value associated with the key in the state, or
	 * {@code defaultValue} if the key does not exist in the state.
	 *
	 * @param key The key whose associated value is to be returned.
	 * @param defaultValue The default value of the key.
	 * @return The value to which the specified key is mapped, or
	 *         {@code defaultValue} if the key does not exist in the state.
	 */
	V getOrDefault(K key, V defaultValue);

	/**
	 * Returns the values associated with the given keys in the state.
	 *
	 * @param keys The keys whose values are to be retrieved.
	 * @return The values associated with the keys in the state.
	 */
	Map<K, V> getAll(Collection<? extends K> keys);

	/**
	 * Removes the pair for the given key from the state if it is present.
	 *
	 * @param key The key whose pair is to be removed from the state.
	 */
	void remove(K key);

	/**
	 * Removes all of the pairs whose keys appear in the given collection from
	 * the state. The removal of these pairs are atomic, i.e., exceptions will
	 * be thrown if some of the pairs fail to be removed.
	 *
	 * @param keys The keys of the pairs to be removed from the state.
	 */
	void removeAll(Collection<? extends K> keys);

	/**
	 * Returns all the key/value map in the state.
	 *
	 * @return The key/value map in the state.
	 */
	Map<K, V> getAll();

	/**
	 * Remove all the items in the state.
	 */
	void removeAll();

	/**
	 * Returns an iterable over all of the keys in the state.
	 *
	 * @return The iterable of all the keys in the state.
	 */
	Iterable<K> keys();

	/**
	 * Returns the serialized value for the given key.
	 *
	 * <p>If no value is associated with key, <code>null</code> is returned.
	 *
	 * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything
	 * stateful (e.g. serializers) should be either duplicated or protected from undesired
	 * consequences of concurrent invocations.
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded context
	 * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded context
	 * @return Serialized value or <code>null</code> if no value is associated with the key.
	 * @throws Exception Exceptions during serialization are forwarded
	 */
	byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<V> safeValueSerializer) throws Exception;

	/**
	 * Returns the state storage within this keyed state.
	 *
	 * @return The state storage within this keyed state.
	 */
	StateStorage<K, V> getStateStorage();

}
