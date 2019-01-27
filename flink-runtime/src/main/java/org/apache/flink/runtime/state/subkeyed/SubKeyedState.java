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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateStorage;

import java.util.Iterator;
import java.util.Map;

/**
 * Base interface for all subkeyed states.
 *
 * @param <K> The type of the keys in the state.
 * @param <N> The type of the namespaces in the state.
 * @param <V> The type of the values in the state.
 */
public interface SubKeyedState<K, N, V> {

	/**
	 * Returns the descriptor of the state.
	 *
	 * @return The descriptor of the state.
	 */
	SubKeyedStateDescriptor getDescriptor();

	/**
	 * Returns true if the state contains a value under the given key and the
	 * given namespace.
	 *
	 * @param key The key under which the value's presence is to be tested.
	 * @param namespace The namespace of the value to be tested.
	 * @return True if the state contains a value under the given key and the
	 *         given namespace.
	 */
	boolean contains(K key, N namespace);

	/**
	 * Returns the value under the given key and namespace in the state.
	 *
	 * @param key The key under which the value is to be retrieved.
	 * @param namespace The namespace of the value to be retrieved.
	 * @return The value under the given key and namespace in the state.
	 */
	V get(K key, N namespace);

	/**
	 * Returns the value under the given key and namespace in the state, or
	 * {@code defaultValue} if there does not exist any value under the key
	 * and the namespace in the state.
	 *
	 * @param key The key under which the value is to be retrieved.
	 * @param namespace The namespace of the value to be retrieved.
	 * @param defaultValue The default value under the given key and namespace in the state.
	 * @return The value under the given key and namespace in the state, or
	 *         {@code defaultValue} if there does not exist any value under the
	 *         key and the namespace in the state.
	 */
	V getOrDefault(K key, N namespace, V defaultValue);

	/**
	 * Returns the namespaces and their values under the given key in the state.
	 *
	 * @param key The key under which the namespaces and values are to be
	 *            retrieved.
	 * @return The namespaces and their values in the state.
	 */
	Map<N, V> getAll(K key);

	/**
	 * Removes the given namespace from the given key in the state, if it is
	 * present.
	 *
	 * @param key The key under which the given namespace is to be removed from.
	 * @param namespace The namespace to be removed.
	 */
	void remove(K key, N namespace);

	/**
	 * Removes the given key from the state.
	 *
	 * @param key The key of the namespaces to be removed.
	 */
	void removeAll(K key);

	/**
	 * Return an iterator over the namespaces under the given key.
	 *
	 * @param key The key whose namespaces are to be iterated.
	 * @return An iterator over the namespace under the given key.
	 */
	Iterator<N> iterator(K key);

	/**
	 * Return all the keys which are in the specified namespace.
	 *
	 * @param namespace The namespace which the keys will be retrieved.
	 *
	 * @return All the keys associated with the specified namespace.
	 */
	Iterable<K> keys(N namespace);

	/**
	 * Returns the state storage within this keyed state.
	 *
	 * @return The state storage within this keyed state.
	 */
	StateStorage<K, V> getStateStorage();

	/**
	 * Returns the serialized value for the given key and namespace.
	 *
	 * <p>If no value is associated with key and namespace, <code>null</code>
	 * is returned.
	 *
	 * <p><b>TO IMPLEMENTERS:</b> This method is called by multiple threads. Anything
	 * stateful (e.g. serializers) should be either duplicated or protected from undesired
	 * consequences of concurrent invocations.
	 *
	 * @param serializedKeyAndNamespace Serialized key and namespace
	 * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded context
	 * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in multi-threaded context
	 * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded context
	 * @return Serialized value or <code>null</code> if no value is associated with the key and namespace.
	 *
	 * @throws Exception Exceptions during serialization are forwarded
	 */
	byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<V> safeValueSerializer) throws Exception;
}
