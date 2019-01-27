/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.internal;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Base class for state tables.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S> {

	/**
	 * backend this state table belongs to.
	 */
	AbstractInternalStateBackend internalStateBackend;

	/**
	 * Whether this table has namespace.
	 */
	protected final boolean usingNamespace;

	protected RegisteredStateMetaInfo stateMetaInfo;

	public StateTable(
		AbstractInternalStateBackend internalStateBackend,
		RegisteredStateMetaInfo stateMetaInfo,
		boolean usingNamespace
		) {
		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.stateMetaInfo = stateMetaInfo;
		this.usingNamespace = usingNamespace;
	}

	// Main interface methods of StateTable -------------------------------------------------------

	/**
	 * Returns whether this {@link StateTable} is empty.
	 *
	 * @return {@code true} if this {@link StateTable} has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the total number of entries in this {@link StateTable}.
	 *
	 * @return the number of entries in this {@link StateTable}.
	 */
	public abstract int size();

	/**
	 * Returns the state for the composite of active key and given namespace.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace.
	 * @return the state of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(K key, N namespace);

	/**
	 * Returns the stream of all keys for the given namespace.
	 *
	 * @param namespace the namespace. Not null.
	 * @return the stream of all keys for the given namespace.
	 */
	public abstract Stream<K> getKeys(N namespace);

	/**
	 * Maps the composite of active key and given namespace to the specified state. This method should be preferred
	 * over {@link #putAndGetOld(K, N, S)} (Key, Namespace, State)} when the caller is not interested in the old state.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param state     the state. Can be null.
	 */
	public abstract void put(K key, N namespace, S state);

	/**
	 * Maps the composite of active key and given namespace to the specified state. Returns the previous state that
	 * was registered under the composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param state     the state. Can be null.
	 * @return the state of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public abstract S putAndGetOld(K key, N namespace, S state);

	/**
	 * Returns whether this table contains a mapping for the composite of active key and given namespace.
	 *
	 * @param key       the key in the composite key to search for. Not null.
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public abstract boolean containsKey(K key, N namespace);

	/**
	 * Removes the mapping for the composite of active key and given namespace. This method should be preferred
	 * over {@link #removeAndGetOld(K, N)} when the caller is not interested in the old state.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return          {@code true} if the key is removed successfully, {@code false}
	 * otherwise. If the key does not exist before, {@code false} is returned.
	 */
	public abstract boolean remove(K key, N namespace);

	/**
	 * Removes the mapping for the composite of active key and given namespace, returning the state that was
	 * found under the entry.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the state of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public abstract S removeAndGetOld(K key, N namespace);

	/**
	 * Gets all mappings for the key when this state has namespace.
	 *
	 * @param key the key to get.
	 */
	public abstract Map<N, S> getAll(K key);

	/**
	 * Removes the mapping for the key when this state has namespace.
	 *
	 * @param key the key of the mapping to remove. Not null.
	 */
	public abstract void removeAll(K key);

	/**
	 * Removes all mappings in the state table.
	 */
	public abstract void removeAll();

	/**
	 * Returns an iterator over all mappings when the state does not have namespace.
	 *
	 * @return An iterator over all mappings.
	 */
	public abstract Iterator<Map.Entry<K, S>> entryIterator();

	/**
	 * Returns an iterator over all namespaces for the key.
	 *
	 * @return An iterator over all namespaces for the key.
	 */
	public abstract Iterator<N> namespaceIterator(K key);

	/**
	 * Applies the given {@link StateTransformationFunction} to the state (1st input argument), using the given value as
	 * second input argument. The result of {@link StateTransformationFunction#apply(Object, Object)} is then stored as
	 * the new state. This function is basically an optimization for get-update-put pattern.
	 *
	 * @param key            the key. Not null.
	 * @param namespace      the namespace. Not null.
	 * @param value          the value to use in transforming the state. Can be null.
	 * @param transformation the transformation function.
	 * @throws Exception if some exception happens in the transformation function.
	 */
	public abstract <T> void transform(
		K key,
		N namespace,
		T value,
		StateTransformationFunction<S, T> transformation) throws Exception;

	// Meta data setter / getter and toString -----------------------------------------------------

	public AbstractInternalStateBackend getStateBackend() {
		return internalStateBackend;
	}

	public TypeSerializer<K> getKeySerializer() {
		return stateMetaInfo.getKeySerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return stateMetaInfo.getNamespaceSerializer();
	}

	public TypeSerializer<S> getStateSerializer() {
		return stateMetaInfo.getValueSerializer();
	}

	public void setStateMetaInfo(RegisteredStateMetaInfo stateMetaInfo) {
		this.stateMetaInfo = stateMetaInfo;
	}

	public boolean isUsingNamespace() {
		return usingNamespace;
	}

	// Snapshot / Restore -------------------------------------------------------------------------

	public abstract StateTableSnapshot createSnapshot();

	// For testing --------------------------------------------------------------------------------

	@VisibleForTesting
	public abstract int sizeOfNamespace(Object namespace);
}
