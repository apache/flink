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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.stream.Stream;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key, as provided
 * through the {@link InternalKeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S> implements StateSnapshotRestore {

	/**
	 * The key context view on the backend. This provides information, such as the currently active key.
	 */
	protected final InternalKeyContext<K> keyContext;

	/**
	 * Combined meta information such as name and serializers for this state.
	 */
	protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

	/**
	 *
	 * @param keyContext the key context provides the key scope for all put/get/delete operations.
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 */
	public StateTable(InternalKeyContext<K> keyContext, RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.metaInfo = Preconditions.checkNotNull(metaInfo);
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
	 * Returns the total number of entries in this {@link StateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link StateTable}.
	 */
	public abstract int size();

	/**
	 * Returns the state of the mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace. Not null.
	 * @return the states of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(N namespace);

	/**
	 * Returns whether this table contains a mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public abstract boolean containsKey(N namespace);

	/**
	 * Maps the composite of active key and given namespace to the specified state. This method should be preferred
	 * over {@link #putAndGetOld(N, S)} (Namespace, State)} when the caller is not interested in the old state.
	 *
	 * @param namespace the namespace. Not null.
	 * @param state     the state. Can be null.
	 */
	public abstract void put(N namespace, S state);

	/**
	 * Maps the composite of active key and given namespace to the specified state. Returns the previous state that
	 * was registered under the composite key.
	 *
	 * @param namespace the namespace. Not null.
	 * @param state     the state. Can be null.
	 * @return the state of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public abstract S putAndGetOld(N namespace, S state);

	/**
	 * Removes the mapping for the composite of active key and given namespace. This method should be preferred
	 * over {@link #removeAndGetOld(N)} when the caller is not interested in the old state.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	public abstract void remove(N namespace);

	/**
	 * Removes the mapping for the composite of active key and given namespace, returning the state that was
	 * found under the entry.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the state of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public abstract S removeAndGetOld(N namespace);

	/**
	 * Applies the given {@link StateTransformationFunction} to the state (1st input argument), using the given value as
	 * second input argument. The result of {@link StateTransformationFunction#apply(Object, Object)} is then stored as
	 * the new state. This function is basically an optimization for get-update-put pattern.
	 *
	 * @param namespace      the namespace. Not null.
	 * @param value          the value to use in transforming the state. Can be null.
	 * @param transformation the transformation function.
	 * @throws Exception if some exception happens in the transformation function.
	 */
	public abstract <T> void transform(
			N namespace,
			T value,
			StateTransformationFunction<S, T> transformation) throws Exception;

	// For queryable state ------------------------------------------------------------------------

	/**
	 * Returns the state for the composite of active key and given namespace. This is typically used by
	 * queryable state.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @return the state of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(K key, N namespace);

	public abstract Stream<K> getKeys(N namespace);

	// Meta data setter / getter and toString -----------------------------------------------------

	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// Snapshot / Restore -------------------------------------------------------------------------

	public abstract void put(K key, int keyGroup, N namespace, S state);

	// For testing --------------------------------------------------------------------------------

	@VisibleForTesting
	public abstract int sizeOfNamespace(Object namespace);

	@Nonnull
	@Override
	public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
		return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
	}
}
