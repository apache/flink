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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key, as provided
 * through the {@link KeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateTable<K, N, S> {

	/**
	 * The key context view on the backend. This provides information, such as the currently active key.
	 */
	protected final KeyContext<K> keyContext;

	/**
	 * Combined meta information such as name and serializers for this state
	 */
	protected RegisteredBackendStateMetaInfo<N, S> metaInfo;

	/**
	 *
	 * @param keyContext the key context provides the key scope for all put/get/delete operations.
	 * @param metaInfo the meta information, including the type serializer for state copy-on-write.
	 */
	public StateTable(KeyContext<K> keyContext, RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.metaInfo = Preconditions.checkNotNull(metaInfo);
	}

	// Main interface methods of StateTable ----------------------------------------------------------------------------

	/**
	 * Returns whether this {@link NestedMapsStateTable} is empty.
	 *
	 * @return {@code true} if this {@link NestedMapsStateTable} has no elements, {@code false}
	 * otherwise.
	 * @see #size()
	 */
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Returns the total number of entries in this {@link NestedMapsStateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link NestedMapsStateTable}.
	 */
	public abstract int size();

	/**
	 * Returns the value of the mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace. Not null.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(Object namespace);

	/**
	 * Returns whether this table contains a mapping for the composite of active key and given namespace.
	 *
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	public abstract boolean containsKey(Object namespace);

	/**
	 * Maps the composite of active key and given namespace to the specified value. This method should be preferred
	 * over {@link #putAndGetOld(Object, Object)} (Object, Object)} when the caller is not interested in the old value.
	 *
	 * @param namespace the namespace. Not null.
	 * @param state     the value. Can be null.
	 */
	public abstract void put(N namespace, S state);

	/**
	 * Maps the composite of active key and given namespace to the specified value. Returns the previous state that
	 * was registered under the composite key.
	 *
	 * @param namespace the namespace. Not null.
	 * @param state     the value. Can be null.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	public abstract S putAndGetOld(N namespace, S state);

	/**
	 * Removes the mapping for the composite of active key and given namespace. This method should be preferred
	 * over {@link #removeAndGetOld(Object)} when the caller is not interested in the old value.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	public abstract void remove(Object namespace);

	/**
	 * Removes the mapping for the composite of active key and given namespace, returning the state that was
	 * found under the entry.
	 *
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	public abstract S removeAndGetOld(Object namespace);

	// For queryable state --------------------------------------------------------------------------

	/**
	 * Returns the value for the composite of active key and given namespace. This is typically used by
	 * queryable state.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @return the value of the mapping with the specified key/namespace composite key, or {@code null}
	 * if no mapping for the specified key is found.
	 */
	public abstract S get(Object key, Object namespace);

	// For efficient restore ------------------------------------------------------------------------

	public abstract void put(K key, int keyGroup, N namespace, S state);

	// Meta data setter / getter and toString --------------------------------------------------------------------------

	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	public RegisteredBackendStateMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	public void setMetaInfo(RegisteredBackendStateMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// Snapshotting -------------------------------------------------------------------------

	public abstract StateTableSnapshot<K, N, S, ? extends StateTable<K, N, S>> createSnapshot();

	void readMappingsInKeyGroup(DataInputView inView, int keyGroupId) throws IOException {
		TypeSerializer<K> keySerializer = keyContext.getKeySerializer();
		TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
		TypeSerializer<S> stateSerializer = getStateSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			N namespace = namespaceSerializer.deserialize(inView);
			K key = keySerializer.deserialize(inView);
			S state = stateSerializer.deserialize(inView);
			put(key, keyGroupId, namespace, state);
		}
	}

	// for testing --------------------------------------------------------------------------

	@VisibleForTesting
	public abstract int sizeOfNamespace(Object namespace);
}