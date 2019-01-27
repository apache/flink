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

import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StorageInstance;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.heap.internal.CopyOnWriteStateTable;
import org.apache.flink.runtime.state.heap.internal.NestedMapsStateTable;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link StateStorage} for heap.
 */
public class HeapStateStorage<K, N, S> implements StateStorage<K, S>{

	private final org.apache.flink.runtime.state.heap.internal.StateTable<K, N, S> stateTable;

	private N currentNamespace;

	public HeapStateStorage(
		AbstractInternalStateBackend stateBackend,
		RegisteredStateMetaInfo stateMetaInfo,
		N defaultNamespace,
		boolean usingNamespace,
		boolean asynchronous
	) {
		Preconditions.checkNotNull(stateBackend);
		Preconditions.checkNotNull(stateMetaInfo);

		this.currentNamespace = defaultNamespace;

		this.stateTable = asynchronous ?
			new CopyOnWriteStateTable<>(
				stateBackend,
				stateMetaInfo,
				usingNamespace) :
			new NestedMapsStateTable<>(
				stateBackend,
				stateMetaInfo,
				usingNamespace);
	}

	@Override
	public void put(K key, S state) {
		stateTable.put(key, currentNamespace, state);
	}

	@Override
	public S get(K key) {
		return stateTable.get(key, currentNamespace);
	}

	@Override
	public boolean remove(K key) {
		return stateTable.remove(key, currentNamespace);
	}

	@Override
	public StorageIterator<K, S> iterator() {
		return new HeapStorageIterator<>(stateTable.entryIterator());
	}

	@Override
	public StorageIterator<K, S> prefixIterator(K prefixKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public StorageIterator<K, S> subIterator(K prefixKeyStart, K prefixKeyEnd) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<K, S> firstEntry(K prefixKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<K, S> lastEntry(K prefixKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void merge(K key, S state) {
		throw new UnsupportedOperationException();
	}

	public <T> void transform (
		K key,
		T value,
		StateTransformationFunction<S, T> transformation
	) throws Exception {
		stateTable.transform(key, currentNamespace, value, transformation);
	}

	@Override
	public boolean lazySerde() {
		return true;
	}

	@Override
	public boolean supportMultiColumnFamilies() {
		return false;
	}

	@Override
	public StorageInstance getStorageInstance() {
		throw new UnsupportedOperationException("HeapStateStorage does not have StorageInstance");
	}

	public S getAndRemove(K key) {
		return stateTable.removeAndGetOld(key, currentNamespace);
	}

	/**
	 * Gets all mappings for the key when this storage has namespace.
	 *
	 * @param key the key to get.
	 */
	public Map<N, S> getAll(K key) {
		return stateTable.getAll(key);
	}

	/**
	 * Removes all mappings for the key when this storage has namespace.
	 *
	 * @param key the key to remove. Not null.
	 */
	public void removeAll(K key) {
		stateTable.removeAll(key);
	}

	/**
	 * Removes all mappings in the storage.
	 */
	public void removeAll() {
		stateTable.removeAll();
	}

	public StateTable getStateTable() {
		return stateTable;
	}

	/**
	 * Sets the namespace of the storage.
	 *
	 * @param namespace the namespace to set.
	 */
	public void setCurrentNamespace(N namespace) {
		currentNamespace = Preconditions.checkNotNull(namespace);
	}

	public void setStateMetaInfo(RegisteredStateMetaInfo stateMetaInfo) {
		stateTable.setStateMetaInfo(stateMetaInfo);
	}

	/**
	 * Returns an iterator over the namespaces for the given key.
	 *
	 * @param key the key to iterate on.
	 * @return an iterator over the namespaces for the given key.
	 */
	public Iterator<N> namespaceIterator(K key) {
		return stateTable.namespaceIterator(key);
	}
}
