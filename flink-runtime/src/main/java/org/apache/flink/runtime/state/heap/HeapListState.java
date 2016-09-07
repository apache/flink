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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ListState} that is snapshotted
 * into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapListState<K, N, V>
		extends AbstractHeapState<K, N, ArrayList<V>, ListState<V>, ListStateDescriptor<V>>
		implements ListState<V> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapListState(
			KeyedStateBackend<K> backend,
			ListStateDescriptor<V> stateDesc,
			StateTable<K, N, ArrayList<V>> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public Iterable<V> get() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, ArrayList<V>>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ArrayList<V>> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(backend.<K>getCurrentKey());
	}

	@Override
	public void add(V value) {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		if (value == null) {
			clear();
			return;
		}

		Map<N, Map<K, ArrayList<V>>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			namespaceMap = createNewMap();
			stateTable.set(backend.getCurrentKeyGroupIndex(), namespaceMap);
		}

		Map<K, ArrayList<V>> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			keyedMap = createNewMap();
			namespaceMap.put(currentNamespace, keyedMap);
		}

		ArrayList<V> list = keyedMap.get(backend.<K>getCurrentKey());

		if (list == null) {
			list = new ArrayList<>();
			keyedMap.put(backend.<K>getCurrentKey(), list);
		}
		list.add(value);
	}
	
	@Override
	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		Map<N, Map<K, ArrayList<V>>> namespaceMap =
				stateTable.get(KeyGroupRangeAssignment.assignToKeyGroup(key, backend.getNumberOfKeyGroups()));

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ArrayList<V>> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return null;
		}

		ArrayList<V> result = keyedMap.get(key);

		if (result == null) {
			return null;
		}

		TypeSerializer<V> serializer = stateDesc.getSerializer();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		// write the same as RocksDB writes lists, with one ',' separator
		for (int i = 0; i < result.size(); i++) {
			serializer.serialize(result.get(i), view);
			if (i < result.size() -1) {
				view.writeByte(',');
			}
		}
		view.flush();

		return baos.toByteArray();
	}
}
