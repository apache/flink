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

import org.apache.flink.api.common.state.SimpleStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.UpdatableState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Heap-backed partitioned states whose values are not composited and is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapSimpleState<K, N, V>
	extends AbstractHeapState<K, N, V, SimpleStateDescriptor<V, ? extends State<V>>>
	implements UpdatableState<V> {

	/**
	 * Creates a new simple state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapSimpleState(KeyedStateBackend<K> backend,
		SimpleStateDescriptor<V, ? extends State<V>> stateDesc,
		StateTable<K, N, V> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer) {

		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public byte[] getSerializedValue(int keyGroup, K key, N namespace) throws Exception {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		Map<N, Map<K, V>> namespaceMap = stateTable.get(keyGroup);
		if (namespaceMap == null) {
			return null;
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);
		if (keyedMap == null) {
			return null;
		}

		V result = keyedMap.get(key);
		if (result == null) {
			return null;
		}

		TypeSerializer serializer = stateDesc.getSerializer();

		return KvStateRequestSerializer.serializeValue(result, serializer);
	}

	@Override
	public V get() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, V>> namespaceMap = stateTable.get(backend.getCurrentKeyGroupIndex());
		if (namespaceMap == null) {
			return stateDesc.getDefaultValue();
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);
		if (keyedMap == null) {
			return stateDesc.getDefaultValue();
		}

		V result = keyedMap.get(backend.getCurrentKey());
		if (result == null) {
			return stateDesc.getDefaultValue();
		}

		return result;
	}

	@Override
	public final void clear() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, V>> namespaceMap = stateTable.get(backend.getCurrentKeyGroupIndex());
		if (namespaceMap == null) {
			return;
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);
		if (keyedMap == null) {
			return;
		}

		keyedMap.remove(backend.getCurrentKey());
		if (keyedMap.isEmpty()) {
			namespaceMap.remove(currentNamespace);
		}
	}
}
