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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ValueState} that is snapshotted
 * into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapValueState<K, N, V>
		extends AbstractHeapState<K, N, V, ValueState<V>, ValueStateDescriptor<V>>
		implements ValueState<V> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapValueState(
			KeyedStateBackend<K> backend,
			ValueStateDescriptor<V> stateDesc,
			StateTable<K, N, V> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public V value() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, V>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			return stateDesc.getDefaultValue();
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return stateDesc.getDefaultValue();
		}

		V result = keyedMap.get(backend.<K>getCurrentKey());

		if (result == null) {
			return stateDesc.getDefaultValue();
		}

		return result;
	}

	@Override
	public void update(V value) {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		if (value == null) {
			clear();
			return;
		}

		Map<N, Map<K, V>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			namespaceMap = createNewMap();
			stateTable.set(backend.getCurrentKeyGroupIndex(), namespaceMap);
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			keyedMap = createNewMap();
			namespaceMap.put(currentNamespace, keyedMap);
		}

		keyedMap.put(backend.<K>getCurrentKey(), value);
	}
}
