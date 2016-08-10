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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ReducingState} that is
 * snapshotted into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapReducingState<K, N, V>
		extends AbstractHeapState<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>>
		implements ReducingState<V> {

	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public HeapReducingState(
			KeyedStateBackend<K> backend,
			ReducingStateDescriptor<V> stateDesc,
			StateTable<K, N, V> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {
		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
		this.reduceFunction = stateDesc.getReduceFunction();
	}

	@Override
	public V get() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, V>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			return null;
		}

		Map<K, V> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(backend.<K>getCurrentKey());
	}

	@Override
	public void add(V value) throws IOException {
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

		V currentValue = keyedMap.put(backend.<K>getCurrentKey(), value);

		if (currentValue == null) {
			// we're good, just added the new value
		} else {
			V reducedValue = null;
			try {
				reducedValue = reduceFunction.reduce(currentValue, value);
			} catch (Exception e) {
				throw new RuntimeException("Could not add value to reducing state.", e);
			}
			keyedMap.put(backend.<K>getCurrentKey(), reducedValue);
		}
	}
}
