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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.util.HashMap;
import java.util.Map;

/**
 * Heap-backed key/value state that is snapshotted into a serialized memory copy.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class MemValueState<K, N, V>
	extends AbstractMemState<K, N, V, ValueState<V>, ValueStateDescriptor<V>>
	implements ValueState<V> {
	
	public MemValueState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc) {
		super(keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc);
	}

	public MemValueState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc,
		HashMap<N, Map<K, V>> state) {
		super(keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc, state);
	}

	@Override
	public V value() {
		if (currentNSState == null) {
			currentNSState = state.get(currentNamespace);
		}
		if (currentNSState != null) {
			V value = currentNSState.get(currentKey);
			return value != null ? value : stateDesc.getDefaultValue();
		}
		return stateDesc.getDefaultValue();
	}

	@Override
	public void update(V value) {
		if (currentKey == null) {
			throw new RuntimeException("No key available.");
		}

		if (currentNSState == null) {
			currentNSState = new HashMap<>();
			state.put(currentNamespace, currentNSState);
		}

		currentNSState.put(currentKey, value);
	}

	@Override
	public KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, MemoryStateBackend> createHeapSnapshot(byte[] bytes) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, bytes);
	}

	public static class Snapshot<K, N, V> extends AbstractMemStateSnapshot<K, N, V, ValueState<V>, ValueStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> stateSerializer,
			ValueStateDescriptor<V> stateDescs, byte[] data) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, data);
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, MemoryStateBackend> createMemState(HashMap<N, Map<K, V>> stateMap) {
			return new MemValueState<>(keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}
}
