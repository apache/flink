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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.util.HashMap;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ValueState} that is snapshotted
 * into files.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class FsValueState<K, N, V>
	extends AbstractFsState<K, N, V, ValueState<V>, ValueStateDescriptor<V>>
	implements ValueState<V> {

	/**
	 * Creates a new and empty key/value state.
	 * 
	 * @param keySerializer The serializer for the key.
     * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 * and can create a default state value.
	 * @param backend The file system state backend backing snapshots of this state
	 */
	public FsValueState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc);
	}

	/**
	 * Creates a new key/value state with the given state contents.
	 * This method is used to re-create key/value state with existing data, for example from
	 * a snapshot.
	 * 
	 * @param keySerializer The serializer for the key.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param state The map of key/value pairs to initialize the state with.
	 * @param backend The file system state backend backing snapshots of this state
	 */
	public FsValueState(FsStateBackend backend,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc,
		HashMap<N, Map<K, V>> state) {
		super(backend, keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc, state);
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
	public KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> createHeapSnapshot(Path filePath) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, filePath);
	}

	public static class Snapshot<K, N, V> extends AbstractFsStateSnapshot<K, N, V, ValueState<V>, ValueStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> stateSerializer,
			ValueStateDescriptor<V> stateDescs,
			Path filePath) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, filePath);
		}

		@Override
		public KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, FsStateBackend> createFsState(FsStateBackend backend, HashMap<N, Map<K, V>> stateMap) {
			return new FsValueState<>(backend, keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}
}
