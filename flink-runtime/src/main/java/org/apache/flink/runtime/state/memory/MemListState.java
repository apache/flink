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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ListState} that is snapshotted
 * into a serialized memory copy.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class MemListState<K, N, V>
	extends AbstractMemState<K, N, ArrayList<V>, ListState<V>, ListStateDescriptor<V>>
	implements ListState<V> {

	public MemListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, ListStateDescriptor<V> stateDesc) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc);
	}

	public MemListState(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, ListStateDescriptor<V> stateDesc, HashMap<N, Map<K, ArrayList<V>>> state) {
		super(keySerializer, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()), stateDesc, state);
	}

	@Override
	public Iterable<V> get() {
		if (currentNSState == null) {
			currentNSState = state.get(currentNamespace);
		}
		if (currentNSState != null) {
			List<V> result = currentNSState.get(currentKey);
			if (result == null) {
				return Collections.emptyList();
			} else {
				return result;
			}
		}
		return Collections.emptyList();
	}

	@Override
	public void add(V value) {
		if (currentKey == null) {
			throw new RuntimeException("No key available.");
		}

		if (currentNSState == null) {
			currentNSState = new HashMap<>();
			state.put(currentNamespace, currentNSState);
		}


		ArrayList<V> list = currentNSState.get(currentKey);
		if (list == null) {
			list = new ArrayList<>();
			currentNSState.put(currentKey, list);
		}
		list.add(value);
	}

	@Override
	public KvStateSnapshot<K, N, ListState<V>, ListStateDescriptor<V>, MemoryStateBackend> createHeapSnapshot(byte[] bytes) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, bytes);
	}

	public static class Snapshot<K, N, V> extends AbstractMemStateSnapshot<K, N, ArrayList<V>, ListState<V>, ListStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<ArrayList<V>> stateSerializer,
			ListStateDescriptor<V> stateDescs, byte[] data) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, data);
		}

		@Override
		public KvState<K, N, ListState<V>, ListStateDescriptor<V>, MemoryStateBackend> createMemState(HashMap<N, Map<K, ArrayList<V>>> stateMap) {
			return new MemListState<>(keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}

}
