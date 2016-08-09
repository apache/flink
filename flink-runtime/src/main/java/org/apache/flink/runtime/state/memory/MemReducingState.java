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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ReducingState} that is
 * snapshotted into a serialized memory copy.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
public class MemReducingState<K, N, V>
	extends AbstractMemState<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>>
	implements ReducingState<V> {

	private final ReduceFunction<V> reduceFunction;

	public MemReducingState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<V> stateDesc) {
		super(keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc);
		this.reduceFunction = stateDesc.getReduceFunction();
	}

	public MemReducingState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ReducingStateDescriptor<V> stateDesc,
		HashMap<N, Map<K, V>> state) {
		super(keySerializer, namespaceSerializer, stateDesc.getSerializer(), stateDesc, state);
		this.reduceFunction = stateDesc.getReduceFunction();
	}

	@Override
	public V get() {
		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = state.get(currentNamespace);
		}
		if (currentNSState != null) {
			Preconditions.checkState(currentKey != null, "No key set");
			return currentNSState.get(currentKey);
		}
		return null;
	}

	@Override
	public void add(V value) throws IOException {
		Preconditions.checkState(currentKey != null, "No key set");

		if (currentNSState == null) {
			Preconditions.checkState(currentNamespace != null, "No namespace set");
			currentNSState = createNewNamespaceMap();
			state.put(currentNamespace, currentNSState);
		}
//		currentKeyState.merge(currentNamespace, value, new BiFunction<V, V, V>() {
//			@Override
//			public V apply(V v, V v2) {
//				try {
//					return reduceFunction.reduce(v, v2);
//				} catch (Exception e) {
//					return null;
//				}
//			}
//		});
		V currentValue = currentNSState.get(currentKey);
		if (currentValue == null) {
			currentNSState.put(currentKey, value);
		} else {
			try {
				currentNSState.put(currentKey, reduceFunction.reduce(currentValue, value));
			} catch (Exception e) {
				throw new RuntimeException("Could not add value to reducing state.", e);
			}
		}
	}

	@Override
	public KvStateSnapshot<K, N, ReducingState<V>, ReducingStateDescriptor<V>, MemoryStateBackend> createHeapSnapshot(byte[] bytes) {
		return new Snapshot<>(getKeySerializer(), getNamespaceSerializer(), stateSerializer, stateDesc, bytes);
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkNotNull(key, "Key");
		Preconditions.checkNotNull(namespace, "Namespace");

		Map<K, V> stateByKey = state.get(namespace);
		if (stateByKey != null) {
			return KvStateRequestSerializer.serializeValue(stateByKey.get(key), stateDesc.getSerializer());
		} else {
			return null;
		}
	}

	public static class Snapshot<K, N, V> extends AbstractMemStateSnapshot<K, N, V, ReducingState<V>, ReducingStateDescriptor<V>> {
		private static final long serialVersionUID = 1L;

		public Snapshot(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> stateSerializer,
			ReducingStateDescriptor<V> stateDescs, byte[] data) {
			super(keySerializer, namespaceSerializer, stateSerializer, stateDescs, data);
		}

		@Override
		public KvState<K, N, ReducingState<V>, ReducingStateDescriptor<V>, MemoryStateBackend> createMemState(HashMap<N, Map<K, V>> stateMap) {
			return new MemReducingState<>(keySerializer, namespaceSerializer, stateDesc, stateMap);
		}
	}}
