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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Heap-backed partitioned {@link ListState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class HeapListState<K, N, V>
		extends AbstractHeapMergingState<K, N, V, List<V>, Iterable<V>, ListState<V>>
		implements InternalListState<K, N, V> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 */
	public HeapListState(
			StateTable<K, N, List<V>> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<List<V>> valueSerializer,
			TypeSerializer<N> namespaceSerializer,
			List<V> defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<List<V>> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public Iterable<V> get() {
		return stateTable.get(currentNamespace);
	}

	@Override
	public void add(V value) {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

		final N namespace = currentNamespace;

		final StateTable<K, N, List<V>> map = stateTable;
		List<V> list = map.get(namespace);

		if (list == null) {
			list = new ArrayList<>();
			map.put(namespace, list);
		}
		list.add(value);
	}

	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<List<V>> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		List<V> result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

		if (result == null) {
			return null;
		}

		final TypeSerializer<V> dupSerializer = ((ListSerializer<V>) safeValueSerializer).getElementSerializer();

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		// write the same as RocksDB writes lists, with one ',' separator
		for (int i = 0; i < result.size(); i++) {
			dupSerializer.serialize(result.get(i), view);
			if (i < result.size() -1) {
				view.writeByte(',');
			}
		}
		view.flush();

		return baos.toByteArray();
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	@Override
	protected List<V> mergeState(List<V> a, List<V> b) {
		a.addAll(b);
		return a;
	}

	@Override
	public void update(List<V> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (values.isEmpty()) {
			clear();
			return;
		}

		List<V> newStateList = new ArrayList<>();
		for (V v : values) {
			Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
			newStateList.add(v);
		}

		stateTable.put(currentNamespace, newStateList);
	}

	@Override
	public void addAll(List<V> values) throws Exception {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			stateTable.transform(currentNamespace, values, (previousState, value) -> {
				if (previousState == null) {
					previousState = new ArrayList<>();
				}
				for (V v : value) {
					Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
					previousState.add(v);
				}
				return previousState;
			});
		}
	}
}
