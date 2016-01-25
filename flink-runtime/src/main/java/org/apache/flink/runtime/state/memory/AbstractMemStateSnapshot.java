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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.util.DataInputDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * A snapshot of a {@link MemValueState} for a checkpoint. The data is stored in a heap byte
 * array, in serialized form.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <N> The type of the namespace in the snapshot state.
 * @param <SV> The type of the value in the snapshot state.
 */
public abstract class AbstractMemStateSnapshot<K, N, SV, S extends State, SD extends StateDescriptor<S>> implements KvStateSnapshot<K, N, S, SD, MemoryStateBackend> {

	private static final long serialVersionUID = 1L;

	/** Key Serializer */
	protected final TypeSerializer<K> keySerializer;

	/** Namespace Serializer */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state value */
	protected final TypeSerializer<SV> stateSerializer;

	/** StateDescriptor, for sanity checks */
	protected final SD stateDesc;

	/** The serialized data of the state key/value pairs */
	private final byte[] data;

	/**
	 * Creates a new heap memory state snapshot.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateSerializer The serializer for the elements in the state HashMap
	 * @param stateDesc The state identifier
	 * @param data The serialized data of the state key/value pairs
	 */
	public AbstractMemStateSnapshot(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		byte[] data) {
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.stateSerializer = stateSerializer;
		this.stateDesc = stateDesc;
		this.data = data;
	}

	public abstract KvState<K, N, S, SD, MemoryStateBackend> createMemState(HashMap<N, Map<K, SV>> stateMap);

	@Override
	public KvState<K, N, S, SD, MemoryStateBackend> restoreState(
		MemoryStateBackend stateBackend,
		final TypeSerializer<K> keySerializer,
		ClassLoader classLoader, long recoveryTimestamp) throws Exception {

		// validity checks
		if (!this.keySerializer.equals(keySerializer)) {
			throw new IllegalArgumentException(
				"Cannot restore the state from the snapshot with the given serializers. " +
					"State (K/V) was serialized with " +
					"(" + this.keySerializer + ") " +
					"now is (" + keySerializer + ")");
		}
		
		// restore state
		DataInputDeserializer inView = new DataInputDeserializer(data, 0, data.length);

		final int numKeys = inView.readInt();
		HashMap<N, Map<K, SV>> stateMap = new HashMap<>(numKeys);

		for (int i = 0; i < numKeys; i++) {
			N namespace = namespaceSerializer.deserialize(inView);
			final int numValues = inView.readInt();
			Map<K, SV> namespaceMap = new HashMap<>(numValues);
			stateMap.put(namespace, namespaceMap);
			for (int j = 0; j < numValues; j++) {
				K key = keySerializer.deserialize(inView);
				SV value = stateSerializer.deserialize(inView);
				namespaceMap.put(key, value);
			}
		}

		return createMemState(stateMap);
	}

	/**
	 * Discarding the heap state is a no-op.
	 */
	@Override
	public void discardState() {}

	@Override
	public long getStateSize() {
		return data.length;
	}
}
