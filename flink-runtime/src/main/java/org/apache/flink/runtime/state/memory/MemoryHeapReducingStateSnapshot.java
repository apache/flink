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

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.util.DataInputDeserializer;

import java.util.HashMap;

/**
 * A snapshot of a {@link MemHeapListState} for a checkpoint. The data is stored in a heap byte
 * array, in serialized form.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <V> The type of the values in the snapshot state.
 */
public class MemoryHeapReducingStateSnapshot<K, V> implements KvStateSnapshot<K, ReducingState<V>, ReducingStateIdentifier<V>,  MemoryStateBackend> {

	private static final long serialVersionUID = 1L;

	/** Name of the key serializer class */
	private final String keySerializerClassName;

	/** Hash of the StateIdentifier, for sanity checks */
	int stateIdentifierHash;

	/** The serialized data of the state key/value pairs */
	private final byte[] data;

	/** The number of key/value pairs */
	private final int numEntries;

	/**
	 * Creates a new heap memory state snapshot.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param data The serialized data of the state key/value pairs
	 * @param numEntries The number of key/value pairs
	 */
	public MemoryHeapReducingStateSnapshot(TypeSerializer<K> keySerializer, ReducingStateIdentifier<V> stateIdentifiers, byte[] data, int numEntries) {
		this.keySerializerClassName = keySerializer.getClass().getName();
		this.stateIdentifierHash = stateIdentifiers.hashCode();
		this.data = data;
		this.numEntries = numEntries;
	}



	@Override
	public MemHeapReducingState<K, V> restoreState(
			MemoryStateBackend stateBackend,
			final TypeSerializer<K> keySerializer,
			ReducingStateIdentifier<V> stateIdentifier,
			ClassLoader classLoader) throws Exception {

		// validity checks
		if (!keySerializer.getClass().getName().equals(keySerializerClassName)
				|| stateIdentifierHash != stateIdentifier.hashCode()) {
			throw new IllegalArgumentException(
					"Cannot restore the state from the snapshot with the given serializers. " +
							"State (K/V) was serialized with " +
							"(" + keySerializerClassName + "/" + stateIdentifierHash + ") " +
							"now is (" + keySerializer.getClass().getName() + "/" + stateIdentifier.hashCode() + ")");
		}
		
		// restore state
		HashMap<K, V> stateMap = new HashMap<>(numEntries);
		DataInputDeserializer in = new DataInputDeserializer(data, 0, data.length);

		TypeSerializer<V> valueSerializer = stateIdentifier.getSerializer();
		for (int i = 0; i < numEntries; i++) {
			K key = keySerializer.deserialize(in);
			V value = valueSerializer.deserialize(in);
			stateMap.put(key, value);
		}
		
		return new MemHeapReducingState<>(stateBackend, keySerializer, stateIdentifier, stateMap);
	}

	/**
	 * Discarding the heap state is a no-op.
	 */
	@Override
	public void discardState() {}
}
