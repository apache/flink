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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.util.DataInputDeserializer;

import java.util.HashMap;

/**
 * A snapshot of a {@link MemHeapKvState} for a checkpoint. The data is stored in a heap byte
 * array, in serialized form.
 * 
 * @param <K> The type of the key in the snapshot state.
 * @param <V> The type of the value in the snapshot state.
 */
public class MemoryHeapKvStateSnapshot<K, V> implements KvStateSnapshot<K, V, MemoryStateBackend> {
	
	private static final long serialVersionUID = 1L;
	
	/** Name of the key serializer class */
	private final String keySerializerClassName;

	/** Name of the value serializer class */
	private final String valueSerializerClassName;
	
	/** The serialized data of the state key/value pairs */
	private final byte[] data;
	
	/** The number of key/value pairs */
	private final int numEntries;

	/**
	 * Creates a new heap memory state snapshot.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 * @param data The serialized data of the state key/value pairs
	 * @param numEntries The number of key/value pairs
	 */
	public MemoryHeapKvStateSnapshot(TypeSerializer<K> keySerializer,
						TypeSerializer<V> valueSerializer, byte[] data, int numEntries) {
		this.keySerializerClassName = keySerializer.getClass().getName();
		this.valueSerializerClassName = valueSerializer.getClass().getName();
		this.data = data;
		this.numEntries = numEntries;
	}

	@Override
	public MemHeapKvState<K, V> restoreState(
			MemoryStateBackend stateBackend,
			final TypeSerializer<K> keySerializer,
			final TypeSerializer<V> valueSerializer,
			V defaultValue,
			ClassLoader classLoader,
			long recoveryTimestamp) throws Exception {

		// validity checks
		if (!keySerializer.getClass().getName().equals(keySerializerClassName) ||
			!valueSerializer.getClass().getName().equals(valueSerializerClassName)) {
				throw new IllegalArgumentException(
						"Cannot restore the state from the snapshot with the given serializers. " +
						"State (K/V) was serialized with (" + valueSerializerClassName + 
						"/" + keySerializerClassName + ")");
		}
		
		// restore state
		HashMap<K, V> stateMap = new HashMap<>(numEntries);
		DataInputDeserializer in = new DataInputDeserializer(data, 0, data.length);
		
		for (int i = 0; i < numEntries; i++) {
			K key = keySerializer.deserialize(in);
			V value = valueSerializer.deserialize(in);
			stateMap.put(key, value);
		}
		
		return new MemHeapKvState<K, V>(keySerializer, valueSerializer, defaultValue, stateMap);
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
