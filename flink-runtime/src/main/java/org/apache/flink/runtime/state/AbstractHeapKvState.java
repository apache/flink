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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Base class for key/value state implementations that are backed by a regular heap hash map. The
 * concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public abstract class AbstractHeapKvState<K, V, Backend extends StateBackend<Backend>> implements KvState<K, V, Backend> {

	/** Map containing the actual key/value pairs */
	private final HashMap<K, V> state;
	
	/** The serializer for the keys */
	private final TypeSerializer<K> keySerializer;

	/** The serializer for the values */
	private final TypeSerializer<V> valueSerializer;
	
	/** The value that is returned when no other value has been associated with a key, yet */
	private final V defaultValue;
	
	/** The current key, which the next value methods will refer to */
	private K currentKey;
	
	/**
	 * Creates a new empty key/value state.
	 * 
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 * @param defaultValue The value that is returned when no other value has been associated with a key, yet.
	 */
	protected AbstractHeapKvState(TypeSerializer<K> keySerializer,
									TypeSerializer<V> valueSerializer,
									V defaultValue) {
		this(keySerializer, valueSerializer, defaultValue, new HashMap<K, V>());
	}

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 * 
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the values.
	 * @param defaultValue The value that is returned when no other value has been associated with a key, yet.
	 * @param state The state map to use in this kev/value state. May contain initial state.   
	 */
	protected AbstractHeapKvState(TypeSerializer<K> keySerializer,
									TypeSerializer<V> valueSerializer,
									V defaultValue,
									HashMap<K, V> state) {
		this.state = requireNonNull(state);
		this.keySerializer = requireNonNull(keySerializer);
		this.valueSerializer = requireNonNull(valueSerializer);
		this.defaultValue = defaultValue;
	}

	// ------------------------------------------------------------------------
	
	@Override
	public V value() {
		V value = state.get(currentKey);
		return value != null ? value : defaultValue;
	}

	@Override
	public void update(V value) {
		if (value != null) {
			state.put(currentKey, value);
		}
		else {
			state.remove(currentKey);
		}
	}

	@Override
	public void setCurrentKey(K currentKey) {
		this.currentKey = currentKey;
	}

	@Override
	public int size() {
		return state.size();
	}

	@Override
	public void dispose() {
		state.clear();
	}

	/**
	 * Gets the serializer for the keys.
	 * @return The serializer for the keys.
	 */
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Gets the serializer for the values.
	 * @return The serializer for the values.
	 */
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  checkpointing utilities
	// ------------------------------------------------------------------------
	
	protected void writeStateToOutputView(final DataOutputView out) throws IOException {
		for (Map.Entry<K, V> entry : state.entrySet()) {
			keySerializer.serialize(entry.getKey(), out);
			valueSerializer.serialize(entry.getValue(), out);
		}
	}
}
