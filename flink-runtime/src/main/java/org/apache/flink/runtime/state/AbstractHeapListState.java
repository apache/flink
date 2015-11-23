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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Base class for partitioned {@link ListState} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <V> The type of the values in the list state.
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public abstract class AbstractHeapListState<K, V, Backend extends AbstractStateBackend>
		implements KvState<K, ListState<V>, ListStateIdentifier<V>, Backend>, ListState<V> {

	/** Map containing the actual key/value pairs */
	private final HashMap<K, List<V>> state;

	/** The serializer for the keys */
	private final TypeSerializer<K> keySerializer;

	/** This holds the name of the state and can create an initial default value for the state. */
	protected final ListStateIdentifier<V> stateIdentifier;

	/** The current key, which the next value methods will refer to */
	private K currentKey;

	private final Backend backend;

	/**
	 * Creates a new empty key/value state.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param stateIdentifier The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 */
	protected AbstractHeapListState(Backend backend,
			TypeSerializer<K> keySerializer,
			ListStateIdentifier<V> stateIdentifier) {
		this(backend, keySerializer, stateIdentifier, new HashMap<K, List<V>>());
	}

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param stateIdentifier The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param state The state map to use in this kev/value state. May contain initial state.
	 */
	protected AbstractHeapListState(Backend backend,
			TypeSerializer<K> keySerializer,
			ListStateIdentifier<V> stateIdentifier,
			HashMap<K, List<V>> state) {
		this.state = requireNonNull(state);
		this.keySerializer = requireNonNull(keySerializer);
		this.stateIdentifier = stateIdentifier;
		this.backend = backend;
	}

	// ------------------------------------------------------------------------

	@Override
	public Iterable<V> get() {
		List<V> result = state.get(currentKey);
		if (result == null) {
			return Collections.emptyList();
		} else {
			return result;
		}
	}

	@Override
	@SuppressWarnings("unchecked,rawtypes")
	public Iterable<Iterable<V>> getAll() {
		return (Iterable) state.values();
	}

	@Override
	public void add(V value) {
		List<V> list = state.get(currentKey);
		if (list == null) {
			list = new ArrayList<>();
			state.put(currentKey, list);
		}
		list.add(value);
	}

	@Override
	public void clear() {
		state.remove(currentKey);
		if (state.size() == 0) {
			backend.clear(stateIdentifier);
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

	// ------------------------------------------------------------------------
	//  checkpointing utilities
	// ------------------------------------------------------------------------
	
	protected void writeStateToOutputView(final DataOutputView out) throws IOException {
		TypeSerializer<V> valueSerializer = stateIdentifier.getSerializer();
		for (Map.Entry<K, List<V>> entry : state.entrySet()) {
			keySerializer.serialize(entry.getKey(), out);
			List<V> list = entry.getValue();
			out.writeInt(list.size());
			for (V value: list) {
				valueSerializer.serialize(value, out);
			}
		}
	}
}
