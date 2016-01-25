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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Base class for partitioned {@link ListState} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 * @param <S> The type of State
 * @param <SD> The type of StateDescriptor for the State S
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public abstract class AbstractHeapState<K, N, SV, S extends State, SD extends StateDescriptor<S>, Backend extends AbstractStateBackend>
		implements KvState<K, N, S, SD, Backend>, State {

	/** Map containing the actual key/value pairs */
	protected final HashMap<N, Map<K, SV>> state;

	/** Serializer for the state value. The state value could be a List<V>, for example. */
	protected final TypeSerializer<SV> stateSerializer;

	/** The serializer for the keys */
	protected final TypeSerializer<K> keySerializer;

	/** The serializer for the namespace */
	protected final TypeSerializer<N> namespaceSerializer;

	/** This holds the name of the state and can create an initial default value for the state. */
	protected final SD stateDesc;

	/** The current key, which the next value methods will refer to */
	protected K currentKey;

	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace = null;

	/** Cache the state map for the current key. */
	protected Map<K, SV> currentNSState;

	/**
	 * Creates a new empty key/value state.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 */
	protected AbstractHeapState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc) {
		this(keySerializer, namespaceSerializer, stateSerializer, stateDesc, new HashMap<N, Map<K, SV>>());
	}

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param state The state map to use in this kev/value state. May contain initial state.
	 */
	protected AbstractHeapState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<SV> stateSerializer,
		SD stateDesc,
		HashMap<N, Map<K, SV>> state) {
		this.state = requireNonNull(state);
		this.keySerializer = requireNonNull(keySerializer);
		this.namespaceSerializer = requireNonNull(namespaceSerializer);
		this.stateSerializer = stateSerializer;
		this.stateDesc = stateDesc;
	}

	// ------------------------------------------------------------------------

	@Override
	public final void clear() {
		if (currentNSState != null) {
			currentNSState.remove(currentKey);
			if (currentNSState.isEmpty()) {
				state.remove(currentNamespace);
				currentNSState = null;
			}
		}
	}

	@Override
	public final void setCurrentKey(K currentKey) {
		this.currentKey = currentKey;
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		if (namespace != null && namespace.equals(this.currentNamespace)) {
			return;
		}
		this.currentNamespace = namespace;
		this.currentNSState = state.get(currentNamespace);
	}

	/**
	 * Returns the number of all state pairs in this state, across namespaces.
	 */
	protected final int size() {
		int size = 0;
		for (Map<K, SV> namespace: state.values()) {
			size += namespace.size();
		}
		return size;
	}

	@Override
	public void dispose() {
		state.clear();
	}

	/**
	 * Gets the serializer for the keys.
	 *
	 * @return The serializer for the keys.
	 */
	public final TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Gets the serializer for the namespace.
	 *
	 * @return The serializer for the namespace.
	 */
	public final TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}
}
