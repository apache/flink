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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for partitioned {@link ListState} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 * @param <S> The type of State
 * @param <SD> The type of StateDescriptor for the State S
 */
public abstract class AbstractHeapState<K, N, SV, S extends State, SD extends StateDescriptor<S, ?>>
		implements KvState<N>, State {

	/** Map containing the actual key/value pairs */
	protected final StateTable<K, N, SV> stateTable;

	/** This holds the name of the state and can create an initial default value for the state. */
	protected final SD stateDesc;

	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace = null;

	protected final KeyedStateBackend<K> backend;

	protected final TypeSerializer<K> keySerializer;

	protected final TypeSerializer<N> namespaceSerializer;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	protected AbstractHeapState(
			KeyedStateBackend<K> backend,
			SD stateDesc,
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		Preconditions.checkNotNull(stateTable, "State table must not be null.");

		this.backend = backend;
		this.stateDesc = stateDesc;
		this.stateTable = stateTable;
		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
	}

	// ------------------------------------------------------------------------

	@Override
	public final void clear() {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		Map<N, Map<K, SV>> namespaceMap =
				stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap == null) {
			return;
		}

		Map<K, SV> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return;
		}

		SV removed = keyedMap.remove(backend.getCurrentKey());

		if (removed == null) {
			return;
		}

		if (!keyedMap.isEmpty()) {
			return;
		}

		namespaceMap.remove(currentNamespace);
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace must not be null.");
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		Preconditions.checkNotNull(serializedKeyAndNamespace, "Serialized key and namespace");

		Tuple2<K, N> keyAndNamespace = KvStateRequestSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, keySerializer, namespaceSerializer);

		return getSerializedValue(keyAndNamespace.f0, keyAndNamespace.f1);
	}

	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		Map<N, Map<K, SV>> namespaceMap =
				stateTable.get(KeyGroupRangeAssignment.assignToKeyGroup(key, backend.getNumberOfKeyGroups()));

		if (namespaceMap == null) {
			return null;
		}

		Map<K, SV> keyedMap = namespaceMap.get(currentNamespace);

		if (keyedMap == null) {
			return null;
		}

		SV result = keyedMap.get(key);

		if (result == null) {
			return null;
		}

		@SuppressWarnings("unchecked,rawtypes")
		TypeSerializer serializer = stateDesc.getSerializer();

		return KvStateRequestSerializer.serializeValue(result, serializer);
	}

	/**
	 * Creates a new map for use in Heap based state.
	 *
	 * <p>If the state queryable ({@link StateDescriptor#isQueryable()}, this
	 * will create a concurrent hash map instead of a regular one.
	 *
	 * @return A new namespace map.
	 */
	protected <MK, MV> Map<MK, MV> createNewMap() {
		if (stateDesc.isQueryable()) {
			return new ConcurrentHashMap<>();
		} else {
			return new HashMap<>();
		}
	}

	/**
	 * This should only be used for testing.
	 */
	public StateTable<K, N, SV> getStateTable() {
		return stateTable;
	}
}
