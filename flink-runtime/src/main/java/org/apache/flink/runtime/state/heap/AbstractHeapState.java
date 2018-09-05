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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

/**
 * Base class for partitioned {@link State} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 */
public abstract class AbstractHeapState<K, N, SV> implements InternalKvState<K, N, SV> {

	/** Map containing the actual key/value pairs. */
	protected final StateTable<K, N, SV> stateTable;

	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace;

	protected final TypeSerializer<K> keySerializer;

	protected final TypeSerializer<SV> valueSerializer;

	protected final TypeSerializer<N> namespaceSerializer;

	private final SV defaultValue;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 */
	AbstractHeapState(
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<SV> valueSerializer,
			TypeSerializer<N> namespaceSerializer,
			SV defaultValue) {

		this.stateTable = Preconditions.checkNotNull(stateTable, "State table must not be null.");
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.defaultValue = defaultValue;
		this.currentNamespace = null;
	}

	// ------------------------------------------------------------------------

	@Override
	public final void clear() {
		stateTable.remove(currentNamespace);
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace must not be null.");
	}

	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<SV> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		SV result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

		if (result == null) {
			return null;
		}
		return KvStateSerializer.serializeValue(result, safeValueSerializer);
	}

	/**
	 * This should only be used for testing.
	 */
	@VisibleForTesting
	public StateTable<K, N, SV> getStateTable() {
		return stateTable;
	}

	protected SV getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}
}
