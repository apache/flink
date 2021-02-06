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

package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class RemoteHeapValueState<K, N, V>
	extends AbstractRemoteHeapState<K, N, V>
	implements InternalValueState<K, N, V> {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteHeapValueState.class);


	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param kvStateInfo StateInfo containing descriptors
	 * @param defaultValue The default value for the state.
	 * @param backend KeyBackend
	 */
	private RemoteHeapValueState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		V defaultValue,
		RemoteHeapKeyedStateBackend backend) {
		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);
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
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public V value() {
		try {
			byte[] valueBytes = backend.remoteKVStore.get(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes));
			if (valueBytes == null) {
				return getDefaultValue();
			}
			dataInputView.setBuffer(valueBytes);
			V value = valueSerializer.deserialize(dataInputView);
			LOG.trace(
				"RemoteHeapValueState retrieve value state {} namespace {} key {}",
				value,
				currentNamespace,
				backend.getCurrentKey());
			return value;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while retrieving data from remote heap.", e);
		}
	}

	@Override
	public void update(V value) {
		if (value == null) {
			clear();
			return;
		}

		try {
			backend.remoteKVStore.set(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
				serializeValue(value));
			LOG.trace(
				"RemoteHeapValueState update to value state {} namespace {} key {}",
				value,
				currentNamespace,
				backend.getCurrentKey());
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapValueState<>(
			keySerializer,
			metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			stateDesc.getDefaultValue(),
			backend);
	}

}
