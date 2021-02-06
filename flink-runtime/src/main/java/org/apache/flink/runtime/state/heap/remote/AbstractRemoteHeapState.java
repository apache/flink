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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Base class for partitioned {@link State} implementations that are backed by a regular
 * heap hash map. The concrete implementations define how the state is checkpointed.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 */
public abstract class AbstractRemoteHeapState<K, N, SV> implements InternalKvState<K, N, SV> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRemoteHeapState.class);

	/** The current namespace, which the access methods will refer to. */
	protected N currentNamespace;

	protected final TypeSerializer<K> keySerializer;

	protected final TypeSerializer<SV> valueSerializer;

	protected final TypeSerializer<N> namespaceSerializer;

	protected RemoteHeapKeyedStateBackend<K> backend;

	protected final DataOutputSerializer dataOutputView;

	protected final DataInputDeserializer dataInputView;

	private final SV defaultValue;

	private final RemoteHeapSerializedCompositeKeyBuilder<K> sharedKeyNamespaceSerializer;

	protected final RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo;


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
	public AbstractRemoteHeapState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<SV> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		SV defaultValue,
		RemoteHeapKeyedStateBackend<K> backend
	) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.kvStateInfo = kvStateInfo;
		this.defaultValue = defaultValue;
		this.currentNamespace = null;

		this.backend = backend;
		this.dataOutputView = new DataOutputSerializer(128);
		this.dataInputView = new DataInputDeserializer();
		this.sharedKeyNamespaceSerializer = backend.getSharedREMKeyBuilder();
	}

	// ------------------------------------------------------------------------

	@Override
	public final void clear() {
		backend.remoteKVStore.del(serializeCurrentKeyWithGroupAndNamespace());
		// stateTable.remove(currentNamespace);
	}

	@Override
	public final void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(
			namespace,
			"Namespace must not be null.");
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<SV> safeValueSerializer) throws Exception {

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
			keyAndNamespace.f0,
			backend.getNumberOfKeyGroups());

		RemoteHeapSerializedCompositeKeyBuilder<K> keyBuilder =
			new RemoteHeapSerializedCompositeKeyBuilder<>(
				safeKeySerializer,
				backend.getKeyGroupPrefixBytes(),
				32
			);
		keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);
		byte[] key = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);
		LOG.trace("AbstractHeapState getSerializedValue keyAndNamespace f0 {} f1 {} keyGroup {}",
			keyAndNamespace.f0, keyAndNamespace.f1, keyGroup);
		return backend.remoteKVStore.get(key);
	}

	protected SV getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}

	@Override
	public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		//return stateTable.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
		throw new UnsupportedOperationException(
			"Global state entry iterator is unsupported for RocksDb backend");
	}

	byte[] serializeCurrentKeyWithGroupAndNamespace() {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamespace(
			currentNamespace,
			namespaceSerializer);
	}

	byte[] serializeCurrentKeyWithGroupAndNamespaceDesc(byte[] arr) throws IOException {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamesSpaceDesc(
			currentNamespace,
			namespaceSerializer,
			arr);
	}

	byte[] serializeValue(SV value) throws IOException {
		return serializeValue(value, valueSerializer);
	}

	<T> byte[] serializeValue(T value, TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		return serializeValueInternal(value, serializer);
	}

	private <T> byte[] serializeValueInternal(
		T value,
		TypeSerializer<T> serializer) throws IOException {
		serializer.serialize(value, dataOutputView);
		return dataOutputView.getCopyOfBuffer();
	}

	<T> byte[] serializeValueList(
		List<T> valueList,
		TypeSerializer<T> elementSerializer,
		byte delimiter) throws IOException {

		dataOutputView.clear();
		boolean first = true;

		for (T value : valueList) {
			Preconditions.checkNotNull(value, "You cannot add null to a value list.");

			if (first) {
				first = false;
			} else {
				dataOutputView.write(delimiter);
			}
			elementSerializer.serialize(value, dataOutputView);
		}

		return dataOutputView.getCopyOfBuffer();
	}

	<UK> byte[] serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
		UK userKey,
		TypeSerializer<UK> userKeySerializer) throws IOException {
		return sharedKeyNamespaceSerializer.buildCompositeKeyNamesSpaceUserKey(
			currentNamespace,
			namespaceSerializer,
			userKey,
			userKeySerializer
		);
	}

	<T> byte[] serializeValueNullSensitive(
		T value,
		TypeSerializer<T> serializer) throws IOException {
		dataOutputView.clear();
		dataOutputView.writeBoolean(value == null);
		return serializeValueInternal(value, serializer);
	}

	byte[] getKeyBytes() {
		return serializeCurrentKeyWithGroupAndNamespace();
	}

	byte[] getValueBytes(SV value) {
		try {
			dataOutputView.clear();
			valueSerializer.serialize(value, dataOutputView);
			return dataOutputView.getCopyOfBuffer();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing value", e);
		}
	}
}
