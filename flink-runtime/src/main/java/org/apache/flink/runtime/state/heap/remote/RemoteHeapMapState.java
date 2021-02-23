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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Heap-backed partitioned {@link MapState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
class RemoteHeapMapState<K, N, UK, UV>
	extends AbstractRemoteHeapState<K, N, Map<UK, UV>>
	implements InternalMapState<K, N, UK, UV> {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteHeapMapState.class);

	/** Serializer for the keys and values. */
	private final TypeSerializer<UK> userKeySerializer;
	private final TypeSerializer<UV> userValueSerializer;

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
	private RemoteHeapMapState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<Map<UK, UV>> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		Map<UK, UV> defaultValue,
		RemoteHeapKeyedStateBackend backend) {
		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);

		Preconditions.checkState(
			valueSerializer instanceof MapSerializer,
			"Unexpected serializer type.");

		MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
		this.userKeySerializer = castedMapSerializer.getKeySerializer();
		this.userValueSerializer = castedMapSerializer.getValueSerializer();
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
	public TypeSerializer<Map<UK, UV>> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public UV get(UK userKey) throws IOException {
		byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
			userKey,
			userKeySerializer);
		byte[] rawValueBytes = backend.syncRemClient.hget(kvStateInfo.nameBytes, rawKeyBytes);
		UV value = (rawValueBytes == null ? null : deserializeUserValue(
			dataInputView,
			rawValueBytes,
			userValueSerializer));
		LOG.trace(
			"RemoteHeapMapState retrieve value state {} userKey {} namespace {}",
			value,
			userKey,
			currentNamespace);
		return value;
	}

	@Override
	public void put(UK userKey, UV userValue) throws IOException {
		byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
			userKey,
			userKeySerializer);
		byte[] rawValueBytes = serializeValueNullSensitive(userValue, userValueSerializer);
		LOG.trace(
			"RemoteHeapMapState put value state {} userKey {} namespace {}",
			userValue,
			userKey,
			currentNamespace);
		backend.syncRemClient.hset(kvStateInfo.nameBytes, rawKeyBytes, rawValueBytes);
	}

	@Override
	public void putAll(Map<UK, UV> map) throws Exception {
		if (map == null) {
			return;
		}

		try (RemoteHeapWriteBatchWrapper writeBatchWrapper = new RemoteHeapWriteBatchWrapper(
			backend.syncRemClient,
			backend.getWriteBatchSize())) {
			for (Map.Entry<UK, UV> entry : map.entrySet()) {
				byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
					entry.getKey(),
					userKeySerializer);
				byte[] rawValueBytes = serializeValueNullSensitive(
					entry.getValue(),
					userValueSerializer);
				writeBatchWrapper.put(kvStateInfo.nameBytes, rawKeyBytes, rawValueBytes);
			}
		}
	}

	@Override
	public void remove(UK userKey) throws IOException {
		byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
			userKey,
			userKeySerializer);
		backend.syncRemClient.hdel(kvStateInfo.nameBytes, rawKeyBytes);
	}

	@Override
	public boolean contains(UK userKey) throws IOException {
		byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
			userKey,
			userKeySerializer);
		return backend.syncRemClient.hexists(kvStateInfo.nameBytes, rawKeyBytes);
	}

	@Override
	public Iterable<Map.Entry<UK, UV>> entries() {
		byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		Map<byte[], byte[]> userMap = backend.syncRemClient.hgetAll(kvStateInfo.nameBytes);
		return userMap == null ? Collections.emptySet() : userMap.entrySet().stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x.getKey()))
			.collect(
				Collectors.toMap(
					e -> {
						try {
							return deserializeUserKey(
								dataInputView,
								prefixBytes.length,
								e.getKey(),
								userKeySerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					},
					e ->
					{
						try {
							return deserializeUserValue(
								dataInputView,
								e.getValue(),
								userValueSerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					}
				)).entrySet();
	}

	@Override
	public Iterable<UK> keys() {
		byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		Collection<byte[]> keys = backend.syncRemClient.hkeys(kvStateInfo.nameBytes);
		return keys == null ? Collections.emptySet() : keys.stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x))
			.map(x -> {
				try {
					return deserializeUserKey(
						dataInputView,
						prefixBytes.length,
						x,
						userKeySerializer);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return null;
			}).collect(Collectors.toSet());
	}

	@Override
	public Iterable<UV> values() {
		byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		Map<byte[], byte[]> userMap = backend.syncRemClient.hgetAll(kvStateInfo.nameBytes);
		return userMap == null ? Collections.emptySet() : userMap.entrySet().stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x.getKey()))
			.map(e -> {
					try {
						return deserializeUserValue(dataInputView, e.getValue(), userValueSerializer);
					} catch (IOException ex) {
						ex.printStackTrace();
					}
					return null;
				}
			).collect(Collectors.toSet());
	}

	@Override
	public Iterator<Map.Entry<UK, UV>> iterator() {

		byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		Map<byte[], byte[]> userMap = backend.syncRemClient.hgetAll(kvStateInfo.nameBytes);
		return userMap == null ? null : userMap.entrySet().stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x.getKey()))
			.collect(
				Collectors.toMap(
					e -> {
						try {
							return deserializeUserKey(
								dataInputView,
								prefixBytes.length,
								e.getKey(),
								userKeySerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					},
					e ->
					{
						try {
							return deserializeUserValue(
								dataInputView,
								e.getValue(),
								userValueSerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					}
				)).entrySet().iterator();
	}

	@Override
	public boolean isEmpty() {
		byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		Map<byte[], byte[]> userMap = backend.syncRemClient.hgetAll(kvStateInfo.nameBytes);
		if (userMap == null) return true;
		return userMap
			.entrySet()
			.stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x.getKey()))
			.count() == 0;
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(
			keyAndNamespace.f0,
			backend.getNumberOfKeyGroups());

		RemoteHeapSerializedCompositeKeyBuilder<K> keyBuilder =
			new RemoteHeapSerializedCompositeKeyBuilder<>(
				safeKeySerializer,
				backend.getKeyGroupPrefixBytes(),
				32);

		keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);

		final byte[] prefixBytes = keyBuilder.buildCompositeKeyNamespace(
			keyAndNamespace.f1,
			namespaceSerializer);

		final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

		final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
		final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();
		final DataInputDeserializer inputView = new DataInputDeserializer();

		Map<byte[], byte[]> userMap = backend.syncRemClient.hgetAll(kvStateInfo.nameBytes);
		Map<UK, UV> result = userMap.entrySet().stream()
			.filter(x -> startWithKeyPrefix(prefixBytes, x.getKey()))
			.collect(
				Collectors.toMap(
					e -> {
						try {
							return deserializeUserKey(
								inputView,
								prefixBytes.length,
								e.getKey(),
								userKeySerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					},
					e ->
					{
						try {
							return deserializeUserValue(
								inputView,
								e.getValue(),
								userValueSerializer);
						} catch (IOException ex) {
							ex.printStackTrace();
						}
						return null;
					}
				));

		return KvStateSerializer.serializeMap(
			result.entrySet(),
			dupUserKeySerializer,
			dupUserValueSerializer);
	}

	@SuppressWarnings("unchecked")
	static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapMapState<>(
			keySerializer,
			(TypeSerializer<Map<UK, UV>>) metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			(Map<UK, UV>) stateDesc.getDefaultValue(),
			backend);
	}

	// ------------------------------------------------------------------------
	//  Serialization Methods
	// ------------------------------------------------------------------------

	private static <UK> UK deserializeUserKey(
		DataInputDeserializer dataInputView,
		int userKeyOffset,
		byte[] rawKeyBytes,
		TypeSerializer<UK> keySerializer) throws IOException {
		dataInputView.setBuffer(rawKeyBytes, userKeyOffset, rawKeyBytes.length - userKeyOffset);
		return keySerializer.deserialize(dataInputView);
	}

	private static <UV> UV deserializeUserValue(
		DataInputDeserializer dataInputView,
		byte[] rawValueBytes,
		TypeSerializer<UV> valueSerializer) throws IOException {

		dataInputView.setBuffer(rawValueBytes);

		boolean isNull = dataInputView.readBoolean();

		return isNull ? null : valueSerializer.deserialize(dataInputView);
	}

	private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
		if (rawKeyBytes.length < keyPrefixBytes.length) {
			return false;
		}

		for (int i = keyPrefixBytes.length; --i >= backend.getKeyGroupPrefixBytes(); ) {
			if (rawKeyBytes[i] != keyPrefixBytes[i]) {
				return false;
			}
		}

		return true;
	}
}
