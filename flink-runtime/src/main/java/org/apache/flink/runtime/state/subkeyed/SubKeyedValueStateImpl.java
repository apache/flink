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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateSerializerUtil;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.StateSerializerUtil.serializeGroupPrefix;

/**
 * An implementation of {@link SubKeyedValueState} based on an {@link StateStorage}
 * The pairs in the state storage are formatted as {(K, N) -> V}, and are
 * partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <V> Type of the values in the state.
 */
public final class SubKeyedValueStateImpl<K, N, V> implements SubKeyedValueState<K, N, V> {

	/**
	 * The descriptor of this state.
	 */
	private final SubKeyedValueStateDescriptor descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * State backend who creates the current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	/**
	 * Serializer of key for current state.
	 */
	private TypeSerializer<K> keySerializer;

	/**
	 * Serializer of namespace for current state.
	 */
	private TypeSerializer<N> namespaceSerializer;

	/**
	 * Serializer of value for current state.
	 */
	private TypeSerializer<V> valueSerializer;

	/**
	 * Serialized bytes of current state name.
	 */
	private final byte[] stateNameByte;

	private final byte[] stateNameForSerialize;

	private int serializedStateNameLength;

	ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
	DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	protected final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	/**
	 * Constructor with the state storage to store the values.
	 *
	 * @param internalStateBackend The state backend who creates the current state.
	 * @param descriptor The descriptor of this state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	public SubKeyedValueStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		SubKeyedValueStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.keySerializer = descriptor.getKeySerializer();
		this.namespaceSerializer = descriptor.getNamespaceSerializer();
		this.valueSerializer = descriptor.getValueSerializer();
		try {
			outputStream.reset();
			StringSerializer.INSTANCE.serialize(descriptor.getName(), outputView);
			stateNameByte = outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
		this.stateNameForSerialize = stateStorage.supportMultiColumnFamilies() ? null : stateNameByte;
		this.serializedStateNameLength = stateNameForSerialize == null ? 0 : stateNameForSerialize.length;
	}

	@Override
	public SubKeyedValueStateDescriptor getDescriptor() {
		return descriptor;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				return heapStateStorage.get(key) != null;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				return stateStorage.get(serializedKey) != null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public V get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getOrDefault(K key, N namespace, V defaultValue) {
		if (key == null || namespace == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				V value = (V) heapStateStorage.get(key);
				return value == null ? defaultValue : value;
			} else {
				byte[] serializedValue = getSerializedValue(key, namespace, outputStream, outputView, keySerializer, namespaceSerializer);

				if (serializedValue == null) {
					return defaultValue;
				} else {
					return StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer);
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, V> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).getAll(key);
			} else {
				Map<N, V> result = new HashMap<>();

				outputStream.reset();
				byte[] prefix = StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					outputStream,
					outputView,
					key,
					keySerializer,
					null,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(prefix);
				while (iterator.hasNext()) {
					Pair<byte[], byte[]> pair = iterator.next();
					byte[] byteKey = pair.getKey();
					byte[] byteValue = pair.getValue();

					N currentNamespace = StateSerializerUtil.getDeserializedNamespcae(
						byteKey,
						keySerializer,
						namespaceSerializer,
						stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
					V currentValue = StateSerializerUtil.getDeserializeSingleValue(byteValue, valueSerializer);
					result.put(currentNamespace, currentValue);
				}

				return result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.remove(key);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				stateStorage.remove(serializedKey);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(K key) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				((HeapStateStorage) stateStorage).removeAll(key);
			} else {
				outputStream.reset();
				byte[] prefix = StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					outputStream,
					outputView,
					key,
					keySerializer,
					null,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(prefix);
				while (iterator.hasNext()) {
					Pair<byte[], byte[]> pair = iterator.next();
					stateStorage.remove(pair.getKey());
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void put(K key, N namespace, V value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.put(key, value);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				valueSerializer.serialize(value, outputView);
				byte[] serializedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, serializedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public V getAndRemove(K key, N namespace) {
		if (key == null || namespace == null) {
			return null;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage<K, N, V> heapSateStorage = (HeapStateStorage) stateStorage;
				heapSateStorage.setCurrentNamespace(namespace);
				return heapSateStorage.getAndRemove(key);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				stateStorage.remove(serializedKey);

				return serializedValue == null ? null :
					StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).namespaceIterator(key);
			} else {
				outputStream.reset();
				byte[] prefix = StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					outputStream,
					outputView,
					key,
					keySerializer,
					null,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(prefix);

				return new Iterator<N>() {
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public N next() {
						try {
							return StateSerializerUtil.getDeserializedNamespcae(
								iterator.next().getKey(),
								keySerializer,
								namespaceSerializer,
								stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
						} catch (IOException e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public void remove() {
						iterator.remove();
					}
				};
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<K> keys(N namespace) {
		Preconditions.checkNotNull(namespace, "Do not support null as namespace.");

		Set<K> keys = new HashSet<>();
		if (stateStorage.lazySerde()) {
			StateTable stateTable = ((HeapStateStorage) stateStorage).getStateTable();

			keys = (Set<K>) stateTable.getKeys(namespace).collect(Collectors.toSet());
		} else {
			try {
				for (int group : internalStateBackend.getKeyGroupRange()) {
					outputStream.reset();
					serializeGroupPrefix(outputStream, group, stateNameForSerialize);
					StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(outputStream.toByteArray());
					while (iterator.hasNext()) {
						Pair<byte[], byte[]> pair = iterator.next();

						Pair<K, N> keyAndNamespace =
							StateSerializerUtil.getDeserializedKeyAndNamespace(
								pair.getKey(),
								keySerializer,
								namespaceSerializer,
								serializedStateNameLength);
						if (namespace.equals(keyAndNamespace.getValue())) {
							keys.add(keyAndNamespace.getKey());
						}
					}
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
		return keys.isEmpty() ? null : keys;
	}

	@Override
	public StateStorage<K, V> getStateStorage() {
		return stateStorage;
	}

	@Override
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer<K> safeKeySerializer,
		TypeSerializer<N> safeNamespaceSerializer,
		TypeSerializer<V> safeValueSerializer) throws Exception {

		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		if (stateStorage.lazySerde()) {
			V value = get(keyAndNamespace.f0, keyAndNamespace.f1);
			if (value == null) {
				return null;
			}
			return KvStateSerializer.serializeValue(value, safeValueSerializer);
		} else {
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

			return getSerializedValue(keyAndNamespace.f0, keyAndNamespace.f1, baos, view, safeKeySerializer, safeNamespaceSerializer);
		}
	}

	private byte[] getSerializedValue(
		K key,
		N namespace,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> safeNamespaceSerializer) throws Exception {

		outputStream.reset();
		byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
			outputStream,
			outputView,
			key,
			keySerializer,
			namespace,
			safeNamespaceSerializer,
			getKeyGroup(key),
			stateNameForSerialize);
		return (byte[]) stateStorage.get(serializedKey);
	}

	@Override
	public <T> void transform(K key, N namespace, T value, StateTransformationFunction<V, T> transformation) {
		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.transform(key, value, transformation);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);

				V oldValue = serializedValue == null ? null :
					StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer);
				V newValue = transformation.apply(oldValue, value);

				outputStream.reset();
				valueSerializer.serialize(newValue, outputView);
				stateStorage.put(serializedKey, outputStream.toByteArray());
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}
}
