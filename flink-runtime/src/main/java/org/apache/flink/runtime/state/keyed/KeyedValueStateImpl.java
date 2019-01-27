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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.BatchPutWrapper;
import org.apache.flink.runtime.state.GroupIterator;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateIteratorUtil;
import org.apache.flink.runtime.state.StateSerializerUtil;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.StorageInstance;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.runtime.state.StateSerializerUtil.KEY_END_BYTE;

/**
 * An implementation of {@link KeyedValueState} based on a {@link StateStorage}
 * The pairs are formatted as {K -> V}, and are partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <V> Type of the values in the state.
 */
public final class KeyedValueStateImpl<K, V> implements KeyedValueState<K, V> {

	/**
	 * The descriptor of this state.
	 */
	private final KeyedValueStateDescriptor<K, V> descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * Serializer of key for current state.
	 */
	private TypeSerializer<K> keySerializer;

	/**
	 * Serializer of value for current state.
	 */
	private TypeSerializer<V> valueSerializer;

	/**
	 * Serialized bytes of current state name.
	 */
	private final byte[] stateNameByte;

	private final byte[] stateNameForSerializer;

	/**
	 * State backend who creates current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	/** partitioner used to generate key group. */
	private static final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	private ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
	private DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	/**
	 * Constructor with the state storage to store the values.
	 *
	 * @param internalStateBackend The state backend who creates the current state.
	 * @param descriptor The descriptor of this state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	public KeyedValueStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		KeyedValueStateDescriptor<K, V> descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);

		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.keySerializer = descriptor.getKeySerializer();
		this.valueSerializer = descriptor.getValueSerializer();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
			stateNameByte = out.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
		this.stateNameForSerializer = stateStorage.supportMultiColumnFamilies() ? null : stateNameByte;
	}

	@Override
	public KeyedValueStateDescriptor getDescriptor() {
		return descriptor;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key) {
		if (key == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				return stateStorage.get(key) != null;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerializer);

				return stateStorage.get(serializedKey) != null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public V get(K key) {
		return getOrDefault(key, null);
	}

	@Override
	public V getOrDefault(K key, V defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				V value = (V) stateStorage.get(key);
				return value == null ? defaultValue : value;
			} else {
				byte[] serializedValue = getSerializedValue(key, outputStream, outputView, keySerializer);
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

	@Override
	public Map<K, V> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		try {
			Map<K, V> results = new HashMap<>();

			if (stateStorage.lazySerde()) {
				for (K key : keys) {
					if (key == null) {
						continue;
					}
					V value = (V) stateStorage.get(key);
					if (value != null) {
						results.put(key, value);
					}
				}
			} else {
				for (K key : keys) {
					if (key == null) {
						continue;
					}
					outputStream.reset();
					byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
						outputStream,
						outputView,
						key,
						keySerializer,
						getKeyGroup(key),
						stateNameForSerializer);
					byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
					if (serializedValue != null) {
						results.put(key, StateSerializerUtil.getDeserializeSingleValue(serializedValue, valueSerializer));
					}
				}
			}
			return results;

		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				stateStorage.remove(key);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerializer);
				stateStorage.remove(serializedKey);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		for (K key : keys) {
			remove(key);
		}
	}

	@Override
	public void put(K key, V value) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				stateStorage.put(key, value);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerializer);

				outputStream.reset();
				byte[] serializedValue = StateSerializerUtil.getSerializeSingleValue(outputStream, outputView, value, valueSerializer);
				stateStorage.put(serializedKey, serializedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				for (Map.Entry<? extends K, ? extends V> entry : pairs.entrySet()) {
					stateStorage.put(entry.getKey(), entry.getValue());
				}
			} else {
				StorageInstance instance = stateStorage.getStorageInstance();
				try (BatchPutWrapper batchPutWrapper = instance.getBatchPutWrapper()) {
					for (Map.Entry<? extends K, ? extends V> entry : pairs.entrySet()) {
						K key = entry.getKey();
						outputStream.reset();
						byte[] byteKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
							outputStream,
							outputView,
							key,
							keySerializer,
							getKeyGroup(key),
							stateNameForSerializer);

						outputStream.reset();

						byte[] byteValue = StateSerializerUtil.getSerializeSingleValue(
							outputStream,
							outputView,
							entry.getValue(),
							valueSerializer);

						batchPutWrapper.put(byteKey, byteValue);
					}
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map<K, V> getAll() {

		try {
			Map<K, V> results = new HashMap<>();

			if (stateStorage.lazySerde()) {
				Iterator<Pair<K, V>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, V> pair = iterator.next();
					results.put(pair.getKey(), pair.getValue());
				}
			} else {
				if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
					for (Integer group : internalStateBackend.getKeyGroupRange()) {
						outputStream.reset();
						StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
						byte[] groupPrefix = outputStream.toByteArray();
						outputStream.write(KEY_END_BYTE);
						byte[] groupPrefixEnd = outputStream.toByteArray();

						StorageIterator<byte[], byte[]> iterator = (StorageIterator<byte[], byte[]>) stateStorage.subIterator(groupPrefix, groupPrefixEnd);
						while (iterator.hasNext()) {
							Pair<byte[], byte[]> bytePair = iterator.next();
							K key = StateSerializerUtil.getDeserializedKeyForKeyedValueState(
								bytePair.getKey(),
								keySerializer,
								stateNameByte.length);
							V value = StateSerializerUtil.getDeserializeSingleValue(bytePair.getValue(), valueSerializer);
							results.put(key, value);
						}
					}
				} else {
					StorageIterator<byte[], byte[]> iterator = (StorageIterator<byte[], byte[]>) stateStorage.iterator();
					while (iterator.hasNext()) {
						Pair<byte[], byte[]> bytePair = iterator.next();
						K key = StateSerializerUtil.getDeserializedKeyForKeyedValueState(
							bytePair.getKey(),
							keySerializer,
							stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
						V value = StateSerializerUtil.getDeserializeSingleValue(bytePair.getValue(), valueSerializer);
						results.put(key, value);
					}
				}
			}
			return results;

		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll() {
		if (stateStorage.lazySerde()) {
			((HeapStateStorage) stateStorage).removeAll();
		} else {
			try {
				if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
					for (Integer group : internalStateBackend.getKeyGroupRange()) {
						outputStream.reset();
						StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
						byte[] groupPrefix = outputStream.toByteArray();
						outputStream.write(KEY_END_BYTE);
						byte[] groupPrefixEnd = outputStream.toByteArray();

						StorageIterator iterator = stateStorage.subIterator(groupPrefix, groupPrefixEnd);
						while (iterator.hasNext()) {
							iterator.next();
							iterator.remove();
						}
					}
				} else {
					StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();
					while (iterator.hasNext()) {
						iterator.next();
						iterator.remove();
					}
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterable<K> keys() {

		return new Iterable<K>() {
			@Override
			public Iterator<K> iterator() {
				try {
					if (stateStorage.lazySerde()) {
						Iterator<Pair<K, V>> iterator = stateStorage.iterator();
						return new Iterator<K>() {

							@Override
							public boolean hasNext() {
								return iterator.hasNext();
							}

							@Override
							public K next() {
								return iterator.next().getKey();
							}

							@Override
							public void remove() {
								iterator.remove();
							}
						};
					} else {
						if (!stateStorage.supportMultiColumnFamilies() && internalStateBackend.getStateStorages().size() > 1) {
							Collection<Iterator<Pair<byte[], byte[]>>> groupIterators = new ArrayList<>();
							for (Integer group : internalStateBackend.getKeyGroupRange()) {
								outputStream.reset();
								StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
								byte[] groupPrefix = outputStream.toByteArray();
								outputStream.write(KEY_END_BYTE);
								byte[] groupPrefixEnd = outputStream.toByteArray();

								StorageIterator iterator = stateStorage.subIterator(groupPrefix, groupPrefixEnd);
								groupIterators.add(iterator);
							}
							GroupIterator groupIterator = new GroupIterator(groupIterators);
							return StateIteratorUtil.createKeyIterator(groupIterator, keySerializer, stateNameByte.length);
						} else {
							StorageIterator<byte[], byte[]> iterator = stateStorage.iterator();

							return StateIteratorUtil.createKeyIterator(iterator, keySerializer, stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
						}
					}
				} catch (Exception e) {
					throw new StateAccessException(e);
				}
			}
		};
	}

	@Override
	public <T> void transform(K key, T value, StateTransformationFunction<V, T> transformation) {
		try {
			if (stateStorage.lazySerde()) {
				((HeapStateStorage) stateStorage).transform(key, value, transformation);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerializer);
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

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<V> safeValueSerializer) throws Exception {
		K key = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace, safeKeySerializer, VoidNamespaceSerializer.INSTANCE).f0;

		if (stateStorage.lazySerde()) {
			V value = get(key);
			if (value == null) {
				return null;
			}

			return KvStateSerializer.serializeValue(value, safeValueSerializer);
		} else {
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

			return getSerializedValue(key, baos, view, safeKeySerializer);
		}
	}

	@Override
	public StateStorage<K, V> getStateStorage() {
		return stateStorage;
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}

	private byte[] getSerializedValue(
		K key,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		TypeSerializer<K> keySerializer) throws Exception {

		outputStream.reset();
		byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedValueState(
			outputStream,
			outputView,
			key,
			keySerializer,
			getKeyGroup(key),
			stateNameForSerializer);
		return (byte[]) stateStorage.get(serializedKey);
	}
}
