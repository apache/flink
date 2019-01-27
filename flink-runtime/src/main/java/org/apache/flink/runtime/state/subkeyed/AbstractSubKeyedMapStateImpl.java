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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.BatchPutWrapper;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateSerializerUtil;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StorageInstance;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.StateSerializerUtil.serializeGroupPrefix;

/**
 * The base implementation of {@link AbstractSubKeyedMapState} backed by an a state storage.
 * The pairs in the storage are formatted as {(K, N, MK) -> MV}. Because the pairs are
 * partitioned by K, the mappings under the same key will be assigned to the same group and
 * can be easily retrieved with the prefix iterator on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 * @param <M> Type of the maps in the state.
 */
abstract class AbstractSubKeyedMapStateImpl<K, N, MK, MV, M extends Map<MK, MV>> implements AbstractSubKeyedMapState<K, N, MK, MV, M> {

	/**
	 * The state storage where the values are stored.
	 */
	protected final StateStorage stateStorage;

	/**
	 * Serialized state name of current state.
	 */
	protected byte[] stateNameByte;

	protected byte[] stateNameForSerialize;

	protected int serializedStateNameLength;

	/**
	 * The key serializer of current state.
	 */
	protected TypeSerializer<K> keySerializer;

	/**
	 * Serializer for map key of current state.
	 */
	protected TypeSerializer<MK> mapKeySerializer;

	/**
	 * Serializer for map value of current state.
	 */
	protected TypeSerializer<MV> mapValueSerializer;

	/**
	 * Serializer for namespace of current state.
	 */
	protected TypeSerializer<N> namespaceSerializer;

	/**
	 * State backend who creates the current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	protected ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
	protected DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	/** partitioner used to get key group.**/
	protected final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param stateStorage The state storage where the values are stored.
	 */
	AbstractSubKeyedMapStateImpl(
		AbstractInternalStateBackend backend,
		StateStorage stateStorage) {
		this.internalStateBackend = Preconditions.checkNotNull(backend);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
	}

	/**
	 * Creates a map under a key.
	 *
	 * @return A map under a key.
	 */
	abstract M createMap();

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace, MK mapKey) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				Map<MK, MV> map = get(key, namespace);
				return map != null && map.containsKey(mapKey);
			} else {
				outputStream.reset();

				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedMapState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				return stateStorage.get(serializedKey) != null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		if (stateStorage.lazySerde()) {
			Map<MK, MV> map = get(key, namespace);
			return map != null;
		} else {
			Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
			return iterator.hasNext();
		}
	}

	@Override
	public MV get(K key, N namespace, MK mapKey) {
		return getOrDefault(key, namespace, mapKey, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public MV getOrDefault(K key, N namespace, MK mapKey, MV defaultMapValue) {
		if (key == null || namespace == null) {
			return defaultMapValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					return defaultMapValue;
				}
				MV value = map.get(mapKey);
				return value == null ? defaultMapValue : value;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedMapState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				if (serializedValue == null) {
					return defaultMapValue;
				} else {
					return StateSerializerUtil.getDeserializeSingleValue(serializedValue, mapValueSerializer);
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public M get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@Override
	public M getOrDefault(K key, N namespace, M defaultMap) {
		if (key == null || namespace == null) {
			return defaultMap;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				M map = (M) heapStateStorage.get(key);
				return map == null ? defaultMap : map;
			} else {
				Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
				M result = createMap();
				while (iterator.hasNext()) {
					Map.Entry<MK, MV> entry = iterator.next();
					result.put(entry.getKey(), entry.getValue());
				}
				return result.isEmpty() ? defaultMap : result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public M getAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys == null || mapKeys.isEmpty()) {
			return null;
		}

		try {
			if (stateStorage.lazySerde()) {
				M map = get(key, namespace);

				if (map == null) {
					return null;
				}

				// lazy create
				M results = null;
				for (MK mapKey : mapKeys) {
					MV value = map.get(mapKey);
					if (value != null) {
						if (results == null) {
							results = createMap();
						}
						results.put(mapKey, value);
					}
				}
				return results;
			} else {
				M result = createMap();

				outputStream.reset();
				StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				int prefixLength = outputStream.getPosition();
				for (MK mapKey : mapKeys) {
					if (mapKey != null) {
						outputStream.setPosition(prefixLength);
						StateSerializerUtil.serializeItemWithKeyPrefix(outputView, mapKey, mapKeySerializer);
						byte[] byteValue = (byte[]) stateStorage.get(outputStream.toByteArray());
						if (byteValue != null) {
							MV value = StateSerializerUtil.getDeserializeSingleValue(byteValue, mapValueSerializer);
							result.put(mapKey, value);
						}
					}
				}

				return result.isEmpty() ? null : result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, M> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).getAll(key);
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
				Map<N, M> result = new HashMap<>();
				while (iterator.hasNext()) {
					Pair<byte[], byte[]> pair = iterator.next();
					N namespace = StateSerializerUtil.getDeserializedNamespcae(
						pair.getKey(),
						keySerializer,
						namespaceSerializer,
						serializedStateNameLength);

					MK mapKey = StateSerializerUtil.getDeserializedMapKeyForSubKeyedMapState(
						pair.getKey(),
						keySerializer,
						namespaceSerializer,
						mapKeySerializer,
						serializedStateNameLength);
					MV mapValue = StateSerializerUtil.getDeserializeSingleValue(pair.getValue(), mapValueSerializer);

					M innerMap = result.get(namespace);
					if (innerMap == null) {
						innerMap = createMap();
						result.put(namespace, innerMap);
					}
					innerMap.put(mapKey, mapValue);
				}
				return result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void add(K key, N namespace, MK mapKey, MV mapValue) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(mapKey);
		Preconditions.checkNotNull(mapValue);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					map = createMap();
					heapStateStorage.put(key, map);
				}
				map.put(mapKey, mapValue);
			} else {
				outputStream.reset();

				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedMapState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				mapValueSerializer.serialize(mapValue, outputView);
				byte[] serializedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, serializedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(K key, N namespace, Map<? extends MK, ? extends MV> mappings) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		if (mappings == null || mappings.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map<MK, MV> map = (Map<MK, MV>) heapStateStorage.get(key);
				if (map == null) {
					map = createMap();
					heapStateStorage.put(key, map);
				}
				map.putAll(mappings);
			} else {
				StorageInstance instance = stateStorage.getStorageInstance();
				try (BatchPutWrapper batchPutWrapper = instance.getBatchPutWrapper()) {

					outputStream.reset();
					StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
						outputStream,
						outputView,
						key,
						keySerializer,
						namespace,
						namespaceSerializer,
						getKeyGroup(key),
						stateNameForSerialize);

					int prefixLength = outputStream.getPosition();

					ByteArrayOutputStreamWithPos valueOutputStream = new ByteArrayOutputStreamWithPos();
					DataOutputView valueOutputView = new DataOutputViewStreamWrapper(valueOutputStream);
					for (Map.Entry<? extends MK, ? extends MV> entry : mappings.entrySet()) {
						Preconditions.checkNotNull(entry.getKey(), "Can not insert null key to mapstate");
						Preconditions.checkNotNull(entry.getValue(), "Can not insert null value to mapstate");

						outputStream.setPosition(prefixLength);
						StateSerializerUtil.serializeItemWithKeyPrefix(outputView, entry.getKey(), mapKeySerializer);
						byte[] byteKey = outputStream.toByteArray();

						valueOutputStream.reset();
						mapValueSerializer.serialize(entry.getValue(), valueOutputView);
						byte[] byteValue = valueOutputStream.toByteArray();
						batchPutWrapper.put(byteKey, byteValue);
					}
				}
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
				Iterator<Map.Entry<MK, MV>> iterator = iterator(key, namespace);
				while (iterator.hasNext()) {
					iterator.next();
					iterator.remove();
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key, N namespace, MK mapKey) {
		if (key == null || namespace == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				if (map != null) {
					map.remove(mapKey);

					if (map.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedMapState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					mapKey,
					mapKeySerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				stateStorage.remove(serializedKey);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(K key, N namespace, Collection<? extends MK> mapKeys) {
		if (key == null || namespace == null || mapKeys.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				if (map != null) {
					for (MK mk : mapKeys) {
						map.remove(mk);
					}

					if (map.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
			} else {
				outputStream.reset();
				StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				int prefixLength = outputStream.getPosition();
				for (MK mapKey : mapKeys) {
					outputStream.setPosition(prefixLength);
					StateSerializerUtil.serializeItemWithKeyPrefix(outputView, mapKey, mapKeySerializer);
					stateStorage.remove(outputStream.toByteArray());
				}
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

		if (stateStorage.lazySerde()) {
			((HeapStateStorage) stateStorage).removeAll(key);
		} else {
			try {
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
					stateStorage.remove(iterator.next().getKey());
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public Iterator<Map.Entry<MK, MV>> iterator(K key, N namespace) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptyIterator() : map.entrySet().iterator();
			} else {
				return getDeSerializedIterator(
					key, namespace, outputStream, outputView, keySerializer, namespaceSerializer, mapKeySerializer, mapValueSerializer);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<Map.Entry<MK, MV>> entries(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.entrySet();
			} else {
				return new Iterable<Map.Entry<MK, MV>>() {
					@Override
					public Iterator<Map.Entry<MK, MV>> iterator() {
						final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
						return new Iterator<Map.Entry<MK, MV>>() {
							@Override
							public boolean hasNext() {
								return innerIter.hasNext();
							}

							@Override
							public Map.Entry<MK, MV> next() {
								return innerIter.next();
							}

							@Override
							public void remove() {
								innerIter.remove();
							}
						};
					}
				};
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<MK> keys(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.keySet();
			} else {
				return new Iterable<MK>() {
					@Override
					public Iterator<MK> iterator() {
						final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
						return new Iterator<MK>() {
							@Override
							public boolean hasNext() {
								return innerIter.hasNext();
							}

							@Override
							public MK next() {
								return innerIter.next().getKey();
							}

							@Override
							public void remove() {
								innerIter.remove();
							}
						};
					}
				};
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterable<MV> values(K key, N namespace) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				Map map = (Map) heapStateStorage.get(key);
				return map == null ? Collections.emptySet() : map.values();
			} else {
				return new Iterable<MV>() {
					@Override
					public Iterator<MV> iterator() {
						final Iterator<Map.Entry<MK, MV>> innerIter = AbstractSubKeyedMapStateImpl.this.iterator(key, namespace);
						return new Iterator<MV>() {
							@Override
							public boolean hasNext() {
								return innerIter.hasNext();
							}

							@Override
							public MV next() {
								return innerIter.next().getValue();
							}

							@Override
							public void remove() {
								innerIter.remove();
							}
						};
					}
				};
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		if (stateStorage.lazySerde()) {
			return ((HeapStateStorage) stateStorage).namespaceIterator(key);
		} else {
			try {
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

				//TODO: Load the namespace in lazy style for better performance.
				Set<N> namespaceSet = new HashSet<>();
				try {
					while (iterator.hasNext()) {
						namespaceSet.add(StateSerializerUtil.getDeserializedNamespcae(
							iterator.next().getKey(),
							keySerializer,
							namespaceSerializer,
							serializedStateNameLength));
					}
				} catch (Exception e) {
					throw new StateAccessException(e);
				}
				Iterator<N> namespaceIterator = namespaceSet.iterator();
				return new Iterator<N>() {
					private N namespace = null;

					@Override
					public boolean hasNext() {
						return namespaceIterator.hasNext();
					}

					@Override
					public N next() {
						namespace = namespaceIterator.next();
						return namespace;
					}

					@Override
					public void remove() {
						if (namespace == null) {
							throw new IllegalStateException();
						}

						namespaceIterator.remove();
						AbstractSubKeyedMapStateImpl.this.remove(key, namespace);
					}
				};
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
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
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<M> safeValueSerializer) throws Exception {
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);
		K key = keyAndNamespace.f0;
		N namespace = keyAndNamespace.f1;

		MapSerializer<MK, MV> mapSerializer = (MapSerializer<MK, MV>) safeValueSerializer;
		final TypeSerializer<MK> dupUserKeySerializer = mapSerializer.getKeySerializer();
		final TypeSerializer<MV> dupUserValueSerializer = mapSerializer.getValueSerializer();

		M result;
		if (stateStorage.lazySerde()) {
			result = get(key, namespace);
			if (result == null) {
				return null;
			}
		} else {
			ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

			Iterator<Map.Entry<MK, MV>> iterator = getDeSerializedIterator(
				key,
				namespace,
				baos,
				view,
				safeKeySerializer,
				safeNamespaceSerializer,
				dupUserKeySerializer,
				dupUserValueSerializer);

			result = createMap();
			while (iterator.hasNext()) {
				Map.Entry<MK, MV> entry = iterator.next();
				if (entry.getKey() != null && entry.getValue() != null) {
					result.put(entry.getKey(), entry.getValue());
				}
			}
			if (result.isEmpty()) {
				return null;
			}
		}

		return KvStateSerializer.serializeMap(result.entrySet(), dupUserKeySerializer, dupUserValueSerializer);
	}

	private Iterator<Map.Entry<MK, MV>> getDeSerializedIterator(
		K key,
		N namespace,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<MK> dupUserKeySerializer,
		TypeSerializer<MV> dupUserValueSerializer) throws Exception {

		outputStream.reset();

		byte[] prefix = StateSerializerUtil.getSerializedPrefixKeyForSubKeyedState(
			outputStream,
			outputView,
			key,
			keySerializer,
			namespace,
			namespaceSerializer,
			getKeyGroup(key),
			stateNameForSerialize);
		StorageIterator<byte[], byte[]> iterator = stateStorage.prefixIterator(prefix);
		return new Iterator<Map.Entry<MK, MV>>() {
			Pair<byte[], byte[]> pair;
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public Map.Entry<MK, MV> next() {
				pair = iterator.next();
				return new Map.Entry<MK, MV>() {
					@Override
					public MK getKey() {
						try {
							return StateSerializerUtil.getDeserializedMapKeyForSubKeyedMapState(
								pair.getKey(),
								keySerializer,
								namespaceSerializer,
								dupUserKeySerializer,
								serializedStateNameLength);
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV getValue() {
						try {
							return StateSerializerUtil.getDeserializeSingleValue(pair.getValue(), dupUserValueSerializer);
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}

					@Override
					public MV setValue(MV value) {
						Preconditions.checkNotNull(value);
						try {

							ByteArrayOutputStreamWithPos valueOutputStream = new ByteArrayOutputStreamWithPos();
							DataOutputView valueOutputView = new DataOutputViewStreamWrapper(valueOutputStream);
							dupUserValueSerializer.serialize(value, valueOutputView);
							return StateSerializerUtil.getDeserializeSingleValue(pair.setValue(valueOutputStream.toByteArray()), dupUserValueSerializer);
						} catch (Exception e) {
							throw new StateAccessException(e);
						}
					}
				};
			}
		};
	}

	@Override
	public StateStorage<K, M> getStateStorage() {
		return stateStorage;
	}

	protected  <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}
}
