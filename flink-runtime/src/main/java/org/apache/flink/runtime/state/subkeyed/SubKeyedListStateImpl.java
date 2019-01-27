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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
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
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.StateSerializerUtil.serializeGroupPrefix;

/**
 * An implementation of {@link SubKeyedListState} backed by a state storage.
 * The pairs in the storage are formatted as {(K, N) -> List{E}}.
 * Because the pairs are partitioned by K, all the elements under the same key
 * reside in the same group. They can be easily retrieved with a prefix iterator
 * on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <E> Type of the elements in the state.
 */
public final class SubKeyedListStateImpl<K, N, E> implements SubKeyedListState<K, N, E> {

	/**
	 * The descriptor of this state.
	 */
	private final SubKeyedListStateDescriptor<K, N, E> descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * The state descriptor associated with the current state.
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
	 * Serializer of list element for current state.
	 */
	private TypeSerializer<E> elementSerializer;

	/**
	 * Serialized bytes of current state name.
	 */
	private final byte[] stateNameByte;

	private final byte[] stateNameForSerialize;

	private final int serializedStateNameLength;

	private ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();

	private DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	protected final HashPartitioner partitioner = HashPartitioner.INSTANCE;
	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store elements.
	 *
	 * @param stateStorage The state storage where elements are stored.
	 */
	public SubKeyedListStateImpl(
		AbstractInternalStateBackend internalStateBackend,
		SubKeyedListStateDescriptor<K, N, E> descriptor,
		StateStorage stateStorage
	) {
		this.internalStateBackend = Preconditions.checkNotNull(internalStateBackend);
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);

		this.keySerializer = descriptor.getKeySerializer();
		this.namespaceSerializer = descriptor.getNamespaceSerializer();
		this.elementSerializer = descriptor.getElementSerializer();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
			this.stateNameByte = out.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
		this.stateNameForSerialize = stateStorage.supportMultiColumnFamilies() ? null : stateNameByte;
		this.serializedStateNameLength = stateNameForSerialize == null ? 0 : stateNameForSerialize.length;
	}

	@Override
	public SubKeyedListStateDescriptor getDescriptor() {
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
				return getOrDefault(key, namespace, null) != null;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
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
	public List<E> get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@Override
	public List<E> getOrDefault(K key, N namespace, List<E> defaultList) {
		if (key == null || namespace == null) {
			return defaultList;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				return value == null ? defaultList : value;
			} else {
				byte[] serializedValue = getSerializedValue(key, namespace, outputStream, outputView, keySerializer, namespaceSerializer);
				if (serializedValue == null) {
					return defaultList;
				} else {
					return StateSerializerUtil.getDeserializeList(serializedValue, elementSerializer);
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, List<E>> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).getAll(key);
			} else {
				Map<N, List<E>> result = new HashMap<>();

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
					List<E> currentValue = StateSerializerUtil.getDeserializeList(byteValue, elementSerializer);
					result.put(currentNamespace, currentValue);
				}

				return result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void add(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> list = (List<E>) heapStateStorage.get(key);
				if (list == null) {
					list = new ArrayList<>();
					heapStateStorage.put(key, list);
				}
				list.add(element);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				elementSerializer.serialize(element, outputView);
				byte[] serializedValue = outputStream.toByteArray();
				stateStorage.merge(serializedKey, serializedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.transform(key, elements, (previousState, value) -> {
					if (previousState == null) {
						previousState = new ArrayList<>();
					}
					for (E v : elements) {
						Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
						((List<E>) previousState).add(v);
					}
					return previousState;
				});
			} else {
				// serialized key
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				// serialized value
				outputStream.reset();
				StateSerializerUtil.getPreMergedList(outputView, elements, elementSerializer);

				byte[] preMergedValue = outputStream.toByteArray();
				stateStorage.merge(serializedKey, preMergedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void put(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.put(key, Arrays.asList(element));
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				elementSerializer.serialize(element, outputView);
				byte[] serilizedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, serilizedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void putAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> list = new ArrayList<>();
				for (E element : elements) {
					Preconditions.checkNotNull(element, "You cannot add null to a ListState.");
					list.add(element);
				}
				heapStateStorage.put(key, list);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				// serialize value
				outputStream.reset();
				StateSerializerUtil.getPreMergedList(outputView, elements, elementSerializer);
				byte[] preMergedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, preMergedValue);
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
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
	public boolean remove(K key, N namespace, E elementToRemove) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				boolean success = false;
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				if (value != null) {
					success = value.remove(elementToRemove);
					if (value.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
				return success;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				byte[] byteValue = (byte[]) stateStorage.get(serializedKey);
				if (byteValue == null) {
					return false;
				} else {
					List<E> list = StateSerializerUtil.getDeserializeList(byteValue, elementSerializer);
					boolean success = list.remove(elementToRemove);
					if (list.isEmpty()) {
						stateStorage.remove(serializedKey);
					} else {
						putAll(key, namespace, list);
					}
					return success;
				}
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean removeAll(K key, N namespace, Collection<? extends E> elements) {
		if (key == null || namespace == null || elements == null || elements.isEmpty()) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				boolean success = false;
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				if (value != null) {
					success = value.removeAll(elements);
					if (value.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
				return success;
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					namespace,
					namespaceSerializer,
					getKeyGroup(key),
					stateNameForSerialize);
				byte[] byteValue = (byte[]) stateStorage.get(serializedKey);
				if (byteValue == null) {
					return false;
				} else {
					List<E> list = StateSerializerUtil.getDeserializeList(byteValue, elementSerializer);
					boolean success = list.removeAll(elements);
					if (list.isEmpty()) {
						stateStorage.remove(serializedKey);
					} else {
						putAll(key, namespace, list);
					}
					return success;
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
							N namespace = StateSerializerUtil.getDeserializedNamespcae(
								iterator.next().getKey(),
								keySerializer,
								namespaceSerializer,
								stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
							return namespace;
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
		Preconditions.checkNotNull(namespace);

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
	public StateStorage<K, List<E>> getStateStorage() {
		return stateStorage;
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<N> safeNamespaceSerializer,
		final TypeSerializer<List<E>> safeValueSerializer) throws Exception {

		Tuple2<K, N> tuple = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		K key = tuple.f0;
		N namespace = tuple.f1;
		ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		if (stateStorage.lazySerde()) {
			List<E> value = get(key, namespace);
			if (value == null) {
				return null;
			}

			final TypeSerializer<E> dupSerializer = ((ListSerializer<E>) safeValueSerializer).getElementSerializer();

			for (int i = 0; i < value.size(); i++) {
				dupSerializer.serialize(value.get(i), view);
				if (i < value.size() -1) {
					view.writeByte(',');
				}
			}
			view.flush();

			return baos.toByteArray();
		} else {

			return getSerializedValue(key, namespace, baos, view, safeKeySerializer, safeNamespaceSerializer);
		}
	}

	private byte[] getSerializedValue(
		K key,
		N namespace,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		outputStream.reset();
		byte[] serializedKey = StateSerializerUtil.getSerializedKeyForSubKeyedListState(
			outputStream,
			outputView,
			key,
			keySerializer,
			namespace,
			namespaceSerializer,
			getKeyGroup(key),
			stateNameForSerialize);
		return (byte[]) stateStorage.get(serializedKey);
	}

	@Override
	public E poll(K key, N namespace) {
		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);

				if (value == null) {
					return null;
				}

				E element = value.remove(0);

				if (value.isEmpty()) {
					heapStateStorage.remove(key);
				}

				return element;
			} else {
				List<E> list = get(key, namespace);
				if (list == null) {
					return null;
				}
				Iterator<E> iterator = list.iterator();
				E result = null;
				if (iterator.hasNext()) {
					result = iterator.next();
					iterator.remove();
				}
				if (list.isEmpty()) {
					remove(key, namespace);
				} else {
					putAll(key, namespace, list);
				}
				return result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public E peek(K key, N namespace) {
		try {
			if (stateStorage.lazySerde()) {
				List<E> list = get(key, namespace);
				return list == null ? null : list.get(0);
			} else {
				List<E> list = get(key, namespace);
				if (list == null) {
					return null;
				}
				Iterator<E> iterator = list.iterator();
				E result = null;
				if (iterator.hasNext()) {
					result = iterator.next();
				}
				return result;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}
}

