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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupIterator;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateIteratorUtil;
import org.apache.flink.runtime.state.StateSerializerUtil;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapStateStorage;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.StateSerializerUtil.KEY_END_BYTE;

/**
 * An implementation of {@link KeyedListState} backed by a {@link StateStorage}.
 * The pairs are formatted as {K -> List{E}}, and the pairs are partitioned by K.
 *
 * @param <K> Type of the keys in the state.
 * @param <E> Type of the elements in the state.
 */
public final class KeyedListStateImpl<K, E> implements KeyedListState<K, E> {

	/**
	 * The descriptor of current state.
	 */
	private final KeyedListStateDescriptor<K, E> descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	/**
	 * Serializer for list element of current state.
	 */
	private TypeSerializer<E> elementSerializer;

	/**
	 * Serializer for key of current state.
	 */
	private TypeSerializer<K> keySerializer;

	/**
	 * Serialized bytes of current state name.
	 */
	private final byte[] stateNameByte;

	private final byte[] stateNameForSerialize;

	/**
	 * State backend who creates the current state.
	 */
	private AbstractInternalStateBackend internalStateBackend;

	/**
	 * partitioner used to generate key group.
	 **/
	private static final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	private ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();

	private DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store the values.
	 *
	 * @param descriptor   The descriptor of this state.
	 * @param stateStorage The state storage where the values are stored.
	 */
	public KeyedListStateImpl(
		AbstractInternalStateBackend backend,
		KeyedListStateDescriptor<K, E> descriptor,
		StateStorage stateStorage) {
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.elementSerializer = descriptor.getElementSerializer();
		this.keySerializer = descriptor.getKeySerializer();
		this.internalStateBackend = Preconditions.checkNotNull(backend);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
			stateNameByte = out.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
		stateNameForSerialize = stateStorage.supportMultiColumnFamilies() ? null : stateNameByte;
	}

	@Override
	public KeyedListStateDescriptor getDescriptor() {
		return descriptor;
	}

	@Override
	public StateStorage<K, List<E>> getStateStorage() {
		return stateStorage;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key) {
		List<E> list = getOrDefault(key, null);

		return list != null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<E> get(K key) {
		return getOrDefault(key, null);
	}

	@Override
	public List<E> getOrDefault(K key, List<E> defaultValue) {
		if (key == null) {
			return defaultValue;
		}

		try {
			if (stateStorage.lazySerde()) {
				List<E> value = (List<E>) stateStorage.get(key);
				return value == null ? defaultValue : value;
			} else {
				byte[] serializedValue = getSerializedValue(key, outputStream, outputView, keySerializer);
				if (serializedValue == null) {
					return defaultValue;
				}
				return StateSerializerUtil.getDeserializeList(serializedValue, elementSerializer);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Map<K, List<E>> getAll(Collection<? extends K> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}

		Map<K, List<E>> results = new HashMap<>(keys.size());

		for (K key : keys) {
			if (key == null) {
				continue;
			}

			List<E> result = get(key);
			if (result != null && !result.isEmpty()) {
				results.put(key, result);
			}
		}

		return results;
	}

	@Override
	public void add(K key, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(element, "You can not add null value to list state.");

		try {
			if (stateStorage.lazySerde()) {
				List<E> list = (List<E>) stateStorage.get(key);
				if (list == null) {
					list = new ArrayList<>();
					stateStorage.put(key, list);
				}

				list.add(element);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
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
	public void addAll(K key, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(elements, "List of values to add cannot be null.");

		if (elements.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				((HeapStateStorage) stateStorage).transform(key, elements, (previousState, value) -> {
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
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				StateSerializerUtil.getPreMergedList(outputView, elements, elementSerializer);
				byte[] preMergedValue = outputStream.toByteArray();
				stateStorage.merge(serializedKey, preMergedValue);
			}
		} catch (Exception e) {
			if (e instanceof NullPointerException) {
				throw (NullPointerException) e;
			} else {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public void addAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		for (Map.Entry<? extends K, ? extends Collection<? extends E>> entry : map.entrySet()) {
			addAll(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void put(K key, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(element, "You can not add null value to list state.");

		try {
			if (stateStorage.lazySerde()) {
				List<E> list = new ArrayList<>(Arrays.asList(element));
				stateStorage.put(key, list);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				elementSerializer.serialize(element, outputView);
				byte[] serializedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, serializedValue);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void putAll(K key, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				List<E> list = new ArrayList<>();
				for (E element : elements) {
					Preconditions.checkNotNull(element, "You cannot add null to a ListState.");
					list.add(element);
				}
				stateStorage.put(key, list);
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				outputStream.reset();
				StateSerializerUtil.getPreMergedList(outputView, elements, elementSerializer);
				byte[] preMergedValue = outputStream.toByteArray();
				stateStorage.put(serializedKey, preMergedValue);
			}
		} catch (Exception e) {
			if (e instanceof NullPointerException) {
				throw (NullPointerException) e;
			} else {
				throw new StateAccessException(e);
			}
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return;
		}

		for (Map.Entry<? extends K, ? extends Collection<? extends E>> entry : map.entrySet()) {
			putAll(entry.getKey(), entry.getValue());
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
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				stateStorage.remove(serializedKey);
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean remove(K key, E elementToRemove) {
		if (key == null) {
			return false;
		}

		try {
			boolean success = false;
			if (stateStorage.lazySerde()) {
				List<E> list = get(key);
				if (list != null) {
					success = list.remove(elementToRemove);
					if (list.isEmpty()) {
						remove(key);
					}
				}
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				if (serializedValue == null) {
					success = false;
				} else {
					List<E> list = StateSerializerUtil.getDeserializeList(serializedValue, elementSerializer);
					success = list.remove(elementToRemove);
					if (list.isEmpty()) {
						stateStorage.remove(serializedKey);
					} else {
						outputStream.reset();
						StateSerializerUtil.getPreMergedList(outputView, list, elementSerializer);
						byte[] preMergedValueValue = outputStream.toByteArray();
						stateStorage.put(serializedKey, preMergedValueValue);
					}
				}
			}
			return success;

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
	public boolean removeAll(K key, Collection<? extends E> elementsToRemove) {
		if (key == null) {
			return false;
		}

		try {
			boolean success = false;
			if (stateStorage.lazySerde()) {
				List<E> value = get(key);
				if (value != null) {
					success = value.removeAll(elementsToRemove);
					if (value.isEmpty()) {
						remove(key);
					}
				}
			} else {
				outputStream.reset();
				byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
					outputStream,
					outputView,
					key,
					keySerializer,
					getKeyGroup(key),
					stateNameForSerialize);

				byte[] serializedValue = (byte[]) stateStorage.get(serializedKey);
				if (serializedValue == null) {
					success = false;
				} else {
					List<E> preList = StateSerializerUtil.getDeserializeList(serializedValue, elementSerializer);
					success = preList.removeAll(elementsToRemove);
					if (preList.isEmpty()) {
						stateStorage.remove(serializedKey);
					} else {
						outputStream.reset();
						StateSerializerUtil.getPreMergedList(outputView, preList, elementSerializer);
						byte[] preMergedValueValue = outputStream.toByteArray();
						stateStorage.put(serializedKey, preMergedValueValue);
					}
				}
			}
			return success;
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean removeAll(Map<? extends K, ? extends Collection<? extends E>> map) {
		if (map == null || map.isEmpty()) {
			return false;
		}

		boolean success = false;
		for (Map.Entry<? extends K, ? extends Collection<? extends E>> entry : map.entrySet()) {
			K key = entry.getKey();
			Collection<? extends E> elements = entry.getValue();
			success = removeAll(key, elements) || success;
		}

		return success;
	}

	@Override
	public Map<K, List<E>> getAll() {
		try {
			Map<K, List<E>> result = new HashMap<>();

			if (stateStorage.lazySerde()) {
				Iterator<Pair<K, List<E>>> iterator = stateStorage.iterator();
				while (iterator.hasNext()) {
					Pair<K, List<E>> pair = iterator.next();
					result.put(pair.getKey(), pair.getValue());
				}
			} else {
				if (!stateStorage.supportMultiColumnFamilies()
					&& internalStateBackend.getStateStorages().size() > 1) {
					for (Integer group : internalStateBackend.getKeyGroupRange()) {
						outputStream.reset();
						StateSerializerUtil.serializeGroupPrefix(outputStream, group, stateNameByte);
						byte[] groupPrefix = outputStream.toByteArray();

						outputStream.write(KEY_END_BYTE);
						byte[] groupPrefixEnd = outputStream.toByteArray();

						StorageIterator<byte[], byte[]> iterator = (StorageIterator<byte[], byte[]>) stateStorage.subIterator(groupPrefix, groupPrefixEnd);
						while (iterator.hasNext()) {
							Pair<byte[], byte[]> pair = iterator.next();
							K key = StateSerializerUtil.getDeserializedKeyForKeyedListState(
								pair.getKey(),
								keySerializer,
								stateNameByte.length);
							List<E> list = StateSerializerUtil.getDeserializeList(pair.getValue(), elementSerializer);
							result.put(key, list);
						}
					}
				} else {
					StorageIterator<byte[], byte[]> iterator = (StorageIterator<byte[], byte[]>) stateStorage.iterator();
					while (iterator.hasNext()) {
						Pair<byte[], byte[]> pair = iterator.next();
						K key = StateSerializerUtil.getDeserializedKeyForKeyedListState(
							pair.getKey(),
							keySerializer,
							stateStorage.supportMultiColumnFamilies() ? 0 : stateNameByte.length);
						List<E> list = StateSerializerUtil.getDeserializeList(pair.getValue(), elementSerializer);
						result.put(key, list);
					}
				}
			}
			return result;

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
				if (!stateStorage.supportMultiColumnFamilies()
					&& internalStateBackend.getStateStorages().size() > 1) {
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
						Iterator<Pair<K, List<E>>> iterator = stateStorage.iterator();
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
						if (!stateStorage.supportMultiColumnFamilies()
							&& internalStateBackend.getStateStorages().size() > 1) {
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
	public E poll(K key) {
		try {
			if (stateStorage.lazySerde()) {
				List<E> value = (List<E>) stateStorage.get(key);

				if (value == null) {
					return null;
				}

				E element = value.remove(0);

				if (value.isEmpty()) {
					stateStorage.remove(key);
				}

				return element;
			} else {
				List<E> value = get(key);
				if (value == null) {
					return null;
				}
				E element = value.remove(0);
				if (value.isEmpty()) {
					remove(key);
				} else {
					putAll(key, value);
				}
				return element;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public E peek(K key) {
		List<E> list = get(key);

		E element = null;
		if (list != null) {
			element = list.get(0);
		}

		return element;
	}

	@Override
	public byte[] getSerializedValue(
		final byte[] serializedKeyAndNamespace,
		final TypeSerializer<K> safeKeySerializer,
		final TypeSerializer<List<E>> safeValueSerializer) throws Exception {

		K key = KvStateSerializer.deserializeKeyAndNamespace(serializedKeyAndNamespace, safeKeySerializer, VoidNamespaceSerializer.INSTANCE).f0;

		ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
		DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);

		if (stateStorage.lazySerde()) {
			List<E> value = get(key);
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

			return getSerializedValue(key, baos, view, safeKeySerializer);
		}

	}

	private <K> int getKeyGroup(K key) {
		return partitioner.partition(key, internalStateBackend.getNumGroups());
	}

	private byte[] getSerializedValue(
		K key,
		ByteArrayOutputStreamWithPos outputStream,
		DataOutputView outputView,
		TypeSerializer<K> safeKeySerializer) throws Exception {

		outputStream.reset();
		byte[] serializedKey = StateSerializerUtil.getSerializedKeyForKeyedListState(
			outputStream,
			outputView,
			key,
			safeKeySerializer,
			getKeyGroup(key),
			stateNameForSerialize);

		return (byte[]) stateStorage.get(serializedKey);
	}
}
