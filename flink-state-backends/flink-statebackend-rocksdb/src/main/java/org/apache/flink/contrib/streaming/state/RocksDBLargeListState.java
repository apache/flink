/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalLargeListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * {@link ListState} implementation that stores state in RocksDB.
 *
 * <p>{@link RocksDBStateBackend} must ensure that we set the
 * {@link org.rocksdb.StringAppendOperator} on the column family that we use for our state since
 * we use the {@code merge()} call.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 */
class RocksDBLargeListState<K, N, V>
	extends AbstractRocksDBMapState<K, N, Long, V, List<V>>
	implements InternalLargeListState<K, N, V> {

	private final TypeSerializer<Long> keySerializer;

	/**
	 * Serializer for the values.
	 */
	private final TypeSerializer<V> elementSerializer;

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	private static final byte DELIMITER = ',';

	/**
	 * Creates a new {@code RocksDBLargeListState}.
	 *
	 * @param columnFamily        The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer     The serializer for the state.
	 * @param defaultValue        The default value for the state.
	 * @param backend             The backend for which this state is bind to.
	 */
	private RocksDBLargeListState(
		ColumnFamilyHandle columnFamily,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<Map<Long, List<V>>> valueSerializer,
		Map<Long, List<V>> defaultValue,
		RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily,
			namespaceSerializer,
			valueSerializer,
			defaultValue,
			backend);

		Preconditions.checkState(valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

		this.elementSerializer = ((ListSerializer) ((MapSerializer) valueSerializer).getValueSerializer()).getElementSerializer();
		this.keySerializer = LongSerializer.INSTANCE;
	}

	@Override
	public Map<Long, List<V>> getInternal() throws Exception {
		final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

		Map<Long, List<V>> map = new HashMap<>();

		new RocksDBElementIterator<Map.Entry<Long, V>>(backend.db, prefixBytes) {
			@Override
			public Map.Entry<Long, V> next() {
				return nextEntry();
			}
		}.forEachRemaining(entry -> {
			List list;
			if (map.containsKey(entry.getKey())) {
				list = map.get(entry.getKey());
			} else {
				list = new ArrayList();
				map.put(entry.getKey(), list);
			}
			list.add(entry.getValue());
		});

		return map;
	}

	@Override
	public Iterable<V> get() throws Exception {
		return get(true);
	}

	@Override
	public Iterable<V> get(boolean forward) throws Exception {
		final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
		byte[] lexMaxKey = backend.db.get(columnFamily, prefixBytes);
		if (lexMaxKey == null) {
			return null;
		} else {
			return () -> new RocksDBElementIterator<V>(
				backend.db,
				prefixBytes,
				forward) {
				@Override
				public V next() {
					RocksDBBaseMapEntry entry = nextEntry();
					return (entry == null ? null : entry.getValue());
				}
			};
		}
	}

	private List<V> deserializeList(
		byte[] valueBytes) {
		if (valueBytes == null) {
			return null;
		}

		dataInputView.setBuffer(valueBytes);

		List<V> result = new ArrayList<>();
		V next;
		while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
			result.add(next);
		}
		return result;
	}

	private static <V> V deserializeNextElement(DataInputDeserializer in, TypeSerializer<V> elementSerializer) {
		try {
			if (in.available() > 0) {
				V element = elementSerializer.deserialize(in);
				if (in.available() > 0) {
					in.readByte();
				}
				return element;
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected list element deserialization failure");
		}
		return null;
	}

	@Override
	public void add(V value) {
		Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

		try {
			long key;
			if (value instanceof StreamRecord<?>) {
				StreamRecord<?> record = ((StreamRecord) value);
				if (record.hasTimestamp()) {
					key = record.getTimestamp();
				} else {
					key = System.currentTimeMillis();
				}
			} else {
				key = System.currentTimeMillis();
			}
			// match lexicographically
			key = key - Long.MIN_VALUE;
			byte[] rawKeyBytes = serializeCurrentKeyWithGroupAndNamespacePlusUserKey(key, keySerializer);

			backend.db.merge(
				columnFamily,
				writeOptions,
				rawKeyBytes,
				serializeValue(value, elementSerializer));

			compareAndSwapMeta(rawKeyBytes);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
		}
	}

	private void compareAndSwapMeta(byte[] bytes) {
		try {
			// We could cache lexicographically max key in somewhere to speed up.
			byte[] lexMaxKey = backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());
			if (lexMaxKey == null ||
				compareLongKeyByteArray(lexMaxKey, bytes) < 0) {
				lexMaxKey = bytes;
				backend.db.put(
					columnFamily,
					writeOptions,
					serializeCurrentKeyWithGroupAndNamespace(),
					lexMaxKey);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
		}
	}

	private int compareLongKeyByteArray(byte[] bytes1, byte[] bytes2) {
		Preconditions.checkNotNull(bytes1, "You cannot compare null");
		Preconditions.checkNotNull(bytes2, "You cannot compare null");
		Preconditions.checkArgument(bytes1.length == bytes2.length,
			"Only compare the equal length byte array");

		int ret = 0;
		for (int i = 0; i < bytes1.length; i++) {
			ret = Integer.compare(Byte.toUnsignedInt(bytes1[i]), Byte.toUnsignedInt(bytes2[i]));
			if (ret != 0) {
				return ret;
			}
		}
		return ret;
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return;
		}
		byte[] lexMaxKey = null;
		byte[] tarLexMaxKey = null;
		try {
			tarLexMaxKey = backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());
			lexMaxKey = tarLexMaxKey;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
		}

		for (N source : sources) {
			if (source != null) {

				setCurrentNamespace(source);
				byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();
				try {
					byte[] currLexMaxKey = backend.db.get(columnFamily, prefixBytes);
					if (currLexMaxKey == null) {
						continue;
					} else if (lexMaxKey == null || compareLongKeyByteArray(lexMaxKey, currLexMaxKey) < 0) {
						lexMaxKey = currLexMaxKey;
					}
				} catch (Exception e) {
					throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
				}
				Iterator<RocksDBBaseMapEntry> iterator = new RocksDBElementBytesIterator<RocksDBBaseMapEntry>(
					backend.db,
					prefixBytes) {
					@Override
					public RocksDBBaseMapEntry next() {
						return nextEntry();
					}
				};
				for (; iterator.hasNext(); ) {
					try {
						RocksDBBaseMapEntry entry = iterator.next();
						byte[] sourceKey = entry.getRawKeyBytes();
						int keyOffset = entry.getUserKeyOffset();
						byte[] valueBytes = entry.getRawValueBytes();
						backend.db.delete(columnFamily, writeOptions, sourceKey);
						if (valueBytes != null) {
							setCurrentNamespace(target);
							final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespacePlusUserSuffix(
								sourceKey,
								keyOffset,
								sourceKey.length - keyOffset);
							backend.db.merge(columnFamily, writeOptions, targetKey, valueBytes);
						}
					} catch (Exception e) {
						throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
					}
				}
				try {
					setCurrentNamespace(source);
					backend.db.delete(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace());
				} catch (Exception e) {
					throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
				}
			}
			try {
				if (lexMaxKey != null) {
					if (tarLexMaxKey == null ||
						compareLongKeyByteArray(tarLexMaxKey, lexMaxKey) < 0) {
						setCurrentNamespace(target);
						backend.db.put(
							columnFamily,
							writeOptions,
							serializeCurrentKeyWithGroupAndNamespace(),
							lexMaxKey);
					}
				}
			} catch (Exception e) {
				throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
			}
		}
	}

	@Override
	public void update(List<V> valueToStore) throws IOException, RocksDBException {
		updateInternal(ImmutableMap.of(0L, valueToStore));
	}

	@Override
	public void updateInternal(Map<Long, List<V>> valueToStore) throws IOException, RocksDBException {
		Preconditions.checkNotNull(valueToStore, "Map of values to add cannot be null.");

		clear();

		for (List<V> values : valueToStore.values()) {
			for (V v : values) {
				add(v);
			}
		}
	}

	@Override
	public void addAll(List<V> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		for (V v : values) {
			add(v);
		}
	}

	@Override
	public void migrateSerializedValue(
		DataInputDeserializer serializedOldValueInput,
		DataOutputSerializer serializedMigratedValueOutput,
		TypeSerializer<Map<Long, List<V>>> priorSerializer,
		TypeSerializer<Map<Long, List<V>>> newSerializer) throws StateMigrationException {
		Preconditions.checkArgument(priorSerializer instanceof MapSerializer);
		Preconditions.checkArgument(newSerializer instanceof MapSerializer);

		TypeSerializer<V> priorElementSerializer =
			((ListSerializer<V>) ((MapSerializer) priorSerializer).getValueSerializer()).getElementSerializer();

		TypeSerializer<V> newElementSerializer =
			((ListSerializer<V>) ((MapSerializer) newSerializer).getValueSerializer()).getElementSerializer();

		try {
			while (serializedOldValueInput.available() > 0) {
				V element = deserializeNextElement(serializedOldValueInput, priorElementSerializer);
				newElementSerializer.serialize(element, serializedMigratedValueOutput);
				if (serializedOldValueInput.available() > 0) {
					serializedMigratedValueOutput.write(DELIMITER);
				}
			}
		} catch (Exception e) {
			throw new StateMigrationException("Error while trying to migrate RocksDB list state.", e);
		}
	}

	@Override
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer<K> safeKeySerializer,
		TypeSerializer<N> safeNamespaceSerializer,
		TypeSerializer<Map<Long, List<V>>> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
			serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(keyAndNamespace.f0, backend.getNumberOfKeyGroups());

		RocksDBSerializedCompositeKeyBuilder<K> keyBuilder =
			new RocksDBSerializedCompositeKeyBuilder<>(
				safeKeySerializer,
				backend.getKeyGroupPrefixBytes(),
				32);

		keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);

		final byte[] keyPrefixBytes = keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);

		final TypeSerializer<V> dupUserValueSerializer =
			((ListSerializer) ((MapSerializer<Long, List<V>>) safeValueSerializer).getValueSerializer()).getElementSerializer();

		final Iterator<V> iterator = new RocksDBElementIterator<V>(
			backend.db,
			keyPrefixBytes) {
			@Override
			public V next() {
				RocksDBBaseMapEntry entry = nextEntry();
				return (entry == null ? null : entry.getValue());
			}
		};

		// Return null to make the behavior consistent with other backends
		if (!iterator.hasNext()) {
			return null;
		}

		return KvStateSerializer.serializeIterator(() -> iterator, dupUserValueSerializer);
	}

	protected class RocksDBValue extends RocksDBBaseMapEntry {

		RocksDBValue(
			@Nonnull RocksDB db,
			int userKeyOffset,
			@Nonnull byte[] rawKeyBytes,
			@Nonnull byte[] rawValueBytes,
			V v) {
			super(db, userKeyOffset, rawKeyBytes, rawValueBytes);
			super.userValue = v;
		}
	}

	protected abstract class RocksDBElementIterator<T> extends RocksDBKVIterator<T> {

		/**
		 * Rocksdb has lexicographically order.
		 */
		private boolean forward;

		RocksDBElementIterator(
			RocksDB db,
			byte[] keyPrefixBytes) {
			this(db, keyPrefixBytes, true);
		}

		RocksDBElementIterator(
			RocksDB db,
			byte[] keyPrefixBytes,
			boolean forward) {
			super(db, keyPrefixBytes);
			this.forward = forward;
		}

		@Override
		protected List<RocksDBBaseMapEntry> deserializeKV(
			@Nonnull RocksDB db,
			int userKeyOffset,
			@Nonnull byte[] rawKeyBytes,
			@Nonnull byte[] rawValueBytes) {
			List<V> list = deserializeList(rawValueBytes);
			if (!forward) {
				Collections.reverse(list);
			}
			return list.stream().map((V v) ->
				new RocksDBValue(db, userKeyOffset, rawKeyBytes, rawValueBytes, v)
			).collect(Collectors.toList());
		}

		@Override
		protected boolean isEndOfIterator(RocksIteratorWrapper iterator) {
			if (forward) {
				return super.isEndOfIterator(iterator);
			} else {
				return !iterator.isValid() || Arrays.equals(metaKeyBytes, iterator.key());
			}
		}

		@Override
		protected void cacheSeek(RocksIteratorWrapper iterator) {
			if (currentEntry == null) {
				iterator.seek(metaKeyBytes);
				if (iterator.isValid()) {
					if (forward) {
						// skip the meta info
						iterator.next();
					} else {
						if (!Arrays.equals(iterator.key(), metaKeyBytes)) {
							throw new FlinkRuntimeException("Failed to find the upper bound key in meta line");
						} else {
							// get the lexicographically max key from meta info
							iterator.seek(iterator.value());
						}
					}
				}
			} else {
				super.cacheSeek(iterator);
			}
		}

		@Override
		protected void cacheNext(RocksIteratorWrapper iterator) {
			if (forward) {
				iterator.next();
			} else {
				iterator.prev();
			}
		}
	}

	protected abstract class RocksDBElementBytesIterator<T> extends RocksDBKVIterator<T> {

		RocksDBElementBytesIterator(RocksDB db,
									byte[] prefixBytes) {
			super(db, prefixBytes);
		}

		@Override
		protected List<RocksDBBaseMapEntry> deserializeKV(
			@Nonnull RocksDB db,
			int userKeyOffset,
			@Nonnull byte[] rawKeyBytes,
			@Nonnull byte[] rawValueBytes) {
			return ImmutableList.of(
				new RocksDBBaseMapEntry(
					db,
					userKeyOffset,
					rawKeyBytes,
					rawValueBytes));
		}

		@Override
		protected void cacheSeek(RocksIteratorWrapper iterator) {
			super.cacheSeek(iterator);
			if (currentEntry == null && iterator.isValid()) {
				// skip the meta info
				iterator.next();
			}
		}

	}

	@SuppressWarnings("unchecked")
	static <E, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
		RocksDBKeyedStateBackend<K> backend) {
		return (IS) new RocksDBLargeListState<>(
			registerResult.f0,
			registerResult.f1.getNamespaceSerializer(),
			(TypeSerializer<Map<Long, List<E>>>) registerResult.f1.getStateSerializer(),
			(Map<Long, List<E>>) stateDesc.getDefaultValue(),
			backend);
	}

	static class StateSnapshotTransformerWrapper<T> implements StateSnapshotTransformer<byte[]> {
		private final StateSnapshotTransformer<T> elementTransformer;
		private final TypeSerializer<T> elementSerializer;
		private final DataOutputSerializer out = new DataOutputSerializer(128);
		private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

		StateSnapshotTransformerWrapper(StateSnapshotTransformer<T> elementTransformer, TypeSerializer<T> elementSerializer) {
			this.elementTransformer = elementTransformer;
			this.elementSerializer = elementSerializer;
			this.transformStrategy = elementTransformer instanceof CollectionStateSnapshotTransformer ?
				((CollectionStateSnapshotTransformer) elementTransformer).getFilterStrategy() :
				CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
		}

		@Override
		@Nullable
		public byte[] filterOrTransform(@Nullable byte[] value) {
			if (value == null) {
				return null;
			}
			List<T> result = new ArrayList<>();
			DataInputDeserializer in = new DataInputDeserializer(value);
			T next;
			int prevPosition = 0;
			while ((next = deserializeNextElement(in, elementSerializer)) != null) {
				T transformedElement = elementTransformer.filterOrTransform(next);
				if (transformedElement != null) {
					if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
						return Arrays.copyOfRange(value, prevPosition, value.length);
					} else {
						result.add(transformedElement);
					}
				}
				prevPosition = in.getPosition();
			}
			try {
				return result.isEmpty() ? null : serializeValueList(result, elementSerializer, DELIMITER);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to serialize transformed list", e);
			}
		}

		@Override
		public boolean keepRaw(@Nullable byte[] key, @Nullable byte[] value) {
			// Work around fix the value 'max lexicographically key' cannot be deserialize issue.
			if (key == null || value == null) {
				return false;
			}
			if (key.length >= value.length) {
				return false;
			}

			for (int i = 0; i < key.length; i++) {
				if (key[i] != value[i]) {
					return false;
				}
			}

			return true;
		}

		byte[] serializeValueList(
			List<T> valueList,
			TypeSerializer<T> elementSerializer,
			byte delimiter) throws IOException {

			out.clear();
			boolean first = true;

			for (T value : valueList) {
				Preconditions.checkNotNull(value, "You cannot add null to a value list.");

				if (first) {
					first = false;
				} else {
					out.write(delimiter);
				}
				elementSerializer.serialize(value, out);
			}

			return out.getCopyOfBuffer();
		}
	}
}
