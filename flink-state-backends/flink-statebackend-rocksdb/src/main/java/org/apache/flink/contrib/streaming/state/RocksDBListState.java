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
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
class RocksDBListState<K, N, V>
	extends AbstractRocksDBState<K, N, List<V>, ListState<V>>
	implements InternalListState<K, N, V> {

	/** Serializer for the values. */
	private final TypeSerializer<V> elementSerializer;

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	private static final byte DELIMITER = ',';

	/**
	 * Creates a new {@code RocksDBListState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param elementSerializer The serializer for elements of the list state.
	 * @param backend The backend for which this state is bind to.
	 */
	private RocksDBListState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<List<V>> valueSerializer,
			List<V> defaultValue,
			TypeSerializer<V> elementSerializer,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
		this.elementSerializer = elementSerializer;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return backend.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<List<V>> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public Iterable<V> get() {
		return getInternal();
	}

	@Override
	public List<V> getInternal() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = dataOutputView.getCopyOfBuffer();
			byte[] valueBytes = backend.db.get(columnFamily, key);
			return deserializeList(valueBytes);
		} catch (IOException | RocksDBException e) {
			throw new FlinkRuntimeException("Error while retrieving data from RocksDB", e);
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
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = dataOutputView.getCopyOfBuffer();
			dataOutputView.clear();
			elementSerializer.serialize(value, dataOutputView);
			backend.db.merge(columnFamily, writeOptions, key, dataOutputView.getCopyOfBuffer());
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		// cache key and namespace
		final K key = backend.getCurrentKey();
		final int keyGroup = backend.getCurrentKeyGroupIndex();

		try {
			// create the target full-binary-key
			writeKeyWithGroupAndNamespace(keyGroup, key, target, dataOutputView);
			final byte[] targetKey = dataOutputView.getCopyOfBuffer();

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					writeKeyWithGroupAndNamespace(keyGroup, key, source, dataOutputView);

					byte[] sourceKey = dataOutputView.getCopyOfBuffer();
					byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, writeOptions, sourceKey);

					if (valueBytes != null) {
						backend.db.merge(columnFamily, writeOptions, targetKey, valueBytes);
					}
				}
			}
		}
		catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
		}
	}

	@Override
	public void update(List<V> valueToStore) {
		updateInternal(valueToStore);
	}

	@Override
	public void updateInternal(List<V> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		clear();

		if (!values.isEmpty()) {
			try {
				writeCurrentKeyWithGroupAndNamespace();
				byte[] key = dataOutputView.getCopyOfBuffer();
				byte[] premerge = getPreMergedValue(values, elementSerializer, dataOutputView);
				backend.db.put(columnFamily, writeOptions, key, premerge);
			} catch (IOException | RocksDBException e) {
				throw new FlinkRuntimeException("Error while updating data to RocksDB", e);
			}
		}
	}

	@Override
	public void addAll(List<V> values) {
		Preconditions.checkNotNull(values, "List of values to add cannot be null.");

		if (!values.isEmpty()) {
			try {
				writeCurrentKeyWithGroupAndNamespace();
				byte[] key = dataOutputView.getCopyOfBuffer();
				byte[] premerge = getPreMergedValue(values, elementSerializer, dataOutputView);
				backend.db.merge(columnFamily, writeOptions, key, premerge);
			} catch (IOException | RocksDBException e) {
				throw new FlinkRuntimeException("Error while updating data to RocksDB", e);
			}
		}
	}

	@Override
	public void migrateSerializedValue(
			DataInputDeserializer serializedOldValueInput,
			DataOutputSerializer serializedMigratedValueOutput,
			TypeSerializer<List<V>> priorSerializer,
			TypeSerializer<List<V>> newSerializer) throws StateMigrationException {

		Preconditions.checkArgument(priorSerializer instanceof ListSerializer);
		Preconditions.checkArgument(newSerializer instanceof ListSerializer);

		TypeSerializer<V> priorElementSerializer =
			((ListSerializer<V>) priorSerializer).getElementSerializer();

		TypeSerializer<V> newElementSerializer =
			((ListSerializer<V>) newSerializer).getElementSerializer();

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

	private static <V> byte[] getPreMergedValue(
		List<V> values,
		TypeSerializer<V> elementSerializer,
		DataOutputSerializer keySerializationStream) throws IOException {

		keySerializationStream.clear();
		boolean first = true;
		for (V value : values) {
			Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
			if (first) {
				first = false;
			} else {
				keySerializationStream.write(DELIMITER);
			}
			elementSerializer.serialize(value, keySerializationStream);
		}

		return keySerializationStream.getCopyOfBuffer();
	}

	@SuppressWarnings("unchecked")
	static <E, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
		RocksDBKeyedStateBackend<K> backend) {
		return (IS) new RocksDBListState<>(
			registerResult.f0,
			registerResult.f1.getNamespaceSerializer(),
			(TypeSerializer<List<E>>) registerResult.f1.getStateSerializer(),
			(List<E>) stateDesc.getDefaultValue(),
			((ListStateDescriptor<E>) stateDesc).getElementSerializer(),
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
				return result.isEmpty() ? null : getPreMergedValue(result, elementSerializer, out);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to serialize transformed list", e);
			}
		}
	}
}
