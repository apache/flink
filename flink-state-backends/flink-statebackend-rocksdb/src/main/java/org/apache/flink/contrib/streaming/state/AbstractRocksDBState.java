/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;

/**
 * Base class for {@link State} implementations that store state in a RocksDB database.
 *
 * <p>State is not stored in this class but in the {@link org.rocksdb.RocksDB} instance that
 * the {@link RocksDBStateBackend} manages and checkpoints.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of values kept internally in state.
 * @param <S> The type of {@link State}.
 */
public abstract class AbstractRocksDBState<K, N, V, S extends State> implements InternalKvState<K, N, V>, State {

	/** Serializer for the namespace. */
	final TypeSerializer<N> namespaceSerializer;

	/** Serializer for the state values. */
	final TypeSerializer<V> valueSerializer;

	/** The current namespace, which the next value methods will refer to. */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state. */
	protected RocksDBKeyedStateBackend<K> backend;

	/** The column family of this particular instance of state. */
	protected ColumnFamilyHandle columnFamily;

	protected final V defaultValue;

	protected final WriteOptions writeOptions;

	protected final ByteArrayOutputStreamWithPos keySerializationStream;

	protected final DataOutputView keySerializationDataOutputView;

	private final boolean ambiguousKeyPossible;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param backend The backend for which this state is bind to.
	 */
	protected AbstractRocksDBState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			RocksDBKeyedStateBackend<K> backend) {

		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;

		this.writeOptions = backend.getWriteOptions();
		this.valueSerializer = Preconditions.checkNotNull(valueSerializer, "State value serializer");
		this.defaultValue = defaultValue;

		this.keySerializationStream = new ByteArrayOutputStreamWithPos(128);
		this.keySerializationDataOutputView = new DataOutputViewStreamWrapper(keySerializationStream);
		this.ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(backend.getKeySerializer(), namespaceSerializer);
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			backend.db.delete(columnFamily, writeOptions, key);
		} catch (IOException | RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace");
	}

	@Override
	public byte[] getSerializedValue(
			final byte[] serializedKeyAndNamespace,
			final TypeSerializer<K> safeKeySerializer,
			final TypeSerializer<N> safeNamespaceSerializer,
			final TypeSerializer<V> safeValueSerializer) throws Exception {

		Preconditions.checkNotNull(serializedKeyAndNamespace);
		Preconditions.checkNotNull(safeKeySerializer);
		Preconditions.checkNotNull(safeNamespaceSerializer);
		Preconditions.checkNotNull(safeValueSerializer);

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> keyAndNamespace = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(keyAndNamespace.f0, backend.getNumberOfKeyGroups());

		// we cannot reuse the keySerializationStream member since this method
		// is called concurrently to the other ones and it may thus contain garbage
		ByteArrayOutputStreamWithPos tmpKeySerializationStream = new ByteArrayOutputStreamWithPos(128);
		DataOutputViewStreamWrapper tmpKeySerializationDateDataOutputView = new DataOutputViewStreamWrapper(tmpKeySerializationStream);

		writeKeyWithGroupAndNamespace(
				keyGroup,
				keyAndNamespace.f0,
				safeKeySerializer,
				keyAndNamespace.f1,
				safeNamespaceSerializer,
				tmpKeySerializationStream,
				tmpKeySerializationDateDataOutputView);

		return backend.db.get(columnFamily, tmpKeySerializationStream.toByteArray());
	}

	byte[] getKeyBytes() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			return keySerializationStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing key", e);
		}
	}

	byte[] getValueBytes(V value) {
		try {
			keySerializationStream.reset();
			valueSerializer.serialize(value, new DataOutputViewStreamWrapper(keySerializationStream));
			return keySerializationStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing value", e);
		}
	}

	protected void writeCurrentKeyWithGroupAndNamespace() throws IOException {
		writeKeyWithGroupAndNamespace(
			backend.getCurrentKeyGroupIndex(),
			backend.getCurrentKey(),
			currentNamespace,
			keySerializationStream,
			keySerializationDataOutputView);
	}

	protected void writeKeyWithGroupAndNamespace(
			int keyGroup, K key, N namespace,
			ByteArrayOutputStreamWithPos keySerializationStream,
			DataOutputView keySerializationDataOutputView) throws IOException {

		writeKeyWithGroupAndNamespace(
				keyGroup,
				key,
				backend.getKeySerializer(),
				namespace,
				namespaceSerializer,
				keySerializationStream,
				keySerializationDataOutputView);
	}

	protected void writeKeyWithGroupAndNamespace(
			final int keyGroup,
			final K key,
			final TypeSerializer<K> keySerializer,
			final N namespace,
			final TypeSerializer<N> namespaceSerializer,
			final ByteArrayOutputStreamWithPos keySerializationStream,
			final DataOutputView keySerializationDataOutputView) throws IOException {

		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(namespaceSerializer);

		keySerializationStream.reset();
		RocksDBKeySerializationUtils.writeKeyGroup(keyGroup, backend.getKeyGroupPrefixBytes(), keySerializationDataOutputView);
		RocksDBKeySerializationUtils.writeKey(key, keySerializer, keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
		RocksDBKeySerializationUtils.writeNameSpace(namespace, namespaceSerializer, keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
	}

	protected V getDefaultValue() {
		if (defaultValue != null) {
			return valueSerializer.copy(defaultValue);
		} else {
			return null;
		}
	}
}
