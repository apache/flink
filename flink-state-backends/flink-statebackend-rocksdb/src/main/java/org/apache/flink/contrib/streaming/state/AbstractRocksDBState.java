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
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
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
 * @param <S> The type of {@link State}.
 * @param <SD> The type of {@link StateDescriptor}.
 */
public abstract class AbstractRocksDBState<K, N, S extends State, SD extends StateDescriptor<S, V>, V>
		implements InternalKvState<N>, State {

	/** Serializer for the namespace. */
	final TypeSerializer<N> namespaceSerializer;

	/** The current namespace, which the next value methods will refer to. */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state. */
	protected RocksDBKeyedStateBackend<K> backend;

	/** The column family of this particular instance of state. */
	protected ColumnFamilyHandle columnFamily;

	/** State descriptor from which to create this state instance. */
	protected final SD stateDesc;

	protected final WriteOptions writeOptions;

	protected final ByteArrayOutputStreamWithPos keySerializationStream;

	protected final DataOutputView keySerializationDataOutputView;

	private final boolean ambiguousKeyPossible;

	/**
	 * Creates a new RocksDB backed state.
	 *  @param namespaceSerializer The serializer for the namespace.
	 */
	protected AbstractRocksDBState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			SD stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;

		this.writeOptions = backend.getWriteOptions();
		this.stateDesc = Preconditions.checkNotNull(stateDesc, "State Descriptor");

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
			throw new RuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace");
	}

	@Override
	@SuppressWarnings("unchecked")
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		Preconditions.checkNotNull(serializedKeyAndNamespace, "Serialized key and namespace");

		//TODO make KvStateSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> des = KvStateSerializer.<K, N>deserializeKeyAndNamespace(
				serializedKeyAndNamespace,
				backend.getKeySerializer(),
				namespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(des.f0, backend.getNumberOfKeyGroups());

		// we cannot reuse the keySerializationStream member since this method
		// is called concurrently to the other ones and it may thus contain garbage
		ByteArrayOutputStreamWithPos tmpKeySerializationStream = new ByteArrayOutputStreamWithPos(128);
		DataOutputViewStreamWrapper tmpKeySerializationDateDataOutputView = new DataOutputViewStreamWrapper(tmpKeySerializationStream);

		writeKeyWithGroupAndNamespace(keyGroup, des.f0, des.f1,
			tmpKeySerializationStream, tmpKeySerializationDateDataOutputView);

		return backend.db.get(columnFamily, tmpKeySerializationStream.toByteArray());
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

		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		keySerializationStream.reset();
		RocksDBKeySerializationUtils.writeKeyGroup(keyGroup, backend.getKeyGroupPrefixBytes(), keySerializationDataOutputView);
		RocksDBKeySerializationUtils.writeKey(key, backend.getKeySerializer(), keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
		RocksDBKeySerializationUtils.writeNameSpace(namespace, namespaceSerializer, keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
	}
}
