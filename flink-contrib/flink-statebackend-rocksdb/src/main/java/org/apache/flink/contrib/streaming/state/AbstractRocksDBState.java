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
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		implements KvState<N>, State {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBState.class);

	/** Serializer for the namespace */
	private final TypeSerializer<N> namespaceSerializer;

	/** The current namespace, which the next value methods will refer to */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state */
	protected RocksDBKeyedStateBackend<K> backend;

	/** The column family of this particular instance of state */
	protected ColumnFamilyHandle columnFamily;

	/** State descriptor from which to create this state instance */
	protected final SD stateDesc;

	/**
	 * We disable writes to the write-ahead-log here.
	 */
	private final WriteOptions writeOptions;

	protected final ByteArrayOutputStreamWithPos keySerializationStream;
	protected final DataOutputView keySerializationDateDataOutputView;

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

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
		this.stateDesc = Preconditions.checkNotNull(stateDesc, "State Descriptor");

		this.keySerializationStream = new ByteArrayOutputStreamWithPos(128);
		this.keySerializationDateDataOutputView = new DataOutputViewStreamWrapper(keySerializationStream);
		this.ambiguousKeyPossible = (backend.getKeySerializer().getLength() < 0)
				&& (namespaceSerializer.getLength() < 0);
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			backend.db.remove(columnFamily, writeOptions, key);
		} catch (IOException|RocksDBException e) {
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

		//TODO make KvStateRequestSerializer key-group aware to save this round trip and key-group computation
		Tuple2<K, N> des = KvStateRequestSerializer.<K, N>deserializeKeyAndNamespace(
				serializedKeyAndNamespace,
				backend.getKeySerializer(),
				namespaceSerializer);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(des.f0, backend.getNumberOfKeyGroups());
		writeKeyWithGroupAndNamespace(keyGroup, des.f0, des.f1);
		return backend.db.get(columnFamily, keySerializationStream.toByteArray());

	}

	protected void writeCurrentKeyWithGroupAndNamespace() throws IOException {
		writeKeyWithGroupAndNamespace(backend.getCurrentKeyGroupIndex(), backend.getCurrentKey(), currentNamespace);
	}

	protected void writeKeyWithGroupAndNamespace(int keyGroup, K key, N namespace) throws IOException {
		keySerializationStream.reset();
		writeKeyGroup(keyGroup);
		writeKey(key);
		writeNameSpace(namespace);
	}

	private void writeKeyGroup(int keyGroup) throws IOException {
		for (int i = backend.getKeyGroupPrefixBytes(); --i >= 0;) {
			keySerializationDateDataOutputView.writeByte(keyGroup >>> (i << 3));
		}
	}

	private void writeKey(K key) throws IOException {
		//write key
		int beforeWrite = keySerializationStream.getPosition();
		backend.getKeySerializer().serialize(key, keySerializationDateDataOutputView);

		if (ambiguousKeyPossible) {
			//write size of key
			writeLengthFrom(beforeWrite);
		}
	}

	private void writeNameSpace(N namespace) throws IOException {
		int beforeWrite = keySerializationStream.getPosition();
		namespaceSerializer.serialize(namespace, keySerializationDateDataOutputView);

		if (ambiguousKeyPossible) {
			//write length of namespace
			writeLengthFrom(beforeWrite);
		}
	}

	private void writeLengthFrom(int fromPosition) throws IOException {
		int length = keySerializationStream.getPosition() - fromPosition;
		writeVariableIntBytes(length);
	}

	private void writeVariableIntBytes(int value) throws IOException {
		do {
			keySerializationDateDataOutputView.writeByte(value);
			value >>>= 8;
		} while (value != 0);
	}
}
