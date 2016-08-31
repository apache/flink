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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
	protected RocksDBKeyedStateBackend backend;

	/** The column family of this particular instance of state */
	protected ColumnFamilyHandle columnFamily;

	/** State descriptor from which to create this state instance */
	protected final SD stateDesc;

	/**
	 * We disable writes to the write-ahead-log here.
	 */
	private final WriteOptions writeOptions;

	/**
	 * Creates a new RocksDB backed state.
	 *  @param namespaceSerializer The serializer for the namespace.
	 */
	protected AbstractRocksDBState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			SD stateDesc,
			RocksDBKeyedStateBackend backend) {

		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);

		this.stateDesc = Preconditions.checkNotNull(stateDesc, "State Descriptor");
	}

	// ------------------------------------------------------------------------

	@Override
	public void clear() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		try {
			writeKeyAndNamespace(out);
			byte[] key = baos.toByteArray();
			backend.db.remove(columnFamily, writeOptions, key);
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	protected void writeKeyAndNamespace(DataOutputView out) throws IOException {
		backend.getKeySerializer().serialize(backend.getCurrentKey(), out);
		out.writeByte(42);
		namespaceSerializer.serialize(currentNamespace, out);
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace");
	}

	@Override
	@SuppressWarnings("unchecked")
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		// Serialized key and namespace is expected to be of the same format
		// as writeKeyAndNamespace()
		Preconditions.checkNotNull(serializedKeyAndNamespace, "Serialized key and namespace");

		byte[] value = backend.db.get(columnFamily, serializedKeyAndNamespace);

		if (value != null) {
			return value;
		} else {
			return null;
		}
	}

}
