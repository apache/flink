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
import org.apache.flink.runtime.state.KvStateSnapshot;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

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
abstract class AbstractRocksDBState<K, N, S extends State, SD extends StateDescriptor<S, ?>>
		implements KvState<K, N, S, SD, RocksDBStateBackend>, State {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractRocksDBState.class);

	/** Serializer for the namespace */
	private final TypeSerializer<N> namespaceSerializer;

	/** The current namespace, which the next value methods will refer to */
	private N currentNamespace;

	/** Backend that holds the actual RocksDB instance where we store state */
	protected RocksDBStateBackend backend;

	/** The column family of this particular instance of state */
	ColumnFamilyHandle columnFamily;

	/**
	 * Creates a new RocksDB backed state.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 */
	AbstractRocksDBState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			RocksDBStateBackend backend) {

		this.namespaceSerializer = namespaceSerializer;
		this.backend = backend;

		this.columnFamily = columnFamily;
	}

	// ------------------------------------------------------------------------

	@Override
	final public void clear() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		try {
			writeKeyAndNamespace(out);
			byte[] key = baos.toByteArray();
			backend.db.remove(columnFamily, key);
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while removing entry from RocksDB", e);
		}
	}

	void writeKeyAndNamespace(DataOutputView out) throws IOException {
		backend.keySerializer().serialize(backend.currentKey(), out);
		out.writeByte(42);
		namespaceSerializer.serialize(currentNamespace, out);
	}

	@Override
	final public void setCurrentNamespace(N namespace) {
		this.currentNamespace = namespace;
	}

	@Override
	final public void dispose() {
		// ignore because we don't hold any state ourselves
	}

	@Override
	public void setCurrentKey(K key) {
		// ignore because we don't hold any state ourselves

	}

	@Override
	public KvStateSnapshot<K, N, S, SD, RocksDBStateBackend> snapshot(long checkpointId,
			long timestamp) throws Exception {
		throw new RuntimeException("Should not be called. Backups happen in RocksDBStateBackend.");
	}
}

