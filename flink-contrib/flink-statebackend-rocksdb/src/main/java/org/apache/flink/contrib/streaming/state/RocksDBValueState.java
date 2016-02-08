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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KvState;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBValueState<K, N, V>
	extends AbstractRocksDBState<K, N, ValueState<V>, ValueStateDescriptor<V>>
	implements ValueState<V> {

	/** Serializer for the values */
	private final TypeSerializer<V> valueSerializer;

	/** This holds the name of the state and can create an initial default value for the state. */
	protected final ValueStateDescriptor<V> stateDesc;

	/**
	 * Creates a new {@code RocksDBValueState}.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 * @param backupPath The path where to store backups.
	 */
	protected RocksDBValueState(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			File dbPath,
			String backupPath,
			Options options) {
		
		super(keySerializer, namespaceSerializer, dbPath, backupPath, options);
		this.stateDesc = requireNonNull(stateDesc);
		this.valueSerializer = stateDesc.getSerializer();
	}

	/**
	 * Creates a {@code RocksDBValueState} by restoring from a directory.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 * @param backupPath The path where to store backups.
	 * @param restorePath The path on the local file system that we are restoring from.
	 */
	protected RocksDBValueState(TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			File dbPath,
			String backupPath,
			String restorePath,
			Options options) {
		
		super(keySerializer, namespaceSerializer, dbPath, backupPath, restorePath, options);
		this.stateDesc = stateDesc;
		this.valueSerializer = stateDesc.getSerializer();
	}

	@Override
	public V value() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		try {
			writeKeyAndNamespace(out);
			byte[] key = baos.toByteArray();
			byte[] valueBytes = db.get(key);
			if (valueBytes == null) {
				return stateDesc.getDefaultValue();
			}
			return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void update(V value) throws IOException {
		if (value == null) {
			clear();
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		try {
			writeKeyAndNamespace(out);
			byte[] key = baos.toByteArray();
			baos.reset();
			valueSerializer.serialize(value, out);
			db.put(key, baos.toByteArray());
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	protected AbstractRocksDBSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>> createRocksDBSnapshot(
			URI backupUri,
			long checkpointId) {
		
		return new Snapshot<>(dbPath, checkpointPath, backupUri, checkpointId, keySerializer, namespaceSerializer, stateDesc);
	}

	private static class Snapshot<K, N, V> 
			extends AbstractRocksDBSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>>
	{
		private static final long serialVersionUID = 1L;

		public Snapshot(File dbPath,
			String checkpointPath,
			URI backupUri,
			long checkpointId,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc) {
			super(dbPath,
				checkpointPath,
				backupUri,
				checkpointId,
				keySerializer,
				namespaceSerializer,
				stateDesc);
		}

		@Override
		protected KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, RocksDBStateBackend> createRocksDBState(
				TypeSerializer<K> keySerializer,
				TypeSerializer<N> namespaceSerializer,
				ValueStateDescriptor<V> stateDesc,
				File dbPath,
				String backupPath,
				String restorePath,
				Options options) throws Exception {
			
			return new RocksDBValueState<>(keySerializer, namespaceSerializer, stateDesc, dbPath, 
					checkpointPath, restorePath, options);
		}
	}
}

