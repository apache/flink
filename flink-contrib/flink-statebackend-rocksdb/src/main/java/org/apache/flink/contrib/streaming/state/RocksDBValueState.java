/**
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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.KvStateSnapshot;
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
 * @param <Backend> The type of the backend that snapshots this key/value state.
 */
public class RocksDBValueState<K, N, V, Backend extends AbstractStateBackend>
	extends AbstractRocksDBState<K, N, ValueState<V>, ValueStateDescriptor<V>, Backend>
	implements ValueState<V> {

	/** Serializer for the values */
	private final TypeSerializer<V> valueSerializer;

	/** This holds the name of the state and can create an initial default value for the state. */
	protected final ValueStateDescriptor<V> stateDesc;

	/**
	 * Creates a new {@code RocksDBReducingState}.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param dbPath The path on the local system where RocksDB data should be stored.
	 */
	protected RocksDBValueState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc,
		File dbPath,
		String backupPath) {
		super(keySerializer, namespaceSerializer, dbPath, backupPath);
		this.stateDesc = requireNonNull(stateDesc);
		this.valueSerializer = stateDesc.getSerializer();
	}

	protected RocksDBValueState(TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ValueStateDescriptor<V> stateDesc,
		File dbPath,
		String backupPath,
		String restorePath) {
		super(keySerializer, namespaceSerializer, dbPath, backupPath, restorePath);
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
	protected KvStateSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, Backend> createRocksDBSnapshot(
		URI backupUri,
		long checkpointId) {
		return new Snapshot<>(dbPath, checkpointPath, backupUri, checkpointId, keySerializer, namespaceSerializer, stateDesc);
	}

	private static class Snapshot<K, N, V, Backend extends AbstractStateBackend> extends AbstractRocksDBSnapshot<K, N, ValueState<V>, ValueStateDescriptor<V>, Backend> {
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
		protected KvState<K, N, ValueState<V>, ValueStateDescriptor<V>, Backend> createRocksDBState(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			File dbPath,
			String backupPath,
			String restorePath) throws Exception {
			return new RocksDBValueState<>(keySerializer, namespaceSerializer, stateDesc, dbPath, checkpointPath, restorePath);
		}
	}
}

