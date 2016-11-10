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

import org.apache.flink.api.common.state.SimpleStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.UpdatableState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * The implementation that stores simple states in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBSimpleState<K, N, V> extends AbstractRocksDBState<K, N> implements UpdatableState<V> {

	/** State descriptor from which to create this state instance */
	protected final SimpleStateDescriptor<V, ? extends State<V>> stateDesc;

	/** Serializer for the values */
	protected final TypeSerializer<V> valueSerializer;

	/**
	 * We disable writes to the write-ahead-log here. We can't have these in the base class
	 * because JNI segfaults for some reason if they are.
	 */
	protected final WriteOptions writeOptions;

	/**
	 * Creates a new {@code RocksDBSimpleState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name and can create a default state value.
	 */
	public RocksDBSimpleState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			SimpleStateDescriptor<V, ? extends State<V>> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, backend);

		this.stateDesc = Preconditions.checkNotNull(stateDesc, "State Descriptor");
		this.valueSerializer = stateDesc.getSerializer();

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
	}

	@Override
	public V get() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			byte[] valueBytes = backend.db.get(columnFamily, key);
			if (valueBytes == null) {
				return stateDesc.getDefaultValue();
			}
			return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB.", e);
		}
	}

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
}
