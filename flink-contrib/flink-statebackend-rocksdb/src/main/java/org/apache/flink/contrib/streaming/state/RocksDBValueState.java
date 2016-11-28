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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBValueState<K, N, V> extends RocksDBSimpleState<K, N, V, ValueStateDescriptor<V>> implements ValueState<V> {
	/**
	 * Creates a new {@code RocksDBValueState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name and can create a default state value.
	 */
	public RocksDBValueState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			ValueStateDescriptor<V> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {
		super(columnFamily, namespaceSerializer, stateDesc, backend);
	}

	@Override
	public V get() {
		V ret = super.get();

		return (ret == null ? stateDesc.getDefaultValue() : ret);
	}

	@Override
	public V value() throws IOException {
		return get();
	}

	@Override
	public void update(V value) throws IOException {
		if (value == null) {
			clear();
			return;
		}

		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();

			keySerializationStream.reset();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
			valueSerializer.serialize(value, out);

			backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace) throws Exception {
		byte[] value = super.getSerializedValue(serializedKeyAndNamespace);

		if (value != null) {
			return value;
		} else {
			return KvStateRequestSerializer.serializeValue(stateDesc.getDefaultValue(), stateDesc.getSerializer());
		}
	}
}
