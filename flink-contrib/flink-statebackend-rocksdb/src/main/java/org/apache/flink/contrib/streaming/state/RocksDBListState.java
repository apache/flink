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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
public class RocksDBListState<K, N, V>
	extends AbstractRocksDBState<K, N, ListState<V>, ListStateDescriptor<V>, V>
	implements ListState<V> {

	/** Serializer for the values */
	private final TypeSerializer<V> valueSerializer;

	/**
	 * We disable writes to the write-ahead-log here. We can't have these in the base class
	 * because JNI segfaults for some reason if they are.
	 */
	private final WriteOptions writeOptions;

	/**
	 * Creates a new {@code RocksDBListState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                     and can create a default state value.
	 */
	public RocksDBListState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<V> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);
		this.valueSerializer = stateDesc.getSerializer();

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
	}

	@Override
	public Iterable<V> get() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			byte[] valueBytes = backend.db.get(columnFamily, key);

			if (valueBytes == null) {
				return null;
			}

			ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes);
			DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

			List<V> result = new ArrayList<>();
			while (in.available() > 0) {
				result.add(valueSerializer.deserialize(in));
				if (in.available() > 0) {
					in.readByte();
				}
			}
			return result;
		} catch (IOException|RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void add(V value) throws IOException {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			keySerializationStream.reset();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
			valueSerializer.serialize(value, out);
			backend.db.merge(columnFamily, writeOptions, key, keySerializationStream.toByteArray());

		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}

}
