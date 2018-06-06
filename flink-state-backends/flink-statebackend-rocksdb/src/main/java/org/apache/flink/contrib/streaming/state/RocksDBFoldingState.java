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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;

/**
 * {@link FoldingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 *
 * @deprecated will be removed in a future version
 */
@Deprecated
public class RocksDBFoldingState<K, N, T, ACC>
		extends AbstractRocksDBState<K, N, ACC, FoldingState<T, ACC>>
		implements InternalFoldingState<K, N, T, ACC> {

	/** User-specified fold function. */
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code RocksDBFoldingState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param foldFunction The fold function used for folding state.
	 * @param backend The backend for which this state is bind to.
	 */
	public RocksDBFoldingState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<ACC> valueSerializer,
			ACC defaultValue,
			FoldFunction<T, ACC> foldFunction,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

		this.foldFunction = foldFunction;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return backend.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<ACC> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public ACC get() {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			byte[] valueBytes = backend.db.get(columnFamily, key);
			if (valueBytes == null) {
				return null;
			}
			return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(valueBytes)));
		} catch (IOException | RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void add(T value) throws IOException {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			byte[] valueBytes = backend.db.get(columnFamily, key);
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
			if (valueBytes == null) {
				keySerializationStream.reset();
				valueSerializer.serialize(foldFunction.fold(getDefaultValue(), value), out);
				backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
			} else {
				ACC oldValue = valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(valueBytes)));
				ACC newValue = foldFunction.fold(oldValue, value);
				keySerializationStream.reset();
				valueSerializer.serialize(newValue, out);
				backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}
}
