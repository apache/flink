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
import org.apache.flink.api.common.state.FoldingStateDescriptor;
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
	extends AbstractRocksDBState<K, N, FoldingState<T, ACC>, FoldingStateDescriptor<T, ACC>, ACC>
	implements InternalFoldingState<N, T, ACC> {

	/** Serializer for the values. */
	private final TypeSerializer<ACC> valueSerializer;

	/** User-specified fold function. */
	private final FoldFunction<T, ACC> foldFunction;

	/**
	 * Creates a new {@code RocksDBFoldingState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                     and can create a default state value.
	 */
	public RocksDBFoldingState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			FoldingStateDescriptor<T, ACC> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);

		this.valueSerializer = stateDesc.getSerializer();
		this.foldFunction = stateDesc.getFoldFunction();
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
				valueSerializer.serialize(foldFunction.fold(stateDesc.getDefaultValue(), value), out);
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
