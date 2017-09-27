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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * {@link ReducingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
public class RocksDBReducingState<K, N, V>
	extends AbstractRocksDBState<K, N, ReducingState<V>, ReducingStateDescriptor<V>, V>
	implements InternalReducingState<N, V> {

	/** Serializer for the values. */
	private final TypeSerializer<V> valueSerializer;

	/** User-specified reduce function. */
	private final ReduceFunction<V> reduceFunction;

	/**
	 * We disable writes to the write-ahead-log here. We can't have these in the base class
	 * because JNI segfaults for some reason if they are.
	 */
	private final WriteOptions writeOptions;

	/**
	 * Creates a new {@code RocksDBReducingState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                     and can create a default state value.
	 */
	public RocksDBReducingState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			ReducingStateDescriptor<V> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);
		this.valueSerializer = stateDesc.getSerializer();
		this.reduceFunction = stateDesc.getReduceFunction();

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
				return null;
			}
			return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
		} catch (IOException | RocksDBException e) {
			throw new RuntimeException("Error while retrieving data from RocksDB", e);
		}
	}

	@Override
	public void add(V value) throws IOException {
		try {
			writeCurrentKeyWithGroupAndNamespace();
			byte[] key = keySerializationStream.toByteArray();
			byte[] valueBytes = backend.db.get(columnFamily, key);

			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
			if (valueBytes == null) {
				keySerializationStream.reset();
				valueSerializer.serialize(value, out);
				backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
			} else {
				V oldValue = valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
				V newValue = reduceFunction.reduce(oldValue, value);
				keySerializationStream.reset();
				valueSerializer.serialize(newValue, out);
				backend.db.put(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		// cache key and namespace
		final K key = backend.getCurrentKey();
		final int keyGroup = backend.getCurrentKeyGroupIndex();

		try {
			V current = null;

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {

					writeKeyWithGroupAndNamespace(
							keyGroup, key, source,
							keySerializationStream, keySerializationDataOutputView);

					final byte[] sourceKey = keySerializationStream.toByteArray();
					final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, sourceKey);

					if (valueBytes != null) {
						V value = valueSerializer.deserialize(
								new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(valueBytes)));

						if (current != null) {
							current = reduceFunction.reduce(current, value);
						}
						else {
							current = value;
						}
					}
				}
			}

			// if something came out of merging the sources, merge it or write it to the target
			if (current != null) {
				// create the target full-binary-key
				writeKeyWithGroupAndNamespace(
						keyGroup, key, target,
						keySerializationStream, keySerializationDataOutputView);

				final byte[] targetKey = keySerializationStream.toByteArray();
				final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

				if (targetValueBytes != null) {
					// target also had a value, merge
					V value = valueSerializer.deserialize(
							new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(targetValueBytes)));

					current = reduceFunction.reduce(current, value);
				}

				// serialize the resulting value
				keySerializationStream.reset();
				valueSerializer.serialize(current, keySerializationDataOutputView);

				// write the resulting value
				backend.db.put(columnFamily, writeOptions, targetKey, keySerializationStream.toByteArray());
			}
		}
		catch (Exception e) {
			throw new Exception("Error while merging state in RocksDB", e);
		}
	}
}
