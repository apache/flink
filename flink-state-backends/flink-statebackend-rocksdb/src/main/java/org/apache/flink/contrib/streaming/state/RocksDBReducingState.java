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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Collection;

/**
 * {@link ReducingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RocksDBReducingState<K, N, V>
	extends AbstractRocksDBAppendingState<K, N, V, V, V>
	implements InternalReducingState<K, N, V> {

	/** User-specified reduce function. */
	private final ReduceFunction<V> reduceFunction;

	/**
	 * Creates a new {@code RocksDBReducingState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param reduceFunction The reduce function used for reducing state.
	 * @param backend The backend for which this state is bind to.
	 */
	private RocksDBReducingState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<V> valueSerializer,
			V defaultValue,
			ReduceFunction<V> reduceFunction,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
		this.reduceFunction = reduceFunction;
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
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public V get() {
		return getInternal();
	}

	@Override
	public void add(V value) throws Exception {
		byte[] key = getKeyBytes();
		V oldValue = getInternal(key);
		V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
		updateInternal(key, newValue);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		try {
			V current = null;

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					setCurrentNamespace(source);
					final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
					final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, writeOptions, sourceKey);

					if (valueBytes != null) {
						dataInputView.setBuffer(valueBytes);
						V value = valueSerializer.deserialize(dataInputView);

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
				setCurrentNamespace(target);
				final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
				final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

				if (targetValueBytes != null) {
					dataInputView.setBuffer(targetValueBytes);
					// target also had a value, merge
					V value = valueSerializer.deserialize(dataInputView);

					current = reduceFunction.reduce(current, value);
				}

				// serialize the resulting value
				dataOutputView.clear();
				valueSerializer.serialize(current, dataOutputView);

				// write the resulting value
				backend.db.put(columnFamily, writeOptions, targetKey, dataOutputView.getCopyOfBuffer());
			}
		}
		catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
		}
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
		RocksDBKeyedStateBackend<K> backend) {
		return (IS) new RocksDBReducingState<>(
			registerResult.f0,
			registerResult.f1.getNamespaceSerializer(),
			registerResult.f1.getStateSerializer(),
			stateDesc.getDefaultValue(),
			((ReducingStateDescriptor<SV>) stateDesc).getReduceFunction(),
			backend);
	}
}
