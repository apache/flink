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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;

import java.util.Collection;

/**
 * An {@link AggregatingState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key
 * @param <N> The type of the namespace
 * @param <T> The type of the values that aggregated into the state
 * @param <ACC> The type of the value stored in the state (the accumulator type)
 * @param <R> The type of the value returned from the state
 */
class RocksDBAggregatingState<K, N, T, ACC, R>
	extends AbstractRocksDBAppendingState<K, N, T, ACC, R, AggregatingState<T, R>>
	implements InternalAggregatingState<K, N, T, ACC, R> {

	/** User-specified aggregation function. */
	private final AggregateFunction<T, ACC, R> aggFunction;

	/**
	 * Creates a new {@code RocksDBAggregatingState}.
	 *
	 * @param columnFamily The RocksDB column family that this state is associated to.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param valueSerializer The serializer for the state.
	 * @param defaultValue The default value for the state.
	 * @param aggFunction The aggregate function used for aggregating state.
	 * @param backend The backend for which this state is bind to.
	 */
	private RocksDBAggregatingState(
			ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<ACC> valueSerializer,
			ACC defaultValue,
			AggregateFunction<T, ACC, R> aggFunction,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
		this.aggFunction = aggFunction;
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
	public R get() {
		ACC accumulator = getInternal();
		if (accumulator == null) {
			return null;
		}
		return aggFunction.getResult(accumulator);
	}

	@Override
	public void add(T value) {
		byte[] key = getKeyBytes();
		ACC accumulator = getInternal(key);
		accumulator = accumulator == null ? aggFunction.createAccumulator() : accumulator;
		updateInternal(key, aggFunction.add(value, accumulator));
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return;
		}

		// cache key and namespace
		final K key = backend.getCurrentKey();
		final int keyGroup = backend.getCurrentKeyGroupIndex();

		try {
			ACC current = null;

			// merge the sources to the target
			for (N source : sources) {
				if (source != null) {
					writeKeyWithGroupAndNamespace(
							keyGroup, key, source,
							keySerializationStream, keySerializationDataOutputView);

					final byte[] sourceKey = keySerializationStream.toByteArray();
					final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
					backend.db.delete(columnFamily, writeOptions, sourceKey);

					if (valueBytes != null) {
						ACC value = valueSerializer.deserialize(
								new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(valueBytes)));

						if (current != null) {
							current = aggFunction.merge(current, value);
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
					ACC value = valueSerializer.deserialize(
							new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(targetValueBytes)));

					current = aggFunction.merge(current, value);
				}

				// serialize the resulting value
				keySerializationStream.reset();
				valueSerializer.serialize(current, keySerializationDataOutputView);

				// write the resulting value
				backend.db.put(columnFamily, writeOptions, targetKey, keySerializationStream.toByteArray());
			}
		}
		catch (Exception e) {
			throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
		}
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, SV>> registerResult,
		RocksDBKeyedStateBackend<K> backend) {
		return (IS) new RocksDBAggregatingState<>(
			registerResult.f0,
			registerResult.f1.getNamespaceSerializer(),
			registerResult.f1.getStateSerializer(),
			stateDesc.getDefaultValue(),
			((AggregatingStateDescriptor<?, SV, ?>) stateDesc).getAggregateFunction(),
			backend);
	}
}
