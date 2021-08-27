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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
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
        extends AbstractRocksDBAppendingState<K, N, T, ACC, R>
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

        try {
            ACC current = null;

            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    setCurrentNamespace(source);
                    final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
                    final byte[] valueBytes = backend.db.get(columnFamily, sourceKey);

                    if (valueBytes != null) {
                        backend.db.delete(columnFamily, writeOptions, sourceKey);
                        dataInputView.setBuffer(valueBytes);
                        ACC value = valueSerializer.deserialize(dataInputView);

                        if (current != null) {
                            current = aggFunction.merge(current, value);
                        } else {
                            current = value;
                        }
                    }
                }
            }

            // if something came out of merging the sources, merge it or write it to the target
            if (current != null) {
                setCurrentNamespace(target);
                // create the target full-binary-key
                final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
                final byte[] targetValueBytes = backend.db.get(columnFamily, targetKey);

                if (targetValueBytes != null) {
                    // target also had a value, merge
                    dataInputView.setBuffer(targetValueBytes);
                    ACC value = valueSerializer.deserialize(dataInputView);

                    current = aggFunction.merge(current, value);
                }

                // serialize the resulting value
                dataOutputView.clear();
                valueSerializer.serialize(current, dataOutputView);

                // write the resulting value
                backend.db.put(
                        columnFamily, writeOptions, targetKey, dataOutputView.getCopyOfBuffer());
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while merging state in RocksDB", e);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBAggregatingState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        ((AggregatingStateDescriptor<?, SV, ?>) stateDesc).getAggregateFunction(),
                        backend);
    }
}
