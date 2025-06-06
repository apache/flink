/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Abstract implementation of metrics tracking state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> Type of the user entry value of state
 * @param <S> Type of the internal kv state
 * @param <LSM> Type of the metrics tracking state metrics
 */
class AbstractMetricsTrackState<
                K, N, V, S extends InternalKvState<K, N, V>, LSM extends StateMetricBase>
        implements InternalKvState<K, N, V> {

    protected S original;
    protected LSM latencyTrackingStateMetric;
    protected LSM sizeTrackingStateMetric;
    protected KeyedStateBackend<K> keyedStateBackend;
    protected N currentNamespace;
    protected final TypeSerializer<K> keySerializer;
    protected final TypeSerializer<N> namespaceSerializer;
    protected final TypeSerializer<V> valueSerializer;
    protected final DataOutputSerializer outputSerializer;
    protected long namespaceSize;
    protected long keySize;
    protected long valueSize;

    AbstractMetricsTrackState(
            S original,
            KeyedStateBackend<K> keyedStateBackend,
            LSM latencyTrackingStateMetric,
            LSM sizeTrackingStateMetric) {
        this.original = original;
        this.keyedStateBackend = keyedStateBackend;
        this.latencyTrackingStateMetric = latencyTrackingStateMetric;
        this.sizeTrackingStateMetric = sizeTrackingStateMetric;
        this.keySerializer = getKeySerializer() != null ? getKeySerializer().duplicate() : null;
        this.namespaceSerializer =
                getNamespaceSerializer() != null ? getNamespaceSerializer().duplicate() : null;
        this.valueSerializer =
                getValueSerializer() != null ? getValueSerializer().duplicate() : null;
        this.outputSerializer = new DataOutputSerializer(64);
        initStateSize();
    }

    private void initStateSize() {
        if (keySerializer != null) {
            this.keySize = keySerializer.getLength();
        }
        if (valueSerializer != null) {
            this.valueSize = valueSerializer.getLength();
        }
        if (namespaceSerializer != null) {
            this.namespaceSize = namespaceSerializer.getLength();
        }
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return original.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return original.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return original.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        original.setCurrentNamespace(namespace);
        this.currentNamespace = namespace;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return original.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return original.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public void clear() {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnClear()) {
            trackLatency(original::clear, StateMetricBase.STATE_CLEAR_LATENCY);
        } else {
            original.clear();
        }
    }

    protected <T> T trackLatency(Supplier<T> supplier, String latencyLabel) {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
        return result;
    }

    protected <T> T trackLatencyWithIOException(
            SupplierWithException<T, IOException> supplier, String latencyLabel)
            throws IOException {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
        return result;
    }

    protected void trackLatencyWithIOException(
            ThrowingRunnable<IOException> runnable, String latencyLabel) throws IOException {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
    }

    protected <T> T trackLatencyWithException(
            SupplierWithException<T, Exception> supplier, String latencyLabel) throws Exception {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
        return result;
    }

    protected void trackLatencyWithException(
            ThrowingRunnable<Exception> runnable, String latencyLabel) throws Exception {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
    }

    protected void trackLatency(Runnable runnable, String latencyLabel) {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateMetrics(latencyLabel, latency);
    }

    protected K getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    public final N getCurrentNamespace() {
        return currentNamespace;
    }

    protected long sizeOfKey() throws IOException {
        if (keySerializer != null && keySerializer.getLength() == -1) {
            try {
                keySerializer.serialize(keyedStateBackend.getCurrentKey(), outputSerializer);
                keySize = outputSerializer.length();
            } finally {
                outputSerializer.clear();
            }
        }

        if (namespaceSerializer != null && namespaceSerializer.getLength() == -1) {
            try {
                namespaceSerializer.serialize(getCurrentNamespace(), outputSerializer);
                namespaceSize = outputSerializer.length();
            } finally {
                outputSerializer.clear();
            }
        }

        return keySize + namespaceSize;
    }

    protected long sizeOfValue(V value) throws IOException {
        if (valueSerializer == null || value == null) {
            valueSize = 0;
        } else if (valueSerializer.getLength() == -1) {
            try {
                valueSerializer.serialize(value, outputSerializer);
                valueSize = outputSerializer.length();
            } finally {
                outputSerializer.clear();
            }
        }
        return valueSize;
    }

    @VisibleForTesting
    LSM getLatencyTrackingStateMetric() {
        return latencyTrackingStateMetric;
    }

    @VisibleForTesting
    LSM getSizeTrackingStateMetric() {
        return sizeTrackingStateMetric;
    }
}
