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
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Abstract implementation of latency tracking state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> Type of the user entry value of state
 * @param <S> Type of the internal kv state
 * @param <LSM> Type of the latency tracking state metrics
 */
class AbstractLatencyTrackState<
                K, N, V, S extends InternalKvState<K, N, V>, LSM extends StateLatencyMetricBase>
        implements InternalKvState<K, N, V> {

    protected S original;
    protected LSM latencyTrackingStateMetric;

    AbstractLatencyTrackState(S original, LSM latencyTrackingStateMetric) {
        this.original = original;
        this.latencyTrackingStateMetric = latencyTrackingStateMetric;
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
        if (latencyTrackingStateMetric.trackLatencyOnClear()) {
            trackLatency(original::clear, StateLatencyMetricBase.STATE_CLEAR_LATENCY);
        } else {
            original.clear();
        }
    }

    protected <T> T trackLatency(Supplier<T> supplier, String latencyLabel) {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
        return result;
    }

    protected <T> T trackLatencyWithIOException(
            SupplierWithException<T, IOException> supplier, String latencyLabel)
            throws IOException {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
        return result;
    }

    protected void trackLatencyWithIOException(
            ThrowingRunnable<IOException> runnable, String latencyLabel) throws IOException {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
    }

    protected <T> T trackLatencyWithException(
            SupplierWithException<T, Exception> supplier, String latencyLabel) throws Exception {
        long startTime = System.nanoTime();
        T result = supplier.get();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
        return result;
    }

    protected void trackLatencyWithException(
            ThrowingRunnable<Exception> runnable, String latencyLabel) throws Exception {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
    }

    protected void trackLatency(Runnable runnable, String latencyLabel) {
        long startTime = System.nanoTime();
        runnable.run();
        long latency = System.nanoTime() - startTime;
        latencyTrackingStateMetric.updateLatency(latencyLabel, latency);
    }

    @VisibleForTesting
    LSM getLatencyTrackingStateMetric() {
        return latencyTrackingStateMetric;
    }
}
