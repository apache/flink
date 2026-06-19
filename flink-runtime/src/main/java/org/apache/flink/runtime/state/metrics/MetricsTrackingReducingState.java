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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.Collection;

/**
 * This class wraps reducing state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state
 */
class MetricsTrackingReducingState<K, N, T>
        extends AbstractMetricsTrackState<
                K,
                N,
                T,
                InternalReducingState<K, N, T>,
                MetricsTrackingReducingState.ReducingStateMetrics>
        implements InternalReducingState<K, N, T> {

    MetricsTrackingReducingState(
            String stateName,
            InternalReducingState<K, N, T> original,
            KeyedStateBackend<K> keyedStateBackend,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            SizeTrackingStateConfig sizeTrackingStateConfig) {
        super(
                original,
                keyedStateBackend,
                new ReducingStateMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize(),
                        latencyTrackingStateConfig.isStateNameAsVariable()),
                new ReducingStateMetrics(
                        stateName,
                        sizeTrackingStateConfig.getMetricGroup(),
                        sizeTrackingStateConfig.getSampleInterval(),
                        sizeTrackingStateConfig.getHistorySize(),
                        sizeTrackingStateConfig.isStateNameAsVariable()));
    }

    @Override
    public T get() throws Exception {
        T result;
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnGet()) {
            result =
                    trackLatencyWithException(
                            () -> original.get(), ReducingStateMetrics.REDUCING_STATE_GET_LATENCY);
        } else {
            result = original.get();
        }
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnGet()) {
            sizeTrackingStateMetric.updateMetrics(
                    ReducingStateMetrics.REDUCING_STATE_GET_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ReducingStateMetrics.REDUCING_STATE_GET_VALUE_SIZE, super.sizeOfValue(result));
        }
        return result;
    }

    @Override
    public void add(T value) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnAdd()) {
            sizeTrackingStateMetric.updateMetrics(
                    ReducingStateMetrics.REDUCING_STATE_ADD_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ReducingStateMetrics.REDUCING_STATE_ADD_VALUE_SIZE, super.sizeOfValue(value));
        }
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnAdd()) {
            trackLatencyWithException(
                    () -> original.add(value), ReducingStateMetrics.REDUCING_STATE_ADD_LATENCY);
        } else {
            original.add(value);
        }
    }

    @Override
    public T getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(T valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnMergeNamespace()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    ReducingStateMetrics.REDUCING_STATE_MERGE_NAMESPACES_LATENCY);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    protected static class ReducingStateMetrics extends StateMetricBase {
        private static final String REDUCING_STATE_GET_LATENCY = "reducingStateGetLatency";
        private static final String REDUCING_STATE_ADD_LATENCY = "reducingStateAddLatency";
        private static final String REDUCING_STATE_MERGE_NAMESPACES_LATENCY =
                "reducingStateMergeNamespacesLatency";
        private static final String REDUCING_STATE_GET_KEY_SIZE = "reducingStateGetKeySize";
        private static final String REDUCING_STATE_GET_VALUE_SIZE = "reducingStateGetValueSize";
        private static final String REDUCING_STATE_ADD_KEY_SIZE = "reducingStateAddKeySize";
        private static final String REDUCING_STATE_ADD_VALUE_SIZE = "reducingStateAddValueSize";

        private int getCount = 0;
        private int addCount = 0;
        private int mergeNamespaceCount = 0;

        ReducingStateMetrics(
                String stateName,
                MetricGroup metricGroup,
                int sampleInterval,
                int historySize,
                boolean stateNameAsVariable) {
            super(stateName, metricGroup, sampleInterval, historySize, stateNameAsVariable);
        }

        int getGetCount() {
            return getCount;
        }

        int getAddCount() {
            return addCount;
        }

        int getMergeNamespaceCount() {
            return mergeNamespaceCount;
        }

        private boolean trackMetricsOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackMetricsOnAdd() {
            addCount = loopUpdateCounter(addCount);
            return addCount == 1;
        }

        private boolean trackMetricsOnMergeNamespace() {
            mergeNamespaceCount = loopUpdateCounter(mergeNamespaceCount);
            return mergeNamespaceCount == 1;
        }
    }
}
