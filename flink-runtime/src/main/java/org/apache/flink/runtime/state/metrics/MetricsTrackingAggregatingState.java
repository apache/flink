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
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.util.Collection;

/**
 * This class wraps aggregating state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> Type of the value extracted from the state
 */
class MetricsTrackingAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractMetricsTrackState<
                K,
                N,
                ACC,
                InternalAggregatingState<K, N, IN, ACC, OUT>,
                MetricsTrackingAggregatingState.AggregatingStateMetrics>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    MetricsTrackingAggregatingState(
            String stateName,
            InternalAggregatingState<K, N, IN, ACC, OUT> original,
            KeyedStateBackend<K> keyedStateBackend,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            SizeTrackingStateConfig sizeTrackingStateConfig) {
        super(
                original,
                keyedStateBackend,
                latencyTrackingStateConfig.isEnabled()
                        ? new AggregatingStateMetrics(
                                stateName,
                                latencyTrackingStateConfig.getMetricGroup(),
                                latencyTrackingStateConfig.getSampleInterval(),
                                latencyTrackingStateConfig.getHistorySize(),
                                latencyTrackingStateConfig.isStateNameAsVariable())
                        : null,
                sizeTrackingStateConfig.isEnabled()
                        ? new AggregatingStateMetrics(
                                stateName,
                                sizeTrackingStateConfig.getMetricGroup(),
                                sizeTrackingStateConfig.getSampleInterval(),
                                sizeTrackingStateConfig.getHistorySize(),
                                sizeTrackingStateConfig.isStateNameAsVariable())
                        : null);
    }

    @Override
    public OUT get() throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnGet()) {
            sizeTrackingStateMetric.updateMetrics(
                    AggregatingStateMetrics.AGGREGATING_STATE_GET_KEY_SIZE, super.sizeOfKey());
        }
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnGet()) {
            return trackLatencyWithException(
                    () -> original.get(), AggregatingStateMetrics.AGGREGATING_STATE_GET_LATENCY);
        } else {
            return original.get();
        }
    }

    @Override
    public void add(IN value) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnAdd()) {
            sizeTrackingStateMetric.updateMetrics(
                    AggregatingStateMetrics.AGGREGATING_STATE_ADD_KEY_SIZE, super.sizeOfKey());
        }
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnAdd()) {
            trackLatencyWithException(
                    () -> original.add(value),
                    AggregatingStateMetrics.AGGREGATING_STATE_ADD_LATENCY);
        } else {
            original.add(value);
        }
    }

    @Override
    public ACC getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnMergeNamespace()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    AggregatingStateMetrics.AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    static class AggregatingStateMetrics extends StateMetricBase {
        private static final String AGGREGATING_STATE_GET_LATENCY = "aggregatingStateGetLatency";
        private static final String AGGREGATING_STATE_ADD_LATENCY = "aggregatingStateAddLatency";
        private static final String AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY =
                "aggregatingStateMergeNamespacesLatency";
        private static final String AGGREGATING_STATE_GET_KEY_SIZE = "aggregatingStateGetKeySize";
        private static final String AGGREGATING_STATE_ADD_KEY_SIZE = "aggregatingStateAddKeySize";

        private int getCount = 0;
        private int addCount = 0;
        private int mergeNamespaceCount = 0;

        private AggregatingStateMetrics(
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
