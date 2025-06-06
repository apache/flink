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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.IOException;

/**
 * This class wraps value state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state
 */
class MetricsTrackingValueState<K, N, T>
        extends AbstractMetricsTrackState<
                K, N, T, InternalValueState<K, N, T>, MetricsTrackingValueState.ValueStateMetrics>
        implements InternalValueState<K, N, T> {

    public MetricsTrackingValueState(
            String stateName,
            InternalValueState<K, N, T> original,
            KeyedStateBackend<K> keyedStateBackend,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            SizeTrackingStateConfig sizeTrackingStateConfig) {
        super(
                original,
                keyedStateBackend,
                latencyTrackingStateConfig.isEnabled()
                        ? new ValueStateMetrics(
                                stateName,
                                latencyTrackingStateConfig.getMetricGroup(),
                                latencyTrackingStateConfig.getSampleInterval(),
                                latencyTrackingStateConfig.getHistorySize(),
                                latencyTrackingStateConfig.isStateNameAsVariable())
                        : null,
                sizeTrackingStateConfig.isEnabled()
                        ? new ValueStateMetrics(
                                stateName,
                                sizeTrackingStateConfig.getMetricGroup(),
                                sizeTrackingStateConfig.getSampleInterval(),
                                sizeTrackingStateConfig.getHistorySize(),
                                sizeTrackingStateConfig.isStateNameAsVariable())
                        : null);
    }

    @Override
    public T value() throws IOException {
        T result;
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnGet()) {
            result =
                    trackLatencyWithIOException(
                            () -> original.value(), ValueStateMetrics.VALUE_STATE_GET_LATENCY);
        } else {
            result = original.value();
        }
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnGet()) {
            sizeTrackingStateMetric.updateMetrics(
                    ValueStateMetrics.VALUE_STATE_GET_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ValueStateMetrics.VALUE_STATE_GET_VALUE_SIZE, super.sizeOfValue(result));
        }
        return result;
    }

    @Override
    public void update(T value) throws IOException {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnUpdate()) {
            sizeTrackingStateMetric.updateMetrics(
                    ValueStateMetrics.VALUE_STATE_UPDATE_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ValueStateMetrics.VALUE_STATE_UPDATE_VALUE_SIZE, super.sizeOfValue(value));
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnUpdate()) {
            trackLatencyWithIOException(
                    () -> original.update(value), ValueStateMetrics.VALUE_STATE_UPDATE_LATENCY);
        } else {
            original.update(value);
        }
    }

    static class ValueStateMetrics extends StateMetricBase {
        private static final String VALUE_STATE_GET_LATENCY = "valueStateGetLatency";
        private static final String VALUE_STATE_UPDATE_LATENCY = "valueStateUpdateLatency";
        private static final String VALUE_STATE_UPDATE_KEY_SIZE = "valueStateUpdateKeySize";
        private static final String VALUE_STATE_UPDATE_VALUE_SIZE = "valueStateUpdateValueSize";
        private static final String VALUE_STATE_GET_KEY_SIZE = "valueStateGetKeySize";
        private static final String VALUE_STATE_GET_VALUE_SIZE = "valueStateGetValueSize";

        private int getCount = 0;
        private int updateCount = 0;

        private ValueStateMetrics(
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

        int getUpdateCount() {
            return updateCount;
        }

        private boolean trackMetricsOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackMetricsOnUpdate() {
            updateCount = loopUpdateCounter(updateCount);
            return updateCount == 1;
        }
    }
}
