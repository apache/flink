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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class wraps list state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state
 */
class MetricsTrackingListState<K, N, T>
        extends AbstractMetricsTrackState<
                K,
                N,
                List<T>,
                InternalListState<K, N, T>,
                MetricsTrackingListState.ListStateMetrics>
        implements InternalListState<K, N, T> {

    private TypeSerializer<T> elementSerializer;

    MetricsTrackingListState(
            String stateName,
            InternalListState<K, N, T> original,
            KeyedStateBackend<K> keyedStateBackend,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            SizeTrackingStateConfig sizeTrackingStateConfig) {
        super(
                original,
                keyedStateBackend,
                latencyTrackingStateConfig.isEnabled()
                        ? new ListStateMetrics(
                                stateName,
                                latencyTrackingStateConfig.getMetricGroup(),
                                latencyTrackingStateConfig.getSampleInterval(),
                                latencyTrackingStateConfig.getHistorySize(),
                                latencyTrackingStateConfig.isStateNameAsVariable())
                        : null,
                sizeTrackingStateConfig.isEnabled()
                        ? new ListStateMetrics(
                                stateName,
                                sizeTrackingStateConfig.getMetricGroup(),
                                sizeTrackingStateConfig.getSampleInterval(),
                                sizeTrackingStateConfig.getHistorySize(),
                                sizeTrackingStateConfig.isStateNameAsVariable())
                        : null);
        if (valueSerializer != null) {
            this.elementSerializer =
                    ((ListSerializer<T>) this.valueSerializer).getElementSerializer().duplicate();
        }
    }

    @Override
    public Iterable<T> get() throws Exception {
        Iterable<T> result;
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnGet()) {
            result =
                    trackLatencyWithException(
                            () -> original.get(), ListStateMetrics.LIST_STATE_GET_LATENCY);
        } else {
            result = original.get();
        }
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnGet()) {
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_GET_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_GET_VALUE_SIZE, sizeOfValueList(result));
        }
        return result;
    }

    @Override
    public void add(T value) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnAdd()) {
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_ADD_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_ADD_VALUE_SIZE,
                    sizeOfValueList(Collections.singletonList(value)));
        }
        if (latencyTrackingStateMetric != null && latencyTrackingStateMetric.trackMetricsOnAdd()) {
            trackLatencyWithException(
                    () -> original.add(value), ListStateMetrics.LIST_STATE_ADD_LATENCY);
        } else {
            original.add(value);
        }
    }

    @Override
    public List<T> getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(List<T> valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void update(List<T> values) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnUpdate()) {
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_UPDATE_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_UPDATE_VALUE_SIZE, sizeOfValueList(values));
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnUpdate()) {
            trackLatencyWithException(
                    () -> original.update(values), ListStateMetrics.LIST_STATE_UPDATE_LATENCY);
        } else {
            original.update(values);
        }
    }

    @Override
    public void addAll(List<T> values) throws Exception {
        if (sizeTrackingStateMetric != null && sizeTrackingStateMetric.trackMetricsOnAddAll()) {
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_ADD_ALL_KEY_SIZE, super.sizeOfKey());
            sizeTrackingStateMetric.updateMetrics(
                    ListStateMetrics.LIST_STATE_ADD_ALL_VALUE_SIZE, sizeOfValueList(values));
        }
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnAddAll()) {
            trackLatencyWithException(
                    () -> original.addAll(values), ListStateMetrics.LIST_STATE_ADD_ALL_LATENCY);
        } else {
            original.addAll(values);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (latencyTrackingStateMetric != null
                && latencyTrackingStateMetric.trackMetricsOnMergeNamespace()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    ListStateMetrics.LIST_STATE_MERGE_NAMESPACES_LATENCY);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    private long sizeOfValueList(Iterable<T> valueList) throws IOException {
        if (elementSerializer == null || valueList == null) {
            return 0;
        }
        long totalSize = 0;

        for (T value : valueList) {
            if (value == null) {
                continue;
            }
            if (elementSerializer.getLength() == -1) {
                try {
                    elementSerializer.serialize(value, outputSerializer);
                    totalSize += outputSerializer.length();
                } finally {
                    outputSerializer.clear();
                }
            } else {
                totalSize += elementSerializer.getLength();
            }
        }
        return totalSize;
    }

    static class ListStateMetrics extends StateMetricBase {
        private static final String LIST_STATE_GET_LATENCY = "listStateGetLatency";
        private static final String LIST_STATE_ADD_LATENCY = "listStateAddLatency";
        private static final String LIST_STATE_ADD_ALL_LATENCY = "listStateAddAllLatency";
        private static final String LIST_STATE_UPDATE_LATENCY = "listStateUpdateLatency";
        private static final String LIST_STATE_MERGE_NAMESPACES_LATENCY =
                "listStateMergeNamespacesLatency";
        private static final String LIST_STATE_GET_KEY_SIZE = "listStateGetKeySize";
        private static final String LIST_STATE_GET_VALUE_SIZE = "listStateGetValueSize";
        private static final String LIST_STATE_ADD_KEY_SIZE = "listStateAddKeySize";
        private static final String LIST_STATE_ADD_VALUE_SIZE = "listStateAddValueSize";
        private static final String LIST_STATE_ADD_ALL_KEY_SIZE = "listStateAddAllKeySize";
        private static final String LIST_STATE_ADD_ALL_VALUE_SIZE = "listStateAddAllValueSize";
        private static final String LIST_STATE_UPDATE_KEY_SIZE = "listStateUpdateKeySize";
        private static final String LIST_STATE_UPDATE_VALUE_SIZE = "listStateUpdateValueSize";

        private int getCount = 0;
        private int addCount = 0;
        private int addAllCount = 0;
        private int updateCount = 0;
        private int mergeNamespaceCount = 0;

        private ListStateMetrics(
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

        int getAddAllCount() {
            return addAllCount;
        }

        int getUpdateCount() {
            return updateCount;
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

        private boolean trackMetricsOnAddAll() {
            addAllCount = loopUpdateCounter(addAllCount);
            return addAllCount == 1;
        }

        private boolean trackMetricsOnUpdate() {
            updateCount = loopUpdateCounter(updateCount);
            return updateCount == 1;
        }

        private boolean trackMetricsOnMergeNamespace() {
            mergeNamespaceCount = loopUpdateCounter(mergeNamespaceCount);
            return mergeNamespaceCount == 1;
        }
    }
}
