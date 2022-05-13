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
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

/**
 * This class wraps list state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user entry value of state
 */
class LatencyTrackingListState<K, N, T>
        extends AbstractLatencyTrackState<
                K,
                N,
                List<T>,
                InternalListState<K, N, T>,
                LatencyTrackingListState.ListStateLatencyMetrics>
        implements InternalListState<K, N, T> {

    LatencyTrackingListState(
            String stateName,
            InternalListState<K, N, T> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new ListStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize(),
                        latencyTrackingStateConfig.isStateNameAsVariable()));
    }

    @Override
    public Iterable<T> get() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnGet()) {
            return trackLatencyWithException(
                    () -> original.get(), ListStateLatencyMetrics.LIST_STATE_GET_LATENCY);
        } else {
            return original.get();
        }
    }

    @Override
    public void add(T value) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnAdd()) {
            trackLatencyWithException(
                    () -> original.add(value), ListStateLatencyMetrics.LIST_STATE_ADD_LATENCY);
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
        if (latencyTrackingStateMetric.trackLatencyOnUpdate()) {
            trackLatencyWithException(
                    () -> original.update(values),
                    ListStateLatencyMetrics.LIST_STATE_UPDATE_LATENCY);
        } else {
            original.update(values);
        }
    }

    @Override
    public void addAll(List<T> values) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnAddAll()) {
            trackLatencyWithException(
                    () -> original.addAll(values),
                    ListStateLatencyMetrics.LIST_STATE_ADD_ALL_LATENCY);
        } else {
            original.addAll(values);
        }
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnMergeNamespace()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    ListStateLatencyMetrics.LIST_STATE_MERGE_NAMESPACES_LATENCY);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    static class ListStateLatencyMetrics extends StateLatencyMetricBase {
        private static final String LIST_STATE_GET_LATENCY = "listStateGetLatency";
        private static final String LIST_STATE_ADD_LATENCY = "listStateAddLatency";
        private static final String LIST_STATE_ADD_ALL_LATENCY = "listStateAddAllLatency";
        private static final String LIST_STATE_UPDATE_LATENCY = "listStateUpdateLatency";
        private static final String LIST_STATE_MERGE_NAMESPACES_LATENCY =
                "listStateMergeNamespacesLatency";

        private int getCount = 0;
        private int addCount = 0;
        private int addAllCount = 0;
        private int updateCount = 0;
        private int mergeNamespaceCount = 0;

        private ListStateLatencyMetrics(
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

        private boolean trackLatencyOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackLatencyOnAdd() {
            addCount = loopUpdateCounter(addCount);
            return addCount == 1;
        }

        private boolean trackLatencyOnAddAll() {
            addAllCount = loopUpdateCounter(addAllCount);
            return addAllCount == 1;
        }

        private boolean trackLatencyOnUpdate() {
            updateCount = loopUpdateCounter(updateCount);
            return updateCount == 1;
        }

        private boolean trackLatencyOnMergeNamespace() {
            mergeNamespaceCount = loopUpdateCounter(mergeNamespaceCount);
            return mergeNamespaceCount == 1;
        }
    }
}
