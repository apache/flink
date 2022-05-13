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
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.Collection;

/**
 * This class wraps reducing state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state
 */
class LatencyTrackingReducingState<K, N, T>
        extends AbstractLatencyTrackState<
                K,
                N,
                T,
                InternalReducingState<K, N, T>,
                LatencyTrackingReducingState.ReducingStateLatencyMetrics>
        implements InternalReducingState<K, N, T> {

    LatencyTrackingReducingState(
            String stateName,
            InternalReducingState<K, N, T> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new ReducingStateLatencyMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getHistorySize(),
                        latencyTrackingStateConfig.isStateNameAsVariable()));
    }

    @Override
    public T get() throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnGet()) {
            return trackLatencyWithException(
                    () -> original.get(), ReducingStateLatencyMetrics.REDUCING_STATE_GET_LATENCY);
        } else {
            return original.get();
        }
    }

    @Override
    public void add(T value) throws Exception {
        if (latencyTrackingStateMetric.trackLatencyOnAdd()) {
            trackLatencyWithException(
                    () -> original.add(value),
                    ReducingStateLatencyMetrics.REDUCING_STATE_ADD_LATENCY);
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
        if (latencyTrackingStateMetric.trackLatencyOnMergeNamespace()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    ReducingStateLatencyMetrics.REDUCING_STATE_MERGE_NAMESPACES_LATENCY);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    protected static class ReducingStateLatencyMetrics extends StateLatencyMetricBase {
        private static final String REDUCING_STATE_GET_LATENCY = "reducingStateGetLatency";
        private static final String REDUCING_STATE_ADD_LATENCY = "reducingStateAddLatency";
        private static final String REDUCING_STATE_MERGE_NAMESPACES_LATENCY =
                "reducingStateMergeNamespacesLatency";

        private int getCount = 0;
        private int addCount = 0;
        private int mergeNamespaceCount = 0;

        ReducingStateLatencyMetrics(
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

        private boolean trackLatencyOnGet() {
            getCount = loopUpdateCounter(getCount);
            return getCount == 1;
        }

        private boolean trackLatencyOnAdd() {
            addCount = loopUpdateCounter(addCount);
            return addCount == 1;
        }

        private boolean trackLatencyOnMergeNamespace() {
            mergeNamespaceCount = loopUpdateCounter(mergeNamespaceCount);
            return mergeNamespaceCount == 1;
        }
    }
}
