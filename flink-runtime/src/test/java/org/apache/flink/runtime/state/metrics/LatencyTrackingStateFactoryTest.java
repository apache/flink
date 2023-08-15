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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link LatencyTrackingStateFactory}. */
@ExtendWith(ParameterizedTestExtension.class)
public class LatencyTrackingStateFactoryTest {

    @Parameter public boolean enableLatencyTracking;

    @Parameters(name = "enable latency tracking: {0}")
    public static Collection<Boolean> enabled() {
        return Arrays.asList(true, false);
    }

    private LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        return LatencyTrackingStateConfig.newBuilder()
                .setEnabled(enableLatencyTracking)
                .setMetricGroup(metricsGroup)
                .build();
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackValueState() throws Exception {
        InternalValueState<K, N, String> valueState = mock(InternalValueState.class);
        ValueStateDescriptor<String> valueStateDescriptor =
                new ValueStateDescriptor<>("value", String.class);
        InternalKvState<K, ?, ?> latencyTrackingState =
                LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                        valueState, valueStateDescriptor, getLatencyTrackingStateConfig());
        if (enableLatencyTracking) {
            assertThat(latencyTrackingState).isInstanceOf(LatencyTrackingValueState.class);
        } else {
            assertThat(latencyTrackingState).isEqualTo(valueState);
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackListState() throws Exception {
        InternalListState<K, N, String> listState = mock(InternalListState.class);
        ListStateDescriptor<String> listStateDescriptor =
                new ListStateDescriptor<>("list", String.class);
        InternalKvState<K, N, ?> latencyTrackingState =
                LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                        listState, listStateDescriptor, getLatencyTrackingStateConfig());
        if (enableLatencyTracking) {
            assertThat(latencyTrackingState).isInstanceOf(LatencyTrackingListState.class);
        } else {
            assertThat(latencyTrackingState).isEqualTo(listState);
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackMapState() throws Exception {
        InternalMapState<K, N, String, Long> mapState = mock(InternalMapState.class);
        MapStateDescriptor<String, Long> mapStateDescriptor =
                new MapStateDescriptor<>("map", String.class, Long.class);
        InternalKvState<K, N, ?> latencyTrackingState =
                LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                        mapState, mapStateDescriptor, getLatencyTrackingStateConfig());
        if (enableLatencyTracking) {
            assertThat(latencyTrackingState).isInstanceOf(LatencyTrackingMapState.class);
        } else {
            assertThat(latencyTrackingState).isEqualTo(mapState);
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackReducingState() throws Exception {
        InternalReducingState<K, N, Long> reducingState = mock(InternalReducingState.class);
        ReducingStateDescriptor<Long> reducingStateDescriptor =
                new ReducingStateDescriptor<>("reducing", Long::sum, Long.class);
        InternalKvState<K, N, ?> latencyTrackingState =
                LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                        reducingState, reducingStateDescriptor, getLatencyTrackingStateConfig());
        if (enableLatencyTracking) {
            assertThat(latencyTrackingState).isInstanceOf(LatencyTrackingReducingState.class);
        } else {
            assertThat(latencyTrackingState).isEqualTo(reducingState);
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackAggregatingState() throws Exception {
        InternalAggregatingState<K, N, Long, Long, Long> aggregatingState =
                mock(InternalAggregatingState.class);
        AggregatingStateDescriptor<Long, Long, Long> aggregatingStateDescriptor =
                new AggregatingStateDescriptor<>(
                        "aggregate",
                        new AggregateFunction<Long, Long, Long>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Long value, Long accumulator) {
                                return value + accumulator;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        },
                        Long.class);
        InternalKvState<K, N, ?> latencyTrackingState =
                LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                        aggregatingState,
                        aggregatingStateDescriptor,
                        getLatencyTrackingStateConfig());
        if (enableLatencyTracking) {
            assertThat(latencyTrackingState).isInstanceOf(LatencyTrackingAggregatingState.class);
        } else {
            assertThat(latencyTrackingState).isEqualTo(aggregatingState);
        }
    }
}
