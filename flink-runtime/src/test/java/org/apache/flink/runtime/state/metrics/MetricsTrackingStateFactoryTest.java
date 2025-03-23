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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackendBuilder;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MetricsTrackingStateFactory}. */
@ExtendWith(ParameterizedTestExtension.class)
public class MetricsTrackingStateFactoryTest {

    @Parameter public Tuple2<Boolean, Boolean> enableLatencyOrSizeTracking;

    @Parameters(name = "enable latency or size tracking: {0}")
    public static Collection<Tuple2<Boolean, Boolean>> enabled() {
        return Arrays.asList(
                Tuple2.of(true, true),
                Tuple2.of(true, false),
                Tuple2.of(false, true),
                Tuple2.of(false, false));
    }

    private LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        return LatencyTrackingStateConfig.newBuilder()
                .setEnabled(enableLatencyOrSizeTracking.f0)
                .setMetricGroup(metricsGroup)
                .build();
    }

    private SizeTrackingStateConfig getSizeTrackingStateConfig() {
        UnregisteredMetricsGroup metricsGroup = new UnregisteredMetricsGroup();
        return SizeTrackingStateConfig.newBuilder()
                .setEnabled(enableLatencyOrSizeTracking.f1)
                .setMetricGroup(metricsGroup)
                .build();
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackValueState() throws Exception {
        try (MockKeyedStateBackend<String> backend = createMock()) {
            ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<>("value", String.class);
            InternalValueState<K, N, String> valueState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE, valueStateDescriptor);
            InternalKvState<K, ?, ?> latencyTrackingState =
                    MetricsTrackingStateFactory.createStateAndWrapWithMetricsTrackingIfEnabled(
                            valueState,
                            null,
                            valueStateDescriptor,
                            getLatencyTrackingStateConfig(),
                            getSizeTrackingStateConfig());
            if (enableLatencyOrSizeTracking.f0 || enableLatencyOrSizeTracking.f1) {
                assertThat(latencyTrackingState).isInstanceOf(MetricsTrackingValueState.class);
            } else {
                assertThat(latencyTrackingState).isEqualTo(valueState);
            }
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackListState() throws Exception {
        try (MockKeyedStateBackend<String> backend = createMock()) {
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<>("list", String.class);
            InternalListState<K, N, String> listState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE, listStateDescriptor);
            InternalKvState<K, N, ?> latencyTrackingState =
                    MetricsTrackingStateFactory.createStateAndWrapWithMetricsTrackingIfEnabled(
                            listState,
                            null,
                            listStateDescriptor,
                            getLatencyTrackingStateConfig(),
                            getSizeTrackingStateConfig());
            if (enableLatencyOrSizeTracking.f0 || enableLatencyOrSizeTracking.f1) {
                assertThat(latencyTrackingState).isInstanceOf(MetricsTrackingListState.class);
            } else {
                assertThat(latencyTrackingState).isEqualTo(listState);
            }
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackMapState() throws Exception {
        try (MockKeyedStateBackend<String> backend = createMock()) {
            MapStateDescriptor<String, Long> mapStateDescriptor =
                    new MapStateDescriptor<>("map", String.class, Long.class);
            InternalMapState<K, N, String, Long> mapState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE, mapStateDescriptor);
            InternalKvState<K, N, ?> latencyTrackingState =
                    MetricsTrackingStateFactory.createStateAndWrapWithMetricsTrackingIfEnabled(
                            mapState,
                            null,
                            mapStateDescriptor,
                            getLatencyTrackingStateConfig(),
                            getSizeTrackingStateConfig());
            if (enableLatencyOrSizeTracking.f0 || enableLatencyOrSizeTracking.f1) {
                assertThat(latencyTrackingState).isInstanceOf(MetricsTrackingMapState.class);
            } else {
                assertThat(latencyTrackingState).isEqualTo(mapState);
            }
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackReducingState() throws Exception {
        try (MockKeyedStateBackend<String> backend = createMock()) {
            ReducingStateDescriptor<Long> reducingStateDescriptor =
                    new ReducingStateDescriptor<>("reducing", Long::sum, Long.class);
            InternalReducingState<K, N, Long> reducingState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE, reducingStateDescriptor);
            InternalKvState<K, N, ?> latencyTrackingState =
                    MetricsTrackingStateFactory.createStateAndWrapWithMetricsTrackingIfEnabled(
                            reducingState,
                            null,
                            reducingStateDescriptor,
                            getLatencyTrackingStateConfig(),
                            getSizeTrackingStateConfig());
            if (enableLatencyOrSizeTracking.f0 || enableLatencyOrSizeTracking.f1) {
                assertThat(latencyTrackingState).isInstanceOf(MetricsTrackingReducingState.class);
            } else {
                assertThat(latencyTrackingState).isEqualTo(reducingState);
            }
        }
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    <K, N> void testTrackAggregatingState() throws Exception {
        try (MockKeyedStateBackend<String> backend = createMock()) {
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
            InternalAggregatingState<K, N, Long, Long, Long> aggregatingState =
                    backend.createOrUpdateInternalState(
                            VoidNamespaceSerializer.INSTANCE, aggregatingStateDescriptor);
            InternalKvState<K, N, ?> latencyTrackingState =
                    MetricsTrackingStateFactory.createStateAndWrapWithMetricsTrackingIfEnabled(
                            aggregatingState,
                            null,
                            aggregatingStateDescriptor,
                            getLatencyTrackingStateConfig(),
                            getSizeTrackingStateConfig());
            if (enableLatencyOrSizeTracking.f0 || enableLatencyOrSizeTracking.f1) {
                assertThat(latencyTrackingState)
                        .isInstanceOf(MetricsTrackingAggregatingState.class);
            } else {
                assertThat(latencyTrackingState).isEqualTo(aggregatingState);
            }
        }
    }

    private MockKeyedStateBackend<String> createMock() {
        return new MockKeyedStateBackendBuilder<>(
                        new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                        StringSerializer.INSTANCE,
                        getClass().getClassLoader(),
                        1,
                        KeyGroupRange.of(0, 0),
                        new ExecutionConfig(),
                        TtlTimeProvider.DEFAULT,
                        getLatencyTrackingStateConfig(),
                        getSizeTrackingStateConfig(),
                        emptyList(),
                        UncompressedStreamCompressionDecorator.INSTANCE,
                        new CloseableRegistry(),
                        MockKeyedStateBackend.MockSnapshotSupplier.EMPTY)
                .build();
    }
}
