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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Factory to create {@link AbstractLatencyTrackState}. */
public class LatencyTrackingStateFactory<
        K, N, V, S extends State, IS extends InternalKvState<K, N, ?>> {

    private final InternalKvState<K, N, ?> kvState;
    private final StateDescriptor<S, V> stateDescriptor;
    private final LatencyTrackingStateConfig latencyTrackingStateConfig;
    private final Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> stateFactories;

    private LatencyTrackingStateFactory(
            InternalKvState<K, N, ?> kvState,
            StateDescriptor<S, V> stateDescriptor,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        this.kvState = kvState;
        this.stateDescriptor = stateDescriptor;
        this.latencyTrackingStateConfig = latencyTrackingStateConfig;
        this.stateFactories = createStateFactories();
    }

    /** Create latency tracking state if enabled. */
    public static <K, N, V, S extends State>
            InternalKvState<K, N, ?> createStateAndWrapWithLatencyTrackingIfEnabled(
                    InternalKvState<K, N, ?> kvState,
                    StateDescriptor<S, V> stateDescriptor,
                    LatencyTrackingStateConfig latencyTrackingStateConfig)
                    throws Exception {
        if (latencyTrackingStateConfig.isEnabled()) {
            return new LatencyTrackingStateFactory<>(
                            kvState, stateDescriptor, latencyTrackingStateConfig)
                    .createState();
        }
        return kvState;
    }

    private Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> createStateFactories() {
        return Stream.of(
                        Tuple2.of(
                                StateDescriptor.Type.VALUE,
                                (SupplierWithException<IS, Exception>) this::createValueState),
                        Tuple2.of(
                                StateDescriptor.Type.LIST,
                                (SupplierWithException<IS, Exception>) this::createListState),
                        Tuple2.of(
                                StateDescriptor.Type.MAP,
                                (SupplierWithException<IS, Exception>) this::createMapState),
                        Tuple2.of(
                                StateDescriptor.Type.REDUCING,
                                (SupplierWithException<IS, Exception>) this::createReducingState),
                        Tuple2.of(
                                StateDescriptor.Type.AGGREGATING,
                                (SupplierWithException<IS, Exception>)
                                        this::createAggregatingState))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    private IS createState() throws Exception {
        SupplierWithException<IS, Exception> stateFactory =
                stateFactories.get(stateDescriptor.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDescriptor.getClass(), LatencyTrackingStateFactory.class);
            throw new FlinkRuntimeException(message);
        }
        return stateFactory.get();
    }

    @SuppressWarnings({"unchecked"})
    private IS createValueState() {
        return (IS)
                new LatencyTrackingValueState<>(
                        stateDescriptor.getName(),
                        (InternalValueState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
    }

    @SuppressWarnings({"unchecked"})
    private IS createListState() {
        return (IS)
                new LatencyTrackingListState<>(
                        stateDescriptor.getName(),
                        (InternalListState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
    }

    @SuppressWarnings({"unchecked"})
    private <UK, UV> IS createMapState() {
        return (IS)
                new LatencyTrackingMapState<>(
                        stateDescriptor.getName(),
                        (InternalMapState<K, N, UK, UV>) kvState,
                        latencyTrackingStateConfig);
    }

    @SuppressWarnings({"unchecked"})
    private IS createReducingState() {
        return (IS)
                new LatencyTrackingReducingState<>(
                        stateDescriptor.getName(),
                        (InternalReducingState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
    }

    @SuppressWarnings({"unchecked"})
    private <IN, SV, OUT> IS createAggregatingState() {
        return (IS)
                new LatencyTrackingAggregatingState<>(
                        stateDescriptor.getName(),
                        (InternalAggregatingState<K, N, IN, SV, OUT>) kvState,
                        latencyTrackingStateConfig);
    }
}
