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

package org.apache.flink.datastream.impl.extension.window.context;

import org.apache.flink.api.common.state.AggregatingStateDeclaration;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.ReducingStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datastream.api.extension.window.context.WindowContext;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.v2.adaptor.AggregatingStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.ListStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.MapStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.ReducingStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.ValueStateAdaptor;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class provides methods to store and retrieve state associated with windows in {@link
 * WindowContext}.
 *
 * @param <K> Type of the window key.
 * @param <W> Type of the window.
 */
public class WindowStateStore<K, W extends Window> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowStateStore.class);

    /**
     * The {@link StateDeclaration}s that have been declared by the {@link
     * WindowProcessFunction#useWindowStates()}.
     */
    private final Set<StateDeclaration> windowStateDeclarations;

    /** The operator to which the window belongs, used for creating and retrieving window state. */
    private final AbstractAsyncStateStreamOperator<?> operator;

    private final TypeSerializer<W> windowSerializer;

    /** Whether the window is a merging window. */
    private final boolean isMergingWindow;

    public WindowStateStore(
            WindowProcessFunction windowProcessFunction,
            AbstractAsyncStateStreamOperator<?> operator,
            TypeSerializer<W> windowSerializer,
            boolean isMergingWindow) {
        this.windowStateDeclarations = windowProcessFunction.useWindowStates();
        this.operator = operator;
        this.windowSerializer = windowSerializer;
        this.isMergingWindow = isMergingWindow;
    }

    private boolean isStateDeclared(StateDeclaration stateDeclaration) {
        if (!windowStateDeclarations.contains(stateDeclaration)) {
            LOG.warn(
                    "Fail to get window state for "
                            + stateDeclaration.getName()
                            + ", please declare the used state in the `WindowProcessFunction#useWindowStates` method first.");
            return false;
        }
        return true;
    }

    private boolean stateRedistributionModeIsNotNone(StateDeclaration stateDeclaration) {
        StateDeclaration.RedistributionMode redistributionMode =
                stateDeclaration.getRedistributionMode();
        return redistributionMode != StateDeclaration.RedistributionMode.NONE;
    }

    /** Retrieve window state of list type. */
    public <T> Optional<ListState<T>> getWindowState(
            ListStateDeclaration<T> stateDeclaration, W namespace) throws Exception {
        checkState(
                !isMergingWindow,
                "Retrieving the window state is not permitted when using merging windows, such as session windows.");

        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNotNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ListStateDescriptor<T> stateDescriptor =
                new ListStateDescriptor<>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        ListStateAdaptor<K, W, T> state =
                operator.getOrCreateKeyedState(namespace, windowSerializer, stateDescriptor);
        state.setCurrentNamespace(namespace);
        return Optional.of(state);
    }

    /** Retrieve window state of map type. */
    public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
            MapStateDeclaration<KEY, V> stateDeclaration, W namespace) throws Exception {
        checkState(
                !isMergingWindow,
                "Retrieving the window state is not permitted when using merging windows, such as session windows.");

        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNotNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        MapStateDescriptor<KEY, V> stateDescriptor =
                new MapStateDescriptor<>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getKeyTypeDescriptor().getTypeClass()),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getValueTypeDescriptor().getTypeClass()));

        MapStateAdaptor<K, W, KEY, V> state =
                operator.getOrCreateKeyedState(namespace, windowSerializer, stateDescriptor);
        state.setCurrentNamespace(namespace);
        return Optional.of(state);
    }

    /** Retrieve window state of value type. */
    public <T> Optional<ValueState<T>> getWindowState(
            ValueStateDeclaration<T> stateDeclaration, W namespace) throws Exception {
        checkState(
                !isMergingWindow,
                "Retrieving the window state is not permitted when using merging windows, such as session windows.");

        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNotNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ValueStateDescriptor<T> stateDescriptor =
                new ValueStateDescriptor<>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        ValueStateAdaptor<K, W, T> state =
                operator.getOrCreateKeyedState(namespace, windowSerializer, stateDescriptor);
        state.setCurrentNamespace(namespace);
        return Optional.of(state);
    }

    /** Retrieve window state of ReducingState type. */
    public <T> Optional<ReducingState<T>> getWindowState(
            ReducingStateDeclaration<T> stateDeclaration, W namespace) throws Exception {
        checkState(
                !isMergingWindow,
                "Retrieving the window state is not permitted when using merging windows, such as session windows.");

        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNotNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ReducingStateDescriptor<T> stateDescriptor =
                new ReducingStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getReduceFunction(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        ReducingStateAdaptor<K, W, T> state =
                operator.getOrCreateKeyedState(namespace, windowSerializer, stateDescriptor);
        state.setCurrentNamespace(namespace);
        return Optional.of(state);
    }

    /** Retrieve window state of AggregatingState type. */
    public <T, ACC, OUT> Optional<AggregatingState<T, OUT>> getWindowState(
            AggregatingStateDeclaration<T, ACC, OUT> stateDeclaration, W namespace)
            throws Exception {
        checkState(
                !isMergingWindow,
                "Retrieving the window state is not permitted when using merging windows, such as session windows.");

        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNotNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        AggregatingStateDescriptor<T, ACC, OUT> stateDescriptor =
                new AggregatingStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getAggregateFunction(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        AggregatingStateAdaptor<K, W, T, ACC, OUT> state =
                operator.getOrCreateKeyedState(namespace, windowSerializer, stateDescriptor);
        state.setCurrentNamespace(namespace);
        return Optional.of(state);
    }
}
