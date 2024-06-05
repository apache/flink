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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDeclaration;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDeclaration;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.datastream.api.context.StateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The default implementation of {@link StateManager}. This class supports eagerly set and reset the
 * current key.
 */
public class DefaultStateManager implements StateManager {

    /**
     * Retrieve the current key. When {@link #currentKeySetter} receives a key, this must return
     * that key until it is reset.
     */
    private final Supplier<Object> currentKeySupplier;

    private final Consumer<Object> currentKeySetter;

    protected final StreamingRuntimeContext operatorContext;

    protected final OperatorStateStore operatorStateStore;

    public DefaultStateManager(
            Supplier<Object> currentKeySupplier,
            Consumer<Object> currentKeySetter,
            StreamingRuntimeContext operatorContext,
            OperatorStateStore operatorStateStore) {
        this.currentKeySupplier = currentKeySupplier;
        this.currentKeySetter = currentKeySetter;
        this.operatorContext = Preconditions.checkNotNull(operatorContext);
        this.operatorStateStore = Preconditions.checkNotNull(operatorStateStore);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> K getCurrentKey() {
        return (K) currentKeySupplier.get();
    }

    @Override
    public <T> Optional<ValueState<T>> getState(ValueStateDeclaration<T> stateDeclaration)
            throws Exception {
        ValueStateDescriptor<T> valueStateDescriptor =
                new ValueStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getTypeDescriptor().getTypeClass());
        return Optional.ofNullable(operatorContext.getState(valueStateDescriptor));
    }

    @Override
    public <T> Optional<ListState<T>> getState(ListStateDeclaration<T> stateDeclaration)
            throws Exception {

        ListStateDescriptor<T> listStateDescriptor =
                new ListStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getTypeDescriptor().getTypeClass());

        if (stateDeclaration.getRedistributionMode()
                == StateDeclaration.RedistributionMode.REDISTRIBUTABLE) {
            if (stateDeclaration.getRedistributionStrategy()
                    == ListStateDeclaration.RedistributionStrategy.UNION) {
                return Optional.ofNullable(
                        operatorStateStore.getUnionListState(listStateDescriptor));
            } else {
                return Optional.ofNullable(operatorStateStore.getListState(listStateDescriptor));
            }
        } else {
            return Optional.ofNullable(operatorContext.getListState(listStateDescriptor));
        }
    }

    @Override
    public <K, V> Optional<MapState<K, V>> getState(MapStateDeclaration<K, V> stateDeclaration)
            throws Exception {
        MapStateDescriptor<K, V> mapStateDescriptor =
                new MapStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getKeyTypeDescriptor().getTypeClass(),
                        stateDeclaration.getValueTypeDescriptor().getTypeClass());
        return Optional.ofNullable(operatorContext.getMapState(mapStateDescriptor));
    }

    @Override
    public <T> Optional<ReducingState<T>> getState(ReducingStateDeclaration<T> stateDeclaration)
            throws Exception {
        ReducingStateDescriptor<T> reducingStateDescriptor =
                new ReducingStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getReduceFunction(),
                        stateDeclaration.getTypeDescriptor().getTypeClass());
        return Optional.ofNullable(operatorContext.getReducingState(reducingStateDescriptor));
    }

    @Override
    public <IN, ACC, OUT> Optional<AggregatingState<IN, OUT>> getState(
            AggregatingStateDeclaration<IN, ACC, OUT> stateDeclaration) throws Exception {
        AggregatingStateDescriptor<IN, ACC, OUT> aggregatingStateDescriptor =
                new AggregatingStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getAggregateFunction(),
                        stateDeclaration.getTypeDescriptor().getTypeClass());
        return Optional.ofNullable(operatorContext.getAggregatingState(aggregatingStateDescriptor));
    }

    @Override
    public <K, V> Optional<BroadcastState<K, V>> getState(
            BroadcastStateDeclaration<K, V> stateDeclaration) throws Exception {
        MapStateDescriptor<K, V> mapStateDescriptor =
                new MapStateDescriptor<>(
                        stateDeclaration.getName(),
                        stateDeclaration.getKeyTypeDescriptor().getTypeClass(),
                        stateDeclaration.getValueTypeDescriptor().getTypeClass());
        return Optional.ofNullable(operatorStateStore.getBroadcastState(mapStateDescriptor));
    }

    /**
     * This method should be used to run a block of code with a specific key context. The original
     * key must be reset after the block is executed.
     */
    public void executeInKeyContext(Runnable runnable, Object key) {
        final Object oldKey = currentKeySupplier.get();
        setCurrentKey(key);
        try {
            runnable.run();
        } finally {
            resetCurrentKey(oldKey);
        }
    }

    private void setCurrentKey(Object key) {
        currentKeySetter.accept(key);
    }

    private void resetCurrentKey(Object oldKey) {
        currentKeySetter.accept(oldKey);
    }
}
