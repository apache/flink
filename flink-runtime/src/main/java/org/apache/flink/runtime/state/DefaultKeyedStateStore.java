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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of KeyedStateStore that currently forwards state registration to a {@link
 * RuntimeContext}.
 */
public class DefaultKeyedStateStore implements KeyedStateStore {

    @Nullable protected final KeyedStateBackend<?> keyedStateBackend;

    @Nullable protected final AsyncKeyedStateBackend<?> asyncKeyedStateBackend;
    protected final SerializerFactory serializerFactory;

    protected SupportKeyedStateApiSet supportKeyedStateApiSet;

    public DefaultKeyedStateStore(
            KeyedStateBackend<?> keyedStateBackend, SerializerFactory serializerFactory) {
        this(keyedStateBackend, null, serializerFactory);
    }

    public DefaultKeyedStateStore(
            AsyncKeyedStateBackend<?> asyncKeyedStateBackend, SerializerFactory serializerFactory) {
        this(null, asyncKeyedStateBackend, serializerFactory);
    }

    public DefaultKeyedStateStore(
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable AsyncKeyedStateBackend<?> asyncKeyedStateBackend,
            SerializerFactory serializerFactory) {
        this.keyedStateBackend = keyedStateBackend;
        this.asyncKeyedStateBackend = asyncKeyedStateBackend;
        this.serializerFactory = Preconditions.checkNotNull(serializerFactory);
        if (keyedStateBackend != null) {
            // By default, we support state v1
            this.supportKeyedStateApiSet = SupportKeyedStateApiSet.STATE_V1;
        } else if (asyncKeyedStateBackend != null) {
            this.supportKeyedStateApiSet = SupportKeyedStateApiSet.STATE_V2;
        } else {
            throw new IllegalArgumentException("The state backend must not be null.");
        }
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            ListState<T> originalState = getPartitionedState(stateProperties);
            return new UserFacingListState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            MapState<UK, UV> originalState = getPartitionedState(stateProperties);
            return new UserFacingMapState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        checkState(
                keyedStateBackend != null
                        && supportKeyedStateApiSet == SupportKeyedStateApiSet.STATE_V1,
                "Current operator does not integrate the async processing logic, "
                        + "thus only supports state v1 APIs. Please use StateDescriptor under "
                        + "'org.apache.flink.runtime.state'.");
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    @Override
    public <T> org.apache.flink.api.common.state.v2.ValueState<T> getValueState(
            @Nonnull org.apache.flink.api.common.state.v2.ValueStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> org.apache.flink.api.common.state.v2.ListState<T> getListState(
            @Nonnull org.apache.flink.api.common.state.v2.ListStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <UK, UV> org.apache.flink.api.common.state.v2.MapState<UK, UV> getMapState(
            @Nonnull
                    org.apache.flink.api.common.state.v2.MapStateDescriptor<UK, UV>
                            stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> org.apache.flink.api.common.state.v2.ReducingState<T> getReducingState(
            @Nonnull
                    org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T>
                            stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <IN, ACC, OUT>
            org.apache.flink.api.common.state.v2.AggregatingState<IN, OUT> getAggregatingState(
                    @Nonnull
                            org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<
                                            IN, ACC, OUT>
                                    stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(serializerFactory);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    protected <S extends org.apache.flink.api.common.state.v2.State, SV> S getPartitionedState(
            org.apache.flink.api.common.state.v2.StateDescriptor<SV> stateDescriptor)
            throws Exception {
        checkState(
                asyncKeyedStateBackend != null
                        && supportKeyedStateApiSet == SupportKeyedStateApiSet.STATE_V2,
                "Current operator integrates the async processing logic, "
                        + "thus only supports state v2 APIs. Please use StateDescriptor under "
                        + "'org.apache.flink.runtime.state.v2'.");
        return asyncKeyedStateBackend.getOrCreateKeyedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }

    public void setSupportKeyedStateApiSetV2() {
        requireNonNull(
                asyncKeyedStateBackend,
                "Current operator integrates the logic of async processing, "
                        + "thus only support state v2 APIs. Please use StateDescriptor under "
                        + "'org.apache.flink.runtime.state.v2'.");
        supportKeyedStateApiSet = SupportKeyedStateApiSet.STATE_V2;
    }

    /**
     * Currently, we only support one keyed state api set. This is determined by the stream
     * operator.
     */
    private enum SupportKeyedStateApiSet {
        STATE_V1,
        STATE_V2
    }
}
