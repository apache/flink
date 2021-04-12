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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Default implementation of {@link StateDataViewStore} that currently forwards state registration
 * to a {@link RuntimeContext}.
 */
@Internal
public final class PerKeyStateDataViewStore implements StateDataViewStore {

    private static final String NULL_STATE_POSTFIX = "_null_state";

    private final RuntimeContext ctx;
    private final StateTtlConfig stateTtlConfig;

    public PerKeyStateDataViewStore(RuntimeContext ctx) {
        this(ctx, StateTtlConfig.DISABLED);
    }

    public PerKeyStateDataViewStore(RuntimeContext ctx, StateTtlConfig stateTtlConfig) {
        this.ctx = ctx;
        this.stateTtlConfig = stateTtlConfig;
    }

    @Override
    public <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
            String stateName,
            boolean supportNullKey,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer) {
        final MapStateDescriptor<EK, EV> mapStateDescriptor =
                new MapStateDescriptor<>(stateName, keySerializer, valueSerializer);

        if (stateTtlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(stateTtlConfig);
        }
        final MapState<EK, EV> mapState = ctx.getMapState(mapStateDescriptor);

        if (supportNullKey) {
            final ValueStateDescriptor<EV> nullStateDescriptor =
                    new ValueStateDescriptor<>(stateName + NULL_STATE_POSTFIX, valueSerializer);
            if (stateTtlConfig.isEnabled()) {
                nullStateDescriptor.enableTimeToLive(stateTtlConfig);
            }
            final ValueState<EV> nullState = ctx.getState(nullStateDescriptor);
            return new StateMapView.KeyedStateMapViewWithKeysNullable<>(mapState, nullState);
        } else {
            return new StateMapView.KeyedStateMapViewWithKeysNotNull<>(mapState);
        }
    }

    @Override
    public <N, EE> StateListView<N, EE> getStateListView(
            String stateName, TypeSerializer<EE> elementSerializer) {
        final ListStateDescriptor<EE> listStateDescriptor =
                new ListStateDescriptor<>(stateName, elementSerializer);

        if (stateTtlConfig.isEnabled()) {
            listStateDescriptor.enableTimeToLive(stateTtlConfig);
        }
        final ListState<EE> listState = ctx.getListState(listStateDescriptor);

        return new StateListView.KeyedStateListView<>(listState);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return ctx;
    }
}
