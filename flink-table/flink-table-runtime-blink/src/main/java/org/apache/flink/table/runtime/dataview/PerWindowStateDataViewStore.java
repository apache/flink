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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * An implementation of {@link StateDataViewStore} for window aggregates which forwards the state
 * registration to an underlying {@link KeyedStateBackend}. The created state by this store
 * has the ability to switch window namespaces.
 */
@Internal
public final class PerWindowStateDataViewStore implements StateDataViewStore {

	private static final String NULL_STATE_POSTFIX = "_null_state";

	private final KeyedStateBackend<?> keyedStateBackend;

	private final TypeSerializer<?> windowSerializer;

	private final RuntimeContext ctx;

	public PerWindowStateDataViewStore(
			KeyedStateBackend<?> keyedStateBackend,
			TypeSerializer<?> windowSerializer,
			RuntimeContext runtimeContext) {
		this.keyedStateBackend = keyedStateBackend;
		this.windowSerializer = windowSerializer;
		this.ctx = runtimeContext;
	}

	@Override
	public <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
			String stateName,
			boolean supportNullKey,
			TypeSerializer<EK> keySerializer,
			TypeSerializer<EV> valueSerializer) throws Exception {
		final MapStateDescriptor<EK, EV> mapStateDescriptor = new MapStateDescriptor<>(
			stateName,
			keySerializer,
			valueSerializer);

		final MapState<EK, EV> mapState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, mapStateDescriptor);
		// explict cast to internal state
		final InternalMapState<?, N, EK, EV> internalMapState = (InternalMapState<?, N, EK, EV>) mapState;

		if (supportNullKey) {
			final ValueStateDescriptor<EV> nullStateDescriptor = new ValueStateDescriptor<>(
				stateName + NULL_STATE_POSTFIX,
				valueSerializer);
			final ValueState<EV> nullState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, nullStateDescriptor);
			// explict cast to internal state
			final InternalValueState<?, N, EV> internalNullState = (InternalValueState<?, N, EV>) nullState;
			return new StateMapView.NamespacedStateMapViewWithKeysNullable<>(internalMapState, internalNullState);
		} else {
			return new StateMapView.NamespacedStateMapViewWithKeysNotNull<>(internalMapState);
		}
	}

	@Override
	public <N, EE> StateListView<N, EE> getStateListView(
			String stateName,
			TypeSerializer<EE> elementSerializer) throws Exception {
		final ListStateDescriptor<EE> listStateDescriptor = new ListStateDescriptor<>(
			stateName,
			elementSerializer);

		final ListState<EE> listState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, listStateDescriptor);
		// explict cast to internal state
		final InternalListState<?, N, EE> internalListState = (InternalListState<?, N, EE>) listState;

		return new StateListView.NamespacedStateListView<>(internalListState);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return ctx;
	}
}
