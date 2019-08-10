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
import org.apache.flink.table.dataview.ListViewTypeInfo;
import org.apache.flink.table.dataview.MapViewTypeInfo;

/**
 * An implementation of StateDataViewStore for window aggregates which forward the state
 * registration to underlying {@link KeyedStateBackend}. The created state by this store
 * has the ability to switch window namespace.
 */
public class PerWindowStateDataViewStore implements StateDataViewStore {
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
	public <N, UK, UV> StateMapView<N, UK, UV> getStateMapView(String stateName, MapViewTypeInfo<UK, UV> mapViewTypeInfo) throws Exception {
		MapStateDescriptor<UK, UV> mapStateDescriptor = new MapStateDescriptor<>(
			stateName,
			mapViewTypeInfo.getKeyType(),
			mapViewTypeInfo.getValueType());

		MapState<UK, UV> mapState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, mapStateDescriptor);
		// explict cast to internal state
		InternalMapState<?, N, UK, UV> internalMapState = (InternalMapState<?, N, UK, UV>) mapState;

		if (mapViewTypeInfo.isNullAware()) {
			ValueStateDescriptor<UV> nullStateDescriptor = new ValueStateDescriptor<>(
				stateName + NULL_STATE_POSTFIX,
				mapViewTypeInfo.getValueType());
			ValueState<UV> nullState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, nullStateDescriptor);
			// explict cast to internal state
			InternalValueState<?, N, UV> internalNullState = (InternalValueState<?, N, UV>) nullState;
			return new StateMapView.NamespacedStateMapViewWithKeysNullable<>(internalMapState, internalNullState);
		} else {
			return new StateMapView.NamespacedStateMapViewWithKeysNotNull<>(internalMapState);
		}
	}

	@Override
	public <N, V> StateListView<N, V> getStateListView(String stateName, ListViewTypeInfo<V> listViewTypeInfo) throws Exception {
		ListStateDescriptor<V> listStateDesc = new ListStateDescriptor<>(
			stateName,
			listViewTypeInfo.getElementType());

		ListState<V> listState = keyedStateBackend.getOrCreateKeyedState(windowSerializer, listStateDesc);
		// explict cast to internal state
		InternalListState<?, N, V> internalListState = (InternalListState<?, N, V>) listState;

		return new StateListView.NamespacedStateListView<>(internalListState);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return ctx;
	}
}
