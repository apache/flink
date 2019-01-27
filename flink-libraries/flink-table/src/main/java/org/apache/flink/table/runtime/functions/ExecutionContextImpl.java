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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataview.KeyedStateListView;
import org.apache.flink.table.dataview.KeyedStateMapView;
import org.apache.flink.table.dataview.KeyedStateSortedMapView;
import org.apache.flink.table.dataview.NullAwareKeyedStateMapView;
import org.apache.flink.table.dataview.NullAwareSubKeyedStateMapView;
import org.apache.flink.table.dataview.StateDataView;
import org.apache.flink.table.dataview.StateListView;
import org.apache.flink.table.dataview.StateMapView;
import org.apache.flink.table.dataview.StateSortedMapView;
import org.apache.flink.table.dataview.SubKeyedStateListView;
import org.apache.flink.table.dataview.SubKeyedStateMapView;
import org.apache.flink.table.typeutils.ListViewTypeInfo;
import org.apache.flink.table.typeutils.MapViewTypeInfo;
import org.apache.flink.table.typeutils.SortedMapViewTypeInfo;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of ExecutionContext.
 */
@SuppressWarnings("unchecked")
public final class ExecutionContextImpl implements ExecutionContext {

	private static final String NULL_STATE_POSTFIX = "_null_state";

	private final AbstractStreamOperator<?> operator;
	private final RuntimeContext runtimeContext;
	private final TypeSerializer<?> namespaceSerializer;
	private final List<StateDataView<BaseRow>> registeredStateDataViews;

	public ExecutionContextImpl(
			AbstractStreamOperator<?> operator,
			RuntimeContext runtimeContext) {
		this(operator, runtimeContext, null);
	}

	public ExecutionContextImpl(
			AbstractStreamOperator<?> operator,
			RuntimeContext runtimeContext,
			TypeSerializer<?> namespaceSerializer) {
		this.operator = operator;
		this.runtimeContext = Preconditions.checkNotNull(runtimeContext);
		this.namespaceSerializer = namespaceSerializer;
		this.registeredStateDataViews = new ArrayList<>();
	}

	@Override
	public <K, V, S extends KeyedState<K, V>> S getKeyedState(KeyedStateDescriptor<K, V, S> descriptor) throws Exception {
		return operator.getKeyedState(descriptor);
	}

	@Override
	public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(SubKeyedStateDescriptor<K, N, V, S> descriptor) throws Exception {
		return operator.getSubKeyedState(descriptor);
	}

	@Override
	public <K, V> KeyedValueState<K, V> getKeyedValueState(
		ValueStateDescriptor<V> descriptor) throws Exception {
		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getKeyedState(
			new KeyedValueStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				descriptor.getSerializer()
			)
		);
	}

	@Override
	public <K, V> KeyedListState<K, V> getKeyedListState(
		ListStateDescriptor<V> descriptor
	) throws Exception {
		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getKeyedState(
			new KeyedListStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				(ListSerializer<V>) descriptor.getSerializer()
			)
		);
	}

	@Override
	public <K, UK, UV> KeyedMapState<K, UK, UV> getKeyedMapState(
		MapStateDescriptor<UK, UV> descriptor
	) throws Exception {
		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getKeyedState(
			new KeyedMapStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				(MapSerializer<UK, UV>) descriptor.getSerializer()
			)
		);
	}

	@Override
	public <K, UK, UV> KeyedSortedMapState<K, UK, UV> getKeyedSortedMapState(
		SortedMapStateDescriptor<UK, UV> descriptor
	) throws Exception {
		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getKeyedState(
			new KeyedSortedMapStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				(SortedMapSerializer<UK, UV>) descriptor.getSerializer()
			)
		);
	}

	@Override
	public <K, N, V> SubKeyedValueState<K, N, V> getSubKeyedValueState(
		ValueStateDescriptor<V> descriptor
	) throws Exception {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getSubKeyedState(
			new SubKeyedValueStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				(TypeSerializer<N>) namespaceSerializer,
				descriptor.getSerializer()
			)
		);
	}

	@Override
	public <K, N, V> SubKeyedListState<K, N, V> getSubKeyedListState(
		ListStateDescriptor<V> descriptor
	) throws Exception {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		return operator.getSubKeyedState(new SubKeyedListStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			((ListSerializer<V>) descriptor.getSerializer()).getElementSerializer()));
	}

	@Override
	public <K, N, UK, UV> SubKeyedMapState<K, N, UK, UV> getSubKeyedMapState(
		MapStateDescriptor<UK, UV> descriptor
	) throws Exception {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		MapSerializer<UK, UV> mapSerializer = (MapSerializer<UK, UV>) descriptor.getSerializer();
		return operator.getSubKeyedState(new SubKeyedMapStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			mapSerializer.getKeySerializer(),
			mapSerializer.getValueSerializer()));
	}

	@Override
	public <K, N, UK, UV> SubKeyedSortedMapState<K, N, UK, UV> getSubKeyedSortedMapState(
		SortedMapStateDescriptor<UK, UV> descriptor
	) throws Exception {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}
		descriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		SortedMapSerializer<UK, UV> sortedMapSerializer = (SortedMapSerializer<UK, UV>) descriptor.getSerializer();
		return operator.getSubKeyedState(new SubKeyedSortedMapStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			sortedMapSerializer.getComparator(),
			sortedMapSerializer.getKeySerializer(),
			sortedMapSerializer.getValueSerializer()));
	}

	@Override
	public <K, UK, UV> StateMapView<K, UK, UV> getStateMapView(
		String stateName,
		MapViewTypeInfo<UK, UV> mapViewTypeInfo,
		boolean hasNamespace) throws Exception {

		MapStateDescriptor<UK, UV> mapStateDescriptor = new MapStateDescriptor<>(
			stateName,
			mapViewTypeInfo.keyType(),
			mapViewTypeInfo.valueType());

		ValueStateDescriptor<UV> nullStateDescriptor = new ValueStateDescriptor<>(
			stateName + NULL_STATE_POSTFIX,
			mapViewTypeInfo.valueType());

		if (hasNamespace) {
			SubKeyedMapState<K, Object, UK, UV> mapState = getSubKeyedMapState(mapStateDescriptor);
			if (mapViewTypeInfo.nullAware()) {
				SubKeyedValueState<K, Object, UV> nullState = getSubKeyedValueState(nullStateDescriptor);
				return new NullAwareSubKeyedStateMapView<>(mapState, nullState);
			} else {
				return new SubKeyedStateMapView<>(mapState);
			}
		} else {
			KeyedMapState<K, UK, UV> mapState = getKeyedMapState(mapStateDescriptor);
			if (mapViewTypeInfo.nullAware()) {
				KeyedValueState<K, UV> nullState = getKeyedValueState(nullStateDescriptor);
				return new NullAwareKeyedStateMapView<>(mapState, nullState);
			} else {
				return new KeyedStateMapView<>(mapState);
			}
		}
	}

	@Override
	public <K, UK, UV> StateSortedMapView<K, UK, UV> getStateSortedMapView(
		String stateName,
		SortedMapViewTypeInfo<UK, UV> sortedMapViewTypeInfo,
		boolean hasNamespace) throws Exception {

		SortedMapStateDescriptor<UK, UV> sortedMapStateDesc = new SortedMapStateDescriptor<>(
			stateName,
			sortedMapViewTypeInfo.comparator,
			sortedMapViewTypeInfo.keyType,
			sortedMapViewTypeInfo.valueType);

		if (!hasNamespace) {
			KeyedSortedMapState<K, UK, UV> mapState = getKeyedSortedMapState(sortedMapStateDesc);
			return new KeyedStateSortedMapView<>(mapState);
		} else {
			throw new UnsupportedOperationException("SubKeyedState SortedMapView is not supported currently");
		}
	}

	@Override
	public <K, V> StateListView<K, V> getStateListView(
		String stateName,
		ListViewTypeInfo<V> listViewTypeInfo,
		boolean hasNamespace) throws Exception {

		ListStateDescriptor<V> listStateDesc = new ListStateDescriptor<>(
			stateName,
			listViewTypeInfo.elementType());

		if (hasNamespace) {
			SubKeyedListState<K, Object, V> listState = getSubKeyedListState(listStateDesc);
			return new SubKeyedStateListView<>(listState);
		} else {
			KeyedListState<K, V> listState = getKeyedListState(listStateDesc);
			return new KeyedStateListView<>(listState);
		}
	}

	@Override
	public void registerStateDataView(StateDataView<BaseRow> stateDataView) {
		registeredStateDataViews.add(stateDataView);
	}

	@Override
	public <K> TypeSerializer<K> getKeySerializer() {
		return (TypeSerializer<K>) operator.getKeySerializer();
	}

	@Override
	public BaseRow currentKey() {
		return (BaseRow) operator.getCurrentKey();
	}

	@Override
	public void setCurrentKey(BaseRow key) {
		operator.setCurrentKey(key);
		// set current key to all the registered stateDataviews
		for (StateDataView<BaseRow> dataView : registeredStateDataViews) {
			dataView.setCurrentKey(key);
		}
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return runtimeContext;
	}
}
