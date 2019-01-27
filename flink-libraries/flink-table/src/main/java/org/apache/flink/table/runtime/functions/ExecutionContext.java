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
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataview.StateDataView;
import org.apache.flink.table.dataview.StateListView;
import org.apache.flink.table.dataview.StateMapView;
import org.apache.flink.table.dataview.StateSortedMapView;
import org.apache.flink.table.typeutils.ListViewTypeInfo;
import org.apache.flink.table.typeutils.MapViewTypeInfo;
import org.apache.flink.table.typeutils.SortedMapViewTypeInfo;

/**
 * A ExecutionContext contains information about the context in which functions are executed and
 * the APIs to create v2 state.
 */
public interface ExecutionContext {

	/**
	 * Creates a keyed state described by the given descriptor.
	 *
	 * @param descriptor The descriptor of the keyed state to be created.
	 * @param <K> Type of the keys in the state.
	 * @param <V> Type of the values in the state.
	 * @param <S> Type of the state to be created.
	 * @return The state described by the given descriptor.
	 */
	<K, V, S extends KeyedState<K, V>> S getKeyedState(
		final KeyedStateDescriptor<K, V, S> descriptor) throws Exception;

	/**
	 * Creates a subkeyed state described by the given descriptor.
	 *
	 * @param descriptor The descriptor of the subkeyed state to be created.
	 * @param <K> Type of the keys in the state.
	 * @param <N> Type of the namespaces in the state.
	 * @param <V> Type of the values in the state.
	 * @param <S> Type of the state to be created.
	 * @return The state described by the given descriptor.
	 */
	<K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
		final SubKeyedStateDescriptor<K, N, V, S> descriptor) throws Exception;

	/**
	 * Creates a keyed value state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <V> Type of the element in the value state
	 * @return a keyed value state
	 */
	<K, V> KeyedValueState<K, V> getKeyedValueState(
		final ValueStateDescriptor<V> descriptor) throws Exception;

	/**
	 * Creates a keyed list state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <V> Type of the elements in the list state
	 * @return a keyed list state
	 */
	<K, V> KeyedListState<K, V> getKeyedListState(
		final ListStateDescriptor<V> descriptor) throws Exception;

	/**
	 * Creates a keyed map state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <UK> Type of the keys in the map state
	 * @param <UV> Type of the values in the map state
	 * @return a keyed map state
	 */
	<K, UK, UV> KeyedMapState<K, UK, UV> getKeyedMapState(
		final MapStateDescriptor<UK, UV> descriptor) throws Exception;

	/**
	 * Creates a keyed sorted map state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <UK> Type of the keys in the sorted map state
	 * @param <UV> Type of the values in the sorted map state
	 * @return a keyed sorted map state
	 */
	<K, UK, UV> KeyedSortedMapState<K, UK, UV> getKeyedSortedMapState(
		final SortedMapStateDescriptor<UK, UV> descriptor) throws Exception;

	/**
	 * Creates a subkeyed value state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <N> Type of the namespace
	 * @param <V> Type of the element in the value state
	 * @return a subkeyed value state
	 */
	<K, N, V> SubKeyedValueState<K, N, V> getSubKeyedValueState(
		final ValueStateDescriptor<V> descriptor) throws Exception;

	/**
	 * Creates a subkeyed list state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <N> Type of the namespace
	 * @param <V> Type of the elements in the list state
	 * @return a subkeyed list state
	 */
	<K, N, V> SubKeyedListState<K, N, V> getSubKeyedListState(
		final ListStateDescriptor<V> descriptor) throws Exception;

	/**
	 * Creates a subkeyed map state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <N> Type of the namespace
	 * @param <UK> Type of the keys in the map state
	 * @param <UV> Type of the values in the map state
	 * @return a subkeyed map state
	 */
	<K, N, UK, UV> SubKeyedMapState<K, N, UK, UV> getSubKeyedMapState(
		final MapStateDescriptor<UK, UV> descriptor) throws Exception;

	/**
	 * Creates a subkeyed sorted map state.
	 * @param descriptor The descriptor defining the properties of the state.
	 * @param <K> Type of the key
	 * @param <N> Type of the namespace
	 * @param <UK> Type of the keys in the sorted map state
	 * @param <UV> Type of the values in the sorted map state
	 * @return a subkeyed sorted map state
	 */
	<K, N, UK, UV> SubKeyedSortedMapState<K, N, UK, UV> getSubKeyedSortedMapState(
		final SortedMapStateDescriptor<UK, UV> descriptor) throws Exception;

	/**
	 * Creates a state map view.
	 * @param stateName The name of underlying state of the map view
	 * @param mapViewTypeInfo The type of the map view
	 * @param hasNamespace whether the state map view works on subkeyed state
	 * @param <K> Type of the key
	 * @param <UK> Type of the keys in the map state
	 * @param <UV> Type of the values in the map state
	 * @return a keyed map state
	 */
	<K, UK, UV> StateMapView<K, UK, UV> getStateMapView(
		String stateName, MapViewTypeInfo<UK, UV> mapViewTypeInfo, boolean hasNamespace) throws Exception;

	/**
	 * Creates a state map view.
	 * @param stateName The name of underlying state of the sorted map view
	 * @param sortedMapViewTypeInfo The type of the sorted map view
	 * @param <K> Type of the key
	 * @param <UK> Type of the keys in the map state
	 * @param <UV> Type of the values in the map state
	 * @return a keyed map state
	 */
	<K, UK, UV> StateSortedMapView<K, UK, UV> getStateSortedMapView(
		String stateName, SortedMapViewTypeInfo<UK, UV> sortedMapViewTypeInfo, boolean hasNamespace) throws Exception;

	/**
	 * Creates a state list view.
	 * @param stateName The name of underlying state of the list view
	 * @param listViewTypeInfo The type of the list view
	 * @param hasNamespace whether the state list view works on subkeyed state
	 * @param <K> Type of the key
	 * @param <V> Type of the elements in the list state
	 * @return a keyed list state
	 */
	<K, V> StateListView<K, V> getStateListView(
		String stateName, ListViewTypeInfo<V> listViewTypeInfo, boolean hasNamespace) throws Exception;

	/**
	 * Registers stateDataView to the context. {@link #setCurrentKey(BaseRow)} will set the
	 * currentKey to all the registered stateDataViews current key.
	 */
	void registerStateDataView(StateDataView<BaseRow> stateDataView);

	/**
	 * @return the key serializer of state key
	 */
	<K> TypeSerializer<K> getKeySerializer();

	/**
	 * @return key of the current processed element.
	 */
	BaseRow currentKey();

	/**
	 * Sets current key.
	 */
	void setCurrentKey(BaseRow key);

	RuntimeContext getRuntimeContext();
}
