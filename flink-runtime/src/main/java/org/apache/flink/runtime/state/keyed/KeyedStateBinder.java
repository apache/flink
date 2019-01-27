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

package org.apache.flink.runtime.state.keyed;

/**
 * The helper class to get {@link KeyedState} described by the given
 * descriptor.
 */
public interface KeyedStateBinder {
	/**
	 * Creates and returns a new {@link KeyedValueState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <V> The type of the values in the state.
	 */
	<K, V> KeyedValueState<K, V> createKeyedValueState(
		KeyedValueStateDescriptor<K, V> stateDescriptor) throws Exception;

	/**
	 * Creates and returns a new {@link KeyedListState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create.
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <E> The type of the list elements in the state.
	 */
	<K, E> KeyedListState<K, E> createKeyedListState(
		KeyedListStateDescriptor<K, E> stateDescriptor) throws Exception;

	/**
	 * Creates and returns a new {@link KeyedMapState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <MK> The type of the map keys in the state.
	 * @param <MV> The type of the map values in the state.
	 */
	<K, MK, MV> KeyedMapState<K, MK, MV> createKeyedMapState(
		KeyedMapStateDescriptor<K, MK, MV> stateDescriptor) throws Exception;

	/**
	 * Creates and returns a new {@link KeyedSortedMapState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create.
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <MK> The type of the map keys in the state.
	 * @param <MV> The type of the map values in the state.
	 */
	<K, MK, MV> KeyedSortedMapState<K, MK, MV> createKeyedSortedMapState(
		KeyedSortedMapStateDescriptor<K, MK, MV> stateDescriptor) throws Exception;
}

