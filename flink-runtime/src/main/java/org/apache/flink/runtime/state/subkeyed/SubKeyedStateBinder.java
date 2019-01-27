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

package org.apache.flink.runtime.state.subkeyed;

/**
 * The helper class to get {@link SubKeyedState} described by the given
 * descriptor.
 */
public interface SubKeyedStateBinder {
	/**
	 * Creates and returns a new {@link SubKeyedValueState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <N> The type of the namespaces in the state.
	 * @param <V> The type of the values in the state.
	 */
	<K, N, V> SubKeyedValueState<K, N, V> createSubKeyedValueState(
		SubKeyedValueStateDescriptor<K, N, V> stateDescriptor
	) throws Exception;

	/**
	 * Creates and returns a new {@link SubKeyedListState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create.
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <N> The type of the namespaces in the state.
	 * @param <E> The type of the list elements in the state.
	 */
	<K, N, E> SubKeyedListState<K, N, E> createSubKeyedListState(
		SubKeyedListStateDescriptor<K, N, E> stateDescriptor
	) throws Exception;

	/**
	 * Creates and returns a new {@link SubKeyedMapState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <N> The type of the namespaces in the state.
	 * @param <MK> The type of the map keys in the state.
	 * @param <MV> The type of the map values in the state.
	 */
	<K, N, MK, MV> SubKeyedMapState<K, N, MK, MV> createSubKeyedMapState(
		SubKeyedMapStateDescriptor<K, N, MK, MV> stateDescriptor
	) throws Exception;

	/**
	 * Creates and returns a new {@link SubKeyedSortedMapState}.
	 *
	 * @param stateDescriptor The descriptor of the state to create.
	 *
	 * @param <K> The type of the keys in the state.
	 * @param <N> The type of the namespaces in the state.
	 * @param <MK> The type of the map keys in the state.
	 * @param <MV> The type of the map values in the state.
	 */
	<K, N, MK, MV> SubKeyedSortedMapState<K, N, MK, MV> createSubKeyedSortedMapState(
		SubKeyedSortedMapStateDescriptor<K, N, MK, MV> stateDescriptor
	) throws Exception;
}
