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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Internal;

/**
 * The {@code StateBinder} is used by {@link StateDescriptor} instances to create actual
 * {@link State} objects.
 */
@Internal
public interface StateBinder {

	/**
	 * Creates and returns a new {@link ValueState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <T> The type of the value that the {@code ValueState} can store.
	 */
	<T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ListState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	<T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ReducingState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <T> The type of the values that the {@code ReducingState} can store.
	 */
	<T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link AggregatingState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <IN> The type of the values that go into the aggregating state
	 * @param <ACC> The type of the values that are stored in the aggregating state
	 * @param <OUT> The type of the values that come out of the aggregating state
	 */
	<IN, ACC, OUT> AggregatingState<IN, OUT> createAggregatingState(
			AggregatingStateDescriptor<IN, ACC, OUT> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link FoldingState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <T> Type of the values folded into the state
	 * @param <ACC> Type of the value in the state
	 *
	 * @deprecated will be removed in a future version in favor of {@link AggregatingState}
	 */
	@Deprecated
	<T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link MapState}.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <MK> Type of the keys in the state
	 * @param <MV> Type of the values in the state
	 */
	<MK, MV> MapState<MK, MV> createMapState(MapStateDescriptor<MK, MV> stateDesc) throws Exception;
}
