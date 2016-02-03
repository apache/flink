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

/**
 * The {@code StateBackend} is used by {@link StateDescriptor} instances to create actual state
 * representations.
 */
public interface StateBackend {

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
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	<T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception;

}
