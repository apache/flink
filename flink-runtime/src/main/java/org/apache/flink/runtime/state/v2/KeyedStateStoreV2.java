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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.ValueState;

import javax.annotation.Nonnull;

/** This interface contains methods for registering {@link State}. */
@Internal
public interface KeyedStateStoreV2 {

    /**
     * Gets a handle to the system's {@link ValueState}. The key/value state is only accessible if
     * the function is executed on a KeyedStream. On each access, the state exposes the value for
     * the key of the element currently processed by the function. Each function may have multiple
     * partitioned states, addressed with different names.
     *
     * <p>Because the scope of each value is the key of the currently processed element, and the
     * elements are distributed by the Flink runtime, the system can transparently scale out and
     * redistribute the state and KeyedStream.
     *
     * @param stateProperties The descriptor defining the properties of the state.
     * @param <T> The type of value stored in the state.
     * @return The partitioned state object.
     */
    <T> ValueState<T> getValueState(@Nonnull ValueStateDescriptor<T> stateProperties);

    /**
     * Gets a handle to the system's key / value list state. This state is optimized for state that
     * holds lists. One can adds elements to the list, or retrieve the list as a whole. This state
     * is only accessible if the function is executed on a KeyedStream.
     *
     * @param stateProperties The descriptor defining the properties of the state.
     * @param <T> The type of value stored in the state.
     * @return The partitioned state object.
     * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
     *     function (function is not part os a KeyedStream).
     */
    <T> ListState<T> getListState(@Nonnull ListStateDescriptor<T> stateProperties);

    /**
     * Gets a handle to the system's key/value map state. This state is only accessible if the
     * function is executed on a KeyedStream.
     *
     * @param stateProperties The descriptor defining the properties of the state.
     * @param <UK> The type of the user keys stored in the state.
     * @param <UV> The type of the user values stored in the state.
     * @return The partitioned state object.
     * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
     *     function (function is not part of a KeyedStream).
     */
    <UK, UV> MapState<UK, UV> getMapState(@Nonnull MapStateDescriptor<UK, UV> stateProperties);

    /**
     * Gets a handle to the system's key/value reducing state. This state is optimized for state
     * that aggregates values.
     *
     * <p>This state is only accessible if the function is executed on a KeyedStream.
     *
     * @param stateProperties The descriptor defining the properties of the stats.
     * @param <T> The type of value stored in the state.
     * @return The partitioned state object.
     * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
     *     function (function is not part of a KeyedStream).
     */
    <T> ReducingState<T> getReducingState(@Nonnull ReducingStateDescriptor<T> stateProperties);

    /**
     * Gets a handle to the system's key/value aggregating state. This state is only accessible if
     * the function is executed on a KeyedStream.
     *
     * @param stateProperties The descriptor defining the properties of the stats.
     * @param <IN> The type of the values that are added to the state.
     * @param <ACC> The type of the accumulator (intermediate aggregation state).
     * @param <OUT> The type of the values that are returned from the state.
     * @return The partitioned state object.
     * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
     *     function (function is not part of a KeyedStream).
     */
    <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            @Nonnull AggregatingStateDescriptor<IN, ACC, OUT> stateProperties);
}
