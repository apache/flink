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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface contains methods for registering keyed state with a managed store.
 */
@PublicEvolving
public interface KeyedStateStore {

	/**
	 * Gets a handle to the system's key/value state. The key/value state is only accessible
	 * if the function is executed on a KeyedStream. On each access, the state exposes the value
	 * for the key of the element currently processed by the function.
	 * Each function may have multiple partitioned states, addressed with different names.
	 *
	 * <p>Because the scope of each value is the key of the currently processed element,
	 * and the elements are distributed by the Flink runtime, the system can transparently
	 * scale out and redistribute the state and KeyedStream.
	 *
	 * <p>The following code example shows how to implement a continuous counter that counts
	 * how many times elements of a certain key occur, and emits an updated count for that
	 * element on each occurrence.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 *
	 * keyedStream.map(new RichMapFunction<MyType, Tuple2<MyType, Long>>() {
	 *
	 *     private ValueState<Long> count;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getState(
	 *                 new ValueStateDescriptor<Long>("count", LongSerializer.INSTANCE, 0L));
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         long count = state.value() + 1;
	 *         state.update(value);
	 *         return new Tuple2<>(value, count);
	 *     }
	 * });
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <T> The type of value stored in the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part of a KeyedStream).
	 */
	@PublicEvolving
	<T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties);

	/**
	 * Gets a handle to the system's key/value list state. This state is similar to the state
	 * accessed via {@link #getState(ValueStateDescriptor)}, but is optimized for state that
	 * holds lists. One can adds elements to the list, or retrieve the list as a whole.
	 *
	 * <p>This state is only accessible if the function is executed on a KeyedStream.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 *
	 * keyedStream.map(new RichFlatMapFunction<MyType, List<MyType>>() {
	 *
	 *     private ListState<MyType> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getListState(
	 *                 new ListStateDescriptor<>("myState", MyType.class));
	 *     }
	 *
	 *     public void flatMap(MyType value, Collector<MyType> out) {
	 *         if (value.isDivider()) {
	 *             for (MyType t : state.get()) {
	 *                 out.collect(t);
	 *             }
	 *         } else {
	 *             state.add(value);
	 *         }
	 *     }
	 * });
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <T> The type of value stored in the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part os a KeyedStream).
	 */
	@PublicEvolving
	<T> ListState<T> getListState(ListStateDescriptor<T> stateProperties);

	/**
	 * Gets a handle to the system's key/value reducing state. This state is similar to the state
	 * accessed via {@link #getState(ValueStateDescriptor)}, but is optimized for state that
	 * aggregates values.
	 *
	 * <p>This state is only accessible if the function is executed on a KeyedStream.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 *
	 * keyedStream.map(new RichMapFunction<MyType, List<MyType>>() {
	 *
	 *     private ReducingState<Long> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getReducingState(
	 *                 new ReducingStateDescriptor<>("sum", (a, b) -> a + b, Long.class));
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         state.add(value.count());
	 *         return new Tuple2<>(value, state.get());
	 *     }
	 * });
	 *
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <T> The type of value stored in the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part of a KeyedStream).
	 */
	@PublicEvolving
	<T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties);

	/**
	 * Gets a handle to the system's key/value folding state. This state is similar to the state
	 * accessed via {@link #getState(ValueStateDescriptor)}, but is optimized for state that
	 * aggregates values with different types.
	 *
	 * <p>This state is only accessible if the function is executed on a KeyedStream.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 * AggregateFunction<...> aggregateFunction = ...
	 *
	 * keyedStream.map(new RichMapFunction<MyType, List<MyType>>() {
	 *
	 *     private AggregatingState<MyType, Long> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getAggregatingState(
	 *                 new AggregatingStateDescriptor<>("sum", aggregateFunction, Long.class));
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         state.add(value);
	 *         return new Tuple2<>(value, state.get());
	 *     }
	 * });
	 *
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <IN> The type of the values that are added to the state.
	 * @param <ACC> The type of the accumulator (intermediate aggregation state).
	 * @param <OUT> The type of the values that are returned from the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part of a KeyedStream).
	 */
	@PublicEvolving
	<IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties);

	/**
	 * Gets a handle to the system's key/value map state. This state is similar to the state
	 * accessed via {@link #getState(ValueStateDescriptor)}, but is optimized for state that
	 * is composed of user-defined key-value pairs
	 *
	 * <p>This state is only accessible if the function is executed on a KeyedStream.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 *
	 * keyedStream.map(new RichMapFunction<MyType, List<MyType>>() {
	 *
	 *     private MapState<MyType, Long> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getMapState(
	 *                 new MapStateDescriptor<>("sum", MyType.class, Long.class));
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         return new Tuple2<>(value, state.get(value));
	 *     }
	 * });
	 *
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <UK> The type of the user keys stored in the state.
	 * @param <UV> The type of the user values stored in the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part of a KeyedStream).
	 */
	@PublicEvolving
	<UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties);
}
