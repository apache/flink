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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.ExecutionOptions;

/**
 * {@link PartitionWindowedStream} represents a data stream that collects all records of each
 * partition separately into a full window. Window emission will be triggered at the end of inputs.
 * For non-keyed {@link DataStream}, a partition contains all records of a subtask. For {@link
 * KeyedStream}, a partition contains all records of a key.
 *
 * @param <T> The type of the elements in this stream.
 */
@PublicEvolving
public interface PartitionWindowedStream<T> {

    /**
     * Process the records of the window by {@link MapPartitionFunction}.
     *
     * @param mapPartitionFunction The map partition function.
     * @param <R> The type of map partition result.
     * @return The data stream with map partition result.
     */
    <R> SingleOutputStreamOperator<R> mapPartition(MapPartitionFunction<T, R> mapPartitionFunction);

    /**
     * Applies a reduce transformation on the records of the window.
     *
     * @param reduceFunction The reduce function.
     * @return The data stream with final reduced result.
     */
    SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reduceFunction);

    /**
     * Applies an aggregate transformation on the records of the window.
     *
     * @param aggregateFunction The aggregate function.
     * @param <ACC> The type of accumulator in aggregate function.
     * @param <R> The type of aggregate function result.
     * @return The data stream with final aggregated result.
     */
    <ACC, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, R> aggregateFunction);

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be {@link Tuple}.
     *
     * <p>This operator will use managed memory for the sort.For {@link
     * NonKeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.partition.memory" in {@link ExecutionOptions}. For {@link
     * KeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.keyed.partition.memory" in {@link ExecutionOptions}.
     *
     * @param field The field index on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The data stream with sorted records.
     */
    SingleOutputStreamOperator<T> sortPartition(int field, Order order);

    /**
     * Sorts the records of the window on the specified field in the specified order. The type of
     * records must be Flink POJO {@link PojoTypeInfo}. A type is considered a Flink POJO type, if
     * it fulfills the conditions below.
     *
     * <ul>
     *   <li>It is a public class, and standalone (not a non-static inner class).
     *   <li>It has a public no-argument constructor.
     *   <li>All non-static, non-transient fields in the class (and all superclasses) are either
     *       public (and non-final) or have a public getter and a setter method that follows the
     *       Java beans naming conventions for getters and setters.
     *   <li>It is a fixed-length, null-aware composite type with non-deterministic field order.
     *       Every field can be null independent of the field's type.
     * </ul>
     *
     * <p>This operator will use managed memory for the sort.For {@link
     * NonKeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.partition.memory" in {@link ExecutionOptions}. For {@link
     * KeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.keyed.partition.memory" in {@link ExecutionOptions}.
     *
     * @param field The field expression referring to the field on which records is sorted.
     * @param order The order in which records is sorted.
     * @return The data stream with sorted records.
     */
    SingleOutputStreamOperator<T> sortPartition(String field, Order order);

    /**
     * Sorts the records according to a {@link KeySelector} in the specified order.
     *
     * <p>This operator will use managed memory for the sort.For {@link
     * NonKeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.partition.memory" in {@link ExecutionOptions}. For {@link
     * KeyedPartitionWindowedStream}, the managed memory size can be set by
     * "execution.sort.keyed.partition.memory" in {@link ExecutionOptions}.
     *
     * @param keySelector The key selector to extract key from the records for sorting.
     * @param order The order in which records is sorted.
     * @return The data stream with sorted records.
     */
    <K> SingleOutputStreamOperator<T> sortPartition(KeySelector<T, K> keySelector, Order order);
}
