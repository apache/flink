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
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

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
}
