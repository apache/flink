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

package org.apache.flink.datastream.api.stream;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;

/** This interface represents a stream that each parallel task processes the same data. */
@Experimental
public interface BroadcastStream<T> extends DataStream {
    /**
     * Apply a two input operation to this and other {@link KeyedPartitionStream}.
     *
     * <p>Generally, concatenating {@link BroadcastStream} and {@link KeyedPartitionStream} will
     * result in a {@link NonKeyedPartitionStream}, and you can manually generate a {@link
     * KeyedPartitionStream} via keyBy partitioning. In some cases, you can guarantee that the
     * partition on which the data is processed will not change, then you can use {@link
     * #connectAndProcess(KeyedPartitionStream, TwoInputBroadcastStreamProcessFunction,
     * KeySelector)} to avoid shuffling.
     *
     * @param other {@link KeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <K, T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction);

    /**
     * Apply a two input operation to this and other {@link NonKeyedPartitionStream}.
     *
     * @param other {@link NonKeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction);

    /**
     * Apply a two input operation to this and other {@link KeyedPartitionStream}.
     *
     * <p>This method is used to avoid shuffle after applying the process function. It is required
     * that for the record from non-broadcast input, the new {@link KeySelector} must extract the
     * same key as the original {@link KeySelector}s on the {@link KeyedPartitionStream}. Otherwise,
     * the partition of data will be messy. As for the record from broadcast input, the output key
     * from keyed partition itself instead of the new key selector, so the data it outputs will not
     * affect the partition.
     *
     * @param other {@link KeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @param newKeySelector to select the key after process.
     * @return new {@link KeyedPartitionStream} with this operation.
     */
    <K, T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);
}
