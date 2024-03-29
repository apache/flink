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
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;

/**
 * This interface represents a kind of partitioned data stream. For this stream, each parallelism is
 * a partition, and the partition to which the data belongs is random.
 */
@Experimental
public interface NonKeyedPartitionStream<T> extends DataStream {
    /**
     * Apply an operation to this {@link NonKeyedPartitionStream}.
     *
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <OUT> NonKeyedPartitionStream<OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction);

    /**
     * Apply a two output operation to this {@link NonKeyedPartitionStream}.
     *
     * @param processFunction to perform two output operation.
     * @return new stream with this operation.
     */
    <OUT1, OUT2> TwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction);

    /**
     * Apply to a two input operation on this and other {@link NonKeyedPartitionStream}.
     *
     * @param other {@link NonKeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    /**
     * Apply a two input operation to this and other {@link BroadcastStream}.
     *
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <T_OTHER, OUT> NonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    /**
     * Coalesce this stream to a {@link GlobalStream}.
     *
     * @return the coalesced global stream.
     */
    GlobalStream<T> global();

    /**
     * Transform this stream to a {@link KeyedPartitionStream}.
     *
     * @param keySelector to decide how to map data to partition.
     * @return the transformed stream partitioned by key.
     */
    <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> keySelector);

    /**
     * Transform this stream to a new {@link NonKeyedPartitionStream}, data will be shuffled between
     * these two streams.
     *
     * @return the transformed stream after shuffle.
     */
    NonKeyedPartitionStream<T> shuffle();

    /**
     * Transform this stream to a new {@link BroadcastStream}.
     *
     * @return the transformed {@link BroadcastStream}.
     */
    BroadcastStream<T> broadcast();

    void toSink(Sink<T> sink);

    /**
     * This interface represents a combination of two {@link NonKeyedPartitionStream}. It will be
     * used as the return value of operation with two output.
     */
    @Experimental
    interface TwoNonKeyedPartitionStreams<T1, T2> {
        /** Get the first stream. */
        NonKeyedPartitionStream<T1> getFirst();

        /** Get the second stream. */
        NonKeyedPartitionStream<T2> getSecond();
    }
}
