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
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.TwoNonKeyedPartitionStreams;

/**
 * This interface represents a kind of partitioned data stream. For this stream, each key is a
 * partition, and the partition to which the data belongs is deterministic.
 */
@Experimental
public interface KeyedPartitionStream<K, T> extends DataStream {
    /**
     * Apply an operation to this {@link KeyedPartitionStream}.
     *
     * <p>This method is used to avoid shuffle after applying the process function. It is required
     * that for the same record, the new {@link KeySelector} must extract the same key as the
     * original {@link KeySelector} on this {@link KeyedPartitionStream}. Otherwise, the partition
     * of data will be messy.
     *
     * @param processFunction to perform operation.
     * @param newKeySelector to select the key after process.
     * @return new {@link KeyedPartitionStream} with this operation.
     */
    <OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);

    /**
     * Apply an operation to this {@link KeyedPartitionStream}.
     *
     * <p>Generally, apply an operation to a {@link KeyedPartitionStream} will result in a {@link
     * NonKeyedPartitionStream}, and you can manually generate a {@link KeyedPartitionStream} via
     * keyBy partitioning. In some cases, you can guarantee that the partition on which the data is
     * processed will not change, then you can use {@link #process(OneInputStreamProcessFunction,
     * KeySelector)} to avoid shuffling.
     *
     * @param processFunction to perform operation.
     * @return new {@link NonKeyedPartitionStream} with this operation.
     */
    <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction);

    /**
     * Apply a two output operation to this {@link KeyedPartitionStream}.
     *
     * <p>This method is used to avoid shuffle after applying the process function. It is required
     * that for the same record, these new two {@link KeySelector}s must extract the same key as the
     * original {@link KeySelector}s on this {@link KeyedPartitionStream}. Otherwise, the partition
     * of data will be messy.
     *
     * @param processFunction to perform two output operation.
     * @param keySelector1 to select the key of first output.
     * @param keySelector2 to select the key of second output.
     * @return new {@link TwoKeyedPartitionStreams} with this operation.
     */
    <OUT1, OUT2> TwoKeyedPartitionStreams<K, OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction,
            KeySelector<OUT1, K> keySelector1,
            KeySelector<OUT2, K> keySelector2);

    /**
     * Apply a two output operation to this {@link KeyedPartitionStream}.
     *
     * @param processFunction to perform two output operation.
     * @return new {@link TwoNonKeyedPartitionStreams} with this operation.
     */
    <OUT1, OUT2> TwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction);

    /**
     * Apply a two input operation to this and other {@link KeyedPartitionStream}. The two keyed
     * streams must have the same partitions, otherwise it makes no sense to connect them.
     *
     * <p>Generally, concatenating two {@link KeyedPartitionStream} will result in a {@link
     * NonKeyedPartitionStream}, and you can manually generate a {@link KeyedPartitionStream} via
     * keyBy partitioning. In some cases, you can guarantee that the partition on which the data is
     * processed will not change, then you can use {@link #connectAndProcess(KeyedPartitionStream,
     * TwoInputNonBroadcastStreamProcessFunction, KeySelector)} to avoid shuffling.
     *
     * @param other {@link KeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @return new {@link NonKeyedPartitionStream} with this operation.
     */
    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    /**
     * Apply a two input operation to this and other {@link KeyedPartitionStream}.The two keyed
     * streams must have the same partitions, otherwise it makes no sense to connect them.
     *
     * <p>This method is used to avoid shuffle after applying the process function. It is required
     * that for the same record, the new {@link KeySelector} must extract the same key as the
     * original {@link KeySelector}s on these two {@link KeyedPartitionStream}s. Otherwise, the
     * partition of data will be messy.
     *
     * @param other {@link KeyedPartitionStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @param newKeySelector to select the key after process.
     * @return new {@link KeyedPartitionStream} with this operation.
     */
    <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);

    /**
     * Apply a two input operation to this and other {@link BroadcastStream}.
     *
     * <p>Generally, concatenating {@link KeyedPartitionStream} and {@link BroadcastStream} will
     * result in a {@link NonKeyedPartitionStream}, and you can manually generate a {@link
     * KeyedPartitionStream} via keyBy partitioning. In some cases, you can guarantee that the
     * partition on which the data is processed will not change, then you can use {@link
     * #connectAndProcess(BroadcastStream, TwoInputBroadcastStreamProcessFunction, KeySelector)} to
     * avoid shuffling.
     *
     * @param processFunction to perform operation.
     * @return new stream with this operation.
     */
    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    /**
     * Apply a two input operation to this and other {@link BroadcastStream}.
     *
     * <p>This method is used to avoid shuffle after applying the process function. It is required
     * that for the record from non-broadcast input, the new {@link KeySelector} must extract the
     * same key as the original {@link KeySelector}s on the {@link KeyedPartitionStream}. Otherwise,
     * the partition of data will be messy. As for the record from broadcast input, the output key
     * from keyed partition itself instead of the new key selector, so the data it outputs will not
     * affect the partition.
     *
     * @param other {@link BroadcastStream} to perform operation with two input.
     * @param processFunction to perform operation.
     * @param newKeySelector to select the key after process.
     * @return new {@link KeyedPartitionStream} with this operation.
     */
    <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);

    /**
     * Coalesce this stream to a {@link GlobalStream}.
     *
     * @return the coalesced global stream.
     */
    GlobalStream<T> global();

    /**
     * Transform this stream to a new {@link KeyedPartitionStream}.
     *
     * @param keySelector to decide how to map data to partition.
     * @return the transformed stream partitioned by key.
     */
    <NEW_KEY> KeyedPartitionStream<NEW_KEY, T> keyBy(KeySelector<T, NEW_KEY> keySelector);

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

    ProcessConfigurable<?> toSink(Sink<T> sink);

    /** This interface represents a configurable {@link KeyedPartitionStream}. */
    @Experimental
    interface ProcessConfigurableAndKeyedPartitionStream<K, T>
            extends KeyedPartitionStream<K, T>,
                    ProcessConfigurable<ProcessConfigurableAndKeyedPartitionStream<K, T>> {}

    /**
     * This class represents a combination of two {@link KeyedPartitionStream}. It will be used as
     * the return value of operation with two output.
     */
    @Experimental
    interface TwoKeyedPartitionStreams<K, T1, T2> {
        /** Get the first stream. */
        ProcessConfigurableAndKeyedPartitionStream<K, T1> getFirst();

        /** Get the second stream. */
        ProcessConfigurableAndKeyedPartitionStream<K, T2> getSecond();
    }
}
