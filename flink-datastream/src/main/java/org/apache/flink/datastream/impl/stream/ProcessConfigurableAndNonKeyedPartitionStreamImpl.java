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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.GlobalStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.ProcessConfigurable;

/**
 * The implementation of {@link ProcessConfigurableAndNonKeyedPartitionStream}. This forwarding all
 * process methods to the underlying stream.
 */
public class ProcessConfigurableAndNonKeyedPartitionStreamImpl<T>
        extends ProcessConfigureHandle<T, ProcessConfigurableAndNonKeyedPartitionStream<T>>
        implements ProcessConfigurableAndNonKeyedPartitionStream<T> {
    private final NonKeyedPartitionStreamImpl<T> stream;

    public ProcessConfigurableAndNonKeyedPartitionStreamImpl(
            NonKeyedPartitionStreamImpl<T> stream) {
        super(stream.getEnvironment(), stream.getTransformation());
        this.stream = stream;
    }

    @Override
    public <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction) {
        return stream.process(processFunction);
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStream<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        return stream.process(processFunction);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        return stream.connectAndProcess(other, processFunction);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        return stream.connectAndProcess(other, processFunction);
    }

    @Override
    public GlobalStream<T> global() {
        return stream.global();
    }

    @Override
    public <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> keySelector) {
        return stream.keyBy(keySelector);
    }

    @Override
    public NonKeyedPartitionStream<T> shuffle() {
        return stream.shuffle();
    }

    @Override
    public BroadcastStream<T> broadcast() {
        return stream.broadcast();
    }

    @Override
    public ProcessConfigurable<?> toSink(Sink<T> sink) {
        return stream.toSink(sink);
    }

    public NonKeyedPartitionStreamImpl<T> getNonKeyedPartitionStream() {
        return stream;
    }
}
