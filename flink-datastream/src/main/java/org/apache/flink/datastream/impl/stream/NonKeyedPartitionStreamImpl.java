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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.GlobalStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.ProcessConfigurable;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.operators.ProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoOutputProcessOperator;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;

/** The implementation of {@link NonKeyedPartitionStream}. */
public class NonKeyedPartitionStreamImpl<T> extends AbstractDataStream<T>
        implements NonKeyedPartitionStream<T> {
    public NonKeyedPartitionStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction) {
        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(processFunction, getType());
        ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);
        OneInputTransformation<T, OUT> outputTransform =
                StreamUtils.getOneInputTransformation("Process", this, outType, operator);
        environment.addOperator(outputTransform);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outputTransform));
    }

    @Override
    public <OUT1, OUT2> TwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondOutputType);

        TwoOutputProcessOperator<T, OUT1, OUT2> operator =
                new TwoOutputProcessOperator<>(processFunction, secondOutputTag);
        OneInputTransformation<T, OUT1> outTransformation =
                StreamUtils.getOneInputTransformation(
                        "Two-Output-Operator", this, firstOutputType, operator);
        NonKeyedPartitionStreamImpl<OUT1> firstStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        NonKeyedPartitionStreamImpl<OUT2> secondStream =
                new NonKeyedPartitionStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        environment.addOperator(outTransformation);
        return TwoNonKeyedPartitionStreamsImpl.of(firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((NonKeyedPartitionStreamImpl<T_OTHER>) other).getType());

        TwoInputNonBroadcastProcessOperator<T, T_OTHER, OUT> processOperator =
                new TwoInputNonBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "TwoInput-Process",
                        this,
                        (NonKeyedPartitionStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((BroadcastStreamImpl<T_OTHER>) other).getType());

        TwoInputBroadcastProcessOperator<T, T_OTHER, OUT> processOperator =
                new TwoInputBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Broadcast-TwoInput-Process",
                        this,
                        // we should always take the broadcast input as second input.
                        (BroadcastStreamImpl<T_OTHER>) other,
                        outTypeInfo,
                        processOperator);
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public ProcessConfigurable<?> toSink(Sink<T> sink) {
        DataStreamV2SinkTransformation<T, T> sinkTransformation =
                StreamUtils.addSinkOperator(this, sink, getType());
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, sinkTransformation));
    }

    // ---------------------
    //   Partitioning
    // ---------------------

    @Override
    public GlobalStream<T> global() {
        return new GlobalStreamImpl<>(
                environment,
                new PartitionTransformation<>(transformation, new GlobalPartitioner<>()));
    }

    @Override
    public <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> keySelector) {
        return new KeyedPartitionStreamImpl<>(this, keySelector);
    }

    @Override
    public NonKeyedPartitionStream<T> shuffle() {
        return new NonKeyedPartitionStreamImpl<>(
                environment,
                new PartitionTransformation<>(getTransformation(), new ShufflePartitioner<>()));
    }

    @Override
    public BroadcastStream<T> broadcast() {
        return new BroadcastStreamImpl<>(environment, getTransformation());
    }

    static class TwoNonKeyedPartitionStreamsImpl<OUT1, OUT2>
            implements TwoNonKeyedPartitionStreams<OUT1, OUT2> {

        private final NonKeyedPartitionStreamImpl<OUT1> firstStream;

        private final NonKeyedPartitionStreamImpl<OUT2> secondStream;

        public static <OUT1, OUT2> TwoNonKeyedPartitionStreamsImpl<OUT1, OUT2> of(
                NonKeyedPartitionStreamImpl<OUT1> firstStream,
                NonKeyedPartitionStreamImpl<OUT2> secondStream) {
            return new TwoNonKeyedPartitionStreamsImpl<>(firstStream, secondStream);
        }

        private TwoNonKeyedPartitionStreamsImpl(
                NonKeyedPartitionStreamImpl<OUT1> firstStream,
                NonKeyedPartitionStreamImpl<OUT2> secondStream) {
            this.firstStream = firstStream;
            this.secondStream = secondStream;
        }

        @Override
        public ProcessConfigurableAndNonKeyedPartitionStream<OUT1> getFirst() {
            return StreamUtils.wrapWithConfigureHandle(firstStream);
        }

        @Override
        public ProcessConfigurableAndNonKeyedPartitionStream<OUT2> getSecond() {
            return StreamUtils.wrapWithConfigureHandle(secondStream);
        }
    }
}
