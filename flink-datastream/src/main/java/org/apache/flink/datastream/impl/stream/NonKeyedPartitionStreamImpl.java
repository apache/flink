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

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.extension.window.strategy.GlobalWindowStrategy;
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
import org.apache.flink.datastream.impl.attribute.AttributeParser;
import org.apache.flink.datastream.impl.extension.window.function.InternalOneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoOutputWindowStreamProcessFunction;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.flink.datastream.impl.utils.StreamUtils.validateStates;
import static org.apache.flink.util.Preconditions.checkState;

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
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Arrays.asList(
                                StateDeclaration.RedistributionMode.NONE,
                                StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(processFunction, getType());
        OneInputTransformation<T, OUT> outputTransform;

        if (processFunction instanceof InternalOneInputWindowStreamProcessFunction) {
            InternalOneInputWindowStreamProcessFunction<T, OUT, ?> internalProcessFunction =
                    (InternalOneInputWindowStreamProcessFunction<T, OUT, ?>) processFunction;
            checkState(
                    internalProcessFunction.getWindowStrategy() instanceof GlobalWindowStrategy,
                    "Only the Global Window is permitted for execution in NonKeyedPartitionStream.");
            outputTransform =
                    (OneInputTransformation<T, OUT>)
                            StreamUtils.transformOneInputWindow(
                                    environment.getExecutionConfig(),
                                    this,
                                    getType(),
                                    outType,
                                    (InternalOneInputWindowStreamProcessFunction<T, OUT, ?>)
                                            processFunction,
                                    new NullByteKeySelector<>(),
                                    Types.BYTE);
        } else {
            ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);
            outputTransform =
                    StreamUtils.getOneInputTransformation("Process", this, outType, operator);
        }

        outputTransform.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outputTransform);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outputTransform));
    }

    @Override
    public <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStream<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Arrays.asList(
                                StateDeclaration.RedistributionMode.NONE,
                                StateDeclaration.RedistributionMode.IDENTICAL)));

        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondOutputType);

        OneInputTransformation<T, OUT1> outTransformation;
        if (processFunction instanceof InternalTwoOutputWindowStreamProcessFunction) {
            InternalTwoOutputWindowStreamProcessFunction<T, OUT1, OUT2, ?> internalProcessFunction =
                    (InternalTwoOutputWindowStreamProcessFunction<T, OUT1, OUT2, ?>)
                            processFunction;
            checkState(
                    internalProcessFunction.getWindowStrategy() instanceof GlobalWindowStrategy,
                    "Only the Global Window is permitted for execution in NonKeyedPartitionStream.");

            outTransformation =
                    (OneInputTransformation<T, OUT1>)
                            StreamUtils.transformTwoOutputWindow(
                                    environment.getExecutionConfig(),
                                    this,
                                    getType(),
                                    firstOutputType,
                                    secondOutputType,
                                    secondOutputTag,
                                    (InternalTwoOutputWindowStreamProcessFunction<T, OUT1, OUT2, ?>)
                                            processFunction,
                                    new NullByteKeySelector<>(),
                                    Types.BYTE);
        } else {
            TwoOutputProcessOperator<T, OUT1, OUT2> operator =
                    new TwoOutputProcessOperator<>(processFunction, secondOutputTag);
            outTransformation =
                    StreamUtils.getOneInputTransformation(
                            "Two-Output-Operator", this, firstOutputType, operator);
        }
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        NonKeyedPartitionStreamImpl<OUT1> firstStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        NonKeyedPartitionStreamImpl<OUT2> secondStream =
                new NonKeyedPartitionStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        environment.addOperator(outTransformation);
        return new ProcessConfigurableAndTwoNonKeyedPartitionStreamImpl<>(
                environment, outTransformation, firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Arrays.asList(
                                StateDeclaration.RedistributionMode.NONE,
                                StateDeclaration.RedistributionMode.IDENTICAL)));
        other =
                other instanceof ProcessConfigurableAndNonKeyedPartitionStreamImpl
                        ? ((ProcessConfigurableAndNonKeyedPartitionStreamImpl) other)
                                .getNonKeyedPartitionStream()
                        : other;
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        getType(),
                        ((NonKeyedPartitionStreamImpl<T_OTHER>) other).getType());

        Transformation<OUT> outTransformation;
        if (processFunction instanceof InternalTwoInputWindowStreamProcessFunction) {
            InternalTwoInputWindowStreamProcessFunction<T, T_OTHER, OUT, ?> internalWindowFunction =
                    (InternalTwoInputWindowStreamProcessFunction<T, T_OTHER, OUT, ?>)
                            processFunction;
            checkState(
                    internalWindowFunction.getWindowStrategy() instanceof GlobalWindowStrategy,
                    "Only the Global Window is permitted for execution in NonKeyedPartitionStream.");

            outTransformation =
                    StreamUtils.transformTwoInputNonBroadcastWindow(
                            environment.getExecutionConfig(),
                            this.transformation,
                            getType(),
                            ((NonKeyedPartitionStreamImpl<T_OTHER>) other).getTransformation(),
                            ((NonKeyedPartitionStreamImpl<T_OTHER>) other).getType(),
                            outTypeInfo,
                            internalWindowFunction,
                            new NullByteKeySelector<>(),
                            Types.BYTE,
                            new NullByteKeySelector<>(),
                            Types.BYTE);
        } else {
            TwoInputNonBroadcastProcessOperator<T, T_OTHER, OUT> processOperator =
                    new TwoInputNonBroadcastProcessOperator<>(processFunction);
            outTransformation =
                    StreamUtils.getTwoInputTransformation(
                            "TwoInput-Process",
                            this,
                            (NonKeyedPartitionStreamImpl<T_OTHER>) other,
                            outTypeInfo,
                            processOperator);
        }
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(Collections.singletonList(StateDeclaration.RedistributionMode.NONE)));

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
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
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
}
