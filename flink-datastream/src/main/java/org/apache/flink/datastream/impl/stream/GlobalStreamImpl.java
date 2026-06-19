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

import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.dsv2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
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
import org.apache.flink.datastream.impl.operators.TwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoOutputProcessOperator;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.DataStreamV2SinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.flink.datastream.impl.utils.StreamUtils.validateStates;

/** The implementation of {@link GlobalStream}. */
public class GlobalStreamImpl<T> extends AbstractDataStream<T> implements GlobalStream<T> {
    public GlobalStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <OUT> ProcessConfigurableAndGlobalStream<OUT> process(
            OneInputStreamProcessFunction<T, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(processFunction, getType());

        if (processFunction instanceof InternalOneInputWindowStreamProcessFunction) {
            Transformation<OUT> outTransformation =
                    StreamUtils.transformOneInputWindow(
                            environment.getExecutionConfig(),
                            this,
                            getType(),
                            outType,
                            (InternalOneInputWindowStreamProcessFunction<T, OUT, ?>)
                                    processFunction,
                            new NullByteKeySelector<>(),
                            Types.BYTE);
            outTransformation.setParallelism(1, true);
            return StreamUtils.wrapWithConfigureHandle(
                    new GlobalStreamImpl<>(this.environment, outTransformation));
        } else {
            ProcessOperator<T, OUT> operator = new ProcessOperator<>(processFunction);
            return StreamUtils.wrapWithConfigureHandle(
                    transform(
                            "Global Process",
                            outType,
                            operator,
                            AttributeParser.parseAttribute(processFunction)));
        }
    }

    @Override
    public <OUT1, OUT2> TwoGlobalStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> twoOutputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(processFunction, getType());
        TypeInformation<OUT1> firstOutputType = twoOutputType.f0;
        TypeInformation<OUT2> secondOutputType = twoOutputType.f1;
        OutputTag<OUT2> secondOutputTag = new OutputTag<>("Second-Output", secondOutputType);

        GlobalStreamImpl<OUT1> firstStream;
        if (processFunction instanceof InternalTwoOutputWindowStreamProcessFunction) {
            Transformation<OUT1> outTransformation =
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
            outTransformation.setParallelism(1, true);
            firstStream = new GlobalStreamImpl<>(environment, outTransformation);
        } else {
            TwoOutputProcessOperator<T, OUT1, OUT2> operator =
                    new TwoOutputProcessOperator<>(processFunction, secondOutputTag);
            firstStream =
                    transform(
                            "Two-Output-Operator",
                            firstOutputType,
                            operator,
                            AttributeParser.parseAttribute(processFunction));
        }
        GlobalStreamImpl<OUT2> secondStream =
                new GlobalStreamImpl<>(
                        environment, firstStream.getSideOutputTransform(secondOutputTag));
        return TwoGlobalStreamsImpl.of(firstStream, secondStream);
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndGlobalStream<OUT> connectAndProcess(
            GlobalStream<T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<T, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Arrays.asList(
                                StateDeclaration.RedistributionMode.NONE,
                                StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction, getType(), ((GlobalStreamImpl<T_OTHER>) other).getType());

        Transformation<OUT> outTransformation;
        if (processFunction instanceof InternalTwoInputWindowStreamProcessFunction) {
            outTransformation =
                    StreamUtils.transformTwoInputNonBroadcastWindow(
                            environment.getExecutionConfig(),
                            getTransformation(),
                            getType(),
                            ((GlobalStreamImpl<T_OTHER>) other).getTransformation(),
                            ((GlobalStreamImpl<T_OTHER>) other).getType(),
                            outTypeInfo,
                            (InternalTwoInputWindowStreamProcessFunction<T, T_OTHER, OUT, ?>)
                                    processFunction,
                            new NullByteKeySelector<>(),
                            Types.BYTE,
                            new NullByteKeySelector<>(),
                            Types.BYTE);
        } else {
            TwoInputNonBroadcastProcessOperator<T, T_OTHER, OUT> processOperator =
                    new TwoInputNonBroadcastProcessOperator<>(processFunction);
            outTransformation =
                    StreamUtils.getTwoInputTransformation(
                            "Global-Global-TwoInput-Process",
                            this,
                            (GlobalStreamImpl<T_OTHER>) other,
                            outTypeInfo,
                            processOperator);
        }
        // Operator parallelism should always be 1 for global stream.
        // parallelismConfigured should be true to avoid overwritten by AdaptiveBatchScheduler.
        outTransformation.setParallelism(1, true);
        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new GlobalStreamImpl<>(environment, outTransformation));
    }

    @Override
    public ProcessConfigurable<?> toSink(Sink<T> sink) {
        DataStreamV2SinkTransformation<T, T> sinkTransformation =
                StreamUtils.addSinkOperator(this, sink, getType());
        // Operator parallelism should always be 1 for global stream.
        // parallelismConfigured should be true to avoid overwritten by AdaptiveBatchScheduler.
        sinkTransformation.setParallelism(1, true);
        return StreamUtils.wrapWithConfigureHandle(
                new GlobalStreamImpl<>(environment, sinkTransformation));
    }

    // ---------------------
    //   Partitioning
    // ---------------------

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

    private <R> GlobalStreamImpl<R> transform(
            String operatorName,
            TypeInformation<R> outputTypeInfo,
            OneInputStreamOperator<T, R> operator,
            Attribute attribute) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        transformation.getOutputType();

        OneInputTransformation<T, R> resultTransform =
                new OneInputTransformation<>(
                        this.transformation,
                        operatorName,
                        SimpleUdfStreamOperatorFactory.of(operator),
                        outputTypeInfo,
                        // Operator parallelism should always be 1 for global stream.
                        1,
                        // parallelismConfigured should be true to avoid overwritten by
                        // AdaptiveBatchScheduler.
                        true);
        resultTransform.setAttribute(attribute);
        GlobalStreamImpl<R> returnStream = new GlobalStreamImpl<>(environment, resultTransform);

        environment.addOperator(resultTransform);

        return returnStream;
    }

    private static class TwoGlobalStreamsImpl<OUT1, OUT2> implements TwoGlobalStreams<OUT1, OUT2> {

        private final GlobalStreamImpl<OUT1> firstStream;

        private final GlobalStreamImpl<OUT2> secondStream;

        public static <OUT1, OUT2> TwoGlobalStreamsImpl<OUT1, OUT2> of(
                GlobalStreamImpl<OUT1> firstStream, GlobalStreamImpl<OUT2> secondStream) {
            return new TwoGlobalStreamsImpl<>(firstStream, secondStream);
        }

        private TwoGlobalStreamsImpl(
                GlobalStreamImpl<OUT1> firstStream, GlobalStreamImpl<OUT2> secondStream) {
            this.firstStream = firstStream;
            this.secondStream = secondStream;
        }

        @Override
        public ProcessConfigurableAndGlobalStream<OUT1> getFirst() {
            return StreamUtils.wrapWithConfigureHandle(firstStream);
        }

        @Override
        public ProcessConfigurableAndGlobalStream<OUT2> getSecond() {
            return StreamUtils.wrapWithConfigureHandle(secondStream);
        }
    }
}
