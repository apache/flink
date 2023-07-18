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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionStateBackend;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Tests for generating correct properties for sorting inputs in {@link RuntimeExecutionMode#BATCH}
 * runtime mode.
 */
public class StreamGraphGeneratorBatchExecutionTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testShuffleMode() {
        testGlobalStreamExchangeMode(
                RuntimeExecutionMode.AUTOMATIC,
                BatchShuffleMode.ALL_EXCHANGES_BLOCKING,
                GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);

        testGlobalStreamExchangeMode(
                RuntimeExecutionMode.STREAMING,
                BatchShuffleMode.ALL_EXCHANGES_BLOCKING,
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED);

        testGlobalStreamExchangeMode(
                RuntimeExecutionMode.BATCH,
                BatchShuffleMode.ALL_EXCHANGES_PIPELINED,
                GlobalStreamExchangeMode.ALL_EDGES_PIPELINED);

        testGlobalStreamExchangeMode(
                RuntimeExecutionMode.BATCH,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL,
                GlobalStreamExchangeMode.ALL_EDGES_HYBRID_FULL);

        testGlobalStreamExchangeMode(
                RuntimeExecutionMode.BATCH,
                BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE,
                GlobalStreamExchangeMode.ALL_EDGES_HYBRID_SELECTIVE);
    }

    @Test
    public void testBatchJobType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Integer> sink = addDummyPipeline(env);

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        assertThat(graph.getJobType(), is(JobType.BATCH));
    }

    @Test
    public void testManagedMemoryWeights() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);
        StreamNode processNode = graph.getStreamNode(process.getId());

        final Map<ManagedMemoryUseCase, Integer> expectedOperatorWeights = new HashMap<>();
        expectedOperatorWeights.put(
                ManagedMemoryUseCase.OPERATOR,
                ExecutionOptions.SORTED_INPUTS_MEMORY.defaultValue().getMebiBytes());
        assertThat(
                processNode.getManagedMemoryOperatorScopeUseCaseWeights(),
                equalTo(expectedOperatorWeights));
        assertThat(
                processNode.getManagedMemorySlotScopeUseCases(),
                equalTo(Collections.singleton(ManagedMemoryUseCase.STATE_BACKEND)));
    }

    @Test
    public void testCustomManagedMemoryWeights() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORTED_INPUTS_MEMORY, MemorySize.ofMebiBytes(42));

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);
        StreamNode processNode = graph.getStreamNode(process.getId());

        final Map<ManagedMemoryUseCase, Integer> expectedOperatorWeights = new HashMap<>();
        expectedOperatorWeights.put(ManagedMemoryUseCase.OPERATOR, 42);
        assertThat(
                processNode.getManagedMemoryOperatorScopeUseCaseWeights(),
                equalTo(expectedOperatorWeights));
        assertThat(
                processNode.getManagedMemorySlotScopeUseCases(),
                equalTo(Collections.singleton(ManagedMemoryUseCase.STATE_BACKEND)));
    }

    @Test
    public void testOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(
                processNode.getInputRequirements().get(0),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getOperatorFactory().getChainingStrategy(),
                equalTo(ChainingStrategy.HEAD));
        assertThat(graph.getStateBackend(), instanceOf(BatchExecutionStateBackend.class));
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider(), notNullValue());
    }

    @Test
    public void testDisablingStateBackendOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(
                processNode.getInputRequirements().get(0),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getOperatorFactory().getChainingStrategy(),
                equalTo(ChainingStrategy.HEAD));
        assertThat(graph.getStateBackend(), nullValue());
        assertThat(graph.getTimerServiceProvider(), nullValue());
    }

    @Test
    public void testDisablingSortingInputsOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0), nullValue());
        assertThat(graph.getStateBackend(), nullValue());
        assertThat(graph.getTimerServiceProvider(), nullValue());
    }

    @Test
    public void testDisablingSortingInputsWithoutBatchStateBackendOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromElements(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "Batch state backend requires the sorted inputs to be enabled!");
        getStreamGraphInBatchMode(sink, configuration);
    }

    @Test
    public void testTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(
                processNode.getInputRequirements().get(0),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getInputRequirements().get(1),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getOperatorFactory().getChainingStrategy(),
                equalTo(ChainingStrategy.HEAD));
        assertThat(graph.getStateBackend(), instanceOf(BatchExecutionStateBackend.class));
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider(), notNullValue());
    }

    @Test
    public void testDisablingStateBackendTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(
                processNode.getInputRequirements().get(0),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getInputRequirements().get(1),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                processNode.getOperatorFactory().getChainingStrategy(),
                equalTo(ChainingStrategy.HEAD));
        assertThat(graph.getStateBackend(), nullValue());
        assertThat(graph.getTimerServiceProvider(), nullValue());
    }

    @Test
    public void testDisablingSortingInputsTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0), nullValue());
        assertThat(processNode.getInputRequirements().get(1), nullValue());
        assertThat(graph.getStateBackend(), nullValue());
        assertThat(graph.getTimerServiceProvider(), nullValue());
    }

    @Test
    public void testDisablingSortingInputsWithoutBatchStateBackendTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "Batch state backend requires the sorted inputs to be enabled!");
        getStreamGraphInBatchMode(sink, configuration);
    }

    @Test
    public void testInputSelectableTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);

        SingleOutputStreamOperator<Integer> selectableOperator =
                process.connect(elements1)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .transform(
                                "operator",
                                BasicTypeInfo.INT_TYPE_INFO,
                                new InputSelectableTwoInputOperator());

        DataStreamSink<Integer> sink = selectableOperator.sinkTo(new DiscardingSink<>());

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "Batch state backend and sorting inputs are not supported in graphs with an InputSelectable operator.");
        getStreamGraphInBatchMode(sink);
    }

    @Test
    public void testMultiInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements3 = env.fromElements(1, 2);

        MultipleInputOperatorFactory selectableOperator =
                new MultipleInputOperatorFactory(3, false);
        KeyedMultipleInputTransformation<Integer> multipleInputTransformation =
                new KeyedMultipleInputTransformation<>(
                        "operator",
                        selectableOperator,
                        BasicTypeInfo.INT_TYPE_INFO,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO);
        multipleInputTransformation.addInput(elements1.getTransformation(), e -> e);
        multipleInputTransformation.addInput(elements2.getTransformation(), e -> e);
        multipleInputTransformation.addInput(elements3.getTransformation(), e -> e);

        DataStreamSink<Integer> sink =
                new MultipleConnectedStreams(env)
                        .transform(multipleInputTransformation)
                        .sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        StreamNode operatorNode = graph.getStreamNode(multipleInputTransformation.getId());
        assertThat(
                operatorNode.getInputRequirements().get(0),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                operatorNode.getInputRequirements().get(1),
                equalTo(StreamConfig.InputRequirement.SORTED));
        assertThat(
                operatorNode.getOperatorFactory().getChainingStrategy(),
                equalTo(ChainingStrategy.HEAD));
        assertThat(graph.getStateBackend(), instanceOf(BatchExecutionStateBackend.class));
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider(), notNullValue());
    }

    @Test
    public void testInputSelectableMultiInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements2 = env.fromElements(1, 2);
        DataStreamSource<Integer> elements3 = env.fromElements(1, 2);

        MultipleInputOperatorFactory selectableOperator = new MultipleInputOperatorFactory(3, true);
        KeyedMultipleInputTransformation<Integer> multipleInputTransformation =
                new KeyedMultipleInputTransformation<>(
                        "operator",
                        selectableOperator,
                        BasicTypeInfo.INT_TYPE_INFO,
                        1,
                        BasicTypeInfo.INT_TYPE_INFO);
        multipleInputTransformation.addInput(elements1.getTransformation(), e -> e);
        multipleInputTransformation.addInput(elements2.getTransformation(), e -> e);
        multipleInputTransformation.addInput(elements3.getTransformation(), e -> e);

        DataStreamSink<Integer> sink =
                new MultipleConnectedStreams(env)
                        .transform(multipleInputTransformation)
                        .sinkTo(new DiscardingSink<>());

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(
                "Batch state backend and sorting inputs are not supported in graphs with an InputSelectable operator.");
        getStreamGraphInBatchMode(sink);
    }

    @Test
    public void testFeedbackThrowsExceptionInBatch() {
        final SourceTransformation<Integer, ?, ?> bounded =
                new SourceTransformation<>(
                        "Bounded Source",
                        new MockSource(Boundedness.BOUNDED, 100),
                        WatermarkStrategy.noWatermarks(),
                        IntegerTypeInfo.of(Integer.class),
                        1);

        final FeedbackTransformation<Integer> feedbackTransformation =
                new FeedbackTransformation<>(bounded, 5L);

        testNoSupportForIterationsInBatchHelper(bounded, feedbackTransformation);
    }

    @Test
    public void testCoFeedbackThrowsExceptionInBatch() {
        final CoFeedbackTransformation<Integer> coFeedbackTransformation =
                new CoFeedbackTransformation<>(2, TypeInformation.of(Integer.TYPE), 5L);
        testNoSupportForIterationsInBatchHelper(coFeedbackTransformation);
    }

    private void testNoSupportForIterationsInBatchHelper(
            final Transformation<?>... transformations) {
        final List<Transformation<?>> registeredTransformations = new ArrayList<>();
        Collections.addAll(registeredTransformations, transformations);

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        StreamGraphGenerator streamGraphGenerator =
                new StreamGraphGenerator(
                        registeredTransformations,
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        configuration);

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Iterations are not supported in BATCH execution mode.");
        streamGraphGenerator.generate();
    }

    private void testGlobalStreamExchangeMode(
            RuntimeExecutionMode runtimeExecutionMode,
            BatchShuffleMode shuffleMode,
            GlobalStreamExchangeMode expectedStreamExchangeMode) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSink<Integer> sink = addDummyPipeline(env);

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
        configuration.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode);

        StreamGraphGenerator streamGraphGenerator =
                new StreamGraphGenerator(
                        Collections.singletonList(sink.getTransformation()),
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        configuration);

        StreamGraph graph = streamGraphGenerator.generate();

        assertEquals(expectedStreamExchangeMode, graph.getGlobalStreamExchangeMode());
    }

    private DataStreamSink<Integer> addDummyPipeline(StreamExecutionEnvironment env) {
        return env.fromElements(1, 2)
                .keyBy(Integer::intValue)
                .process(DUMMY_PROCESS_FUNCTION)
                .sinkTo(new DiscardingSink<>());
    }

    private StreamGraph getStreamGraphInBatchMode(DataStreamSink<?> sink) {
        return getStreamGraphInBatchMode(sink, new Configuration());
    }

    private StreamGraph getStreamGraphInBatchMode(
            DataStreamSink<?> sink, Configuration configuration) {
        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.configure(configuration, StreamGraphGenerator.class.getClassLoader());

        final CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.configure(configuration);

        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        return new StreamGraphGenerator(
                        Collections.singletonList(sink.getTransformation()),
                        executionConfig,
                        checkpointConfig,
                        configuration)
                .generate();
    }

    private static final KeyedProcessFunction<Integer, Integer, Integer> DUMMY_PROCESS_FUNCTION =
            new KeyedProcessFunction<Integer, Integer, Integer>() {
                @Override
                public void processElement(Integer value, Context ctx, Collector<Integer> out) {}
            };
    private static final KeyedCoProcessFunction<Integer, Integer, Integer, Integer>
            DUMMY_KEYED_CO_PROCESS_FUNCTION =
                    new KeyedCoProcessFunction<Integer, Integer, Integer, Integer>() {
                        @Override
                        public void processElement1(
                                Integer value, Context ctx, Collector<Integer> out) {}

                        @Override
                        public void processElement2(
                                Integer value, Context ctx, Collector<Integer> out) {}
                    };

    private static final class InputSelectableTwoInputOperator
            extends AbstractStreamOperator<Integer>
            implements TwoInputStreamOperator<Integer, Integer, Integer>, InputSelectable {
        @Override
        public InputSelection nextSelection() {
            return null;
        }

        @Override
        public void processElement1(StreamRecord<Integer> element) {}

        @Override
        public void processElement2(StreamRecord<Integer> element) {}
    }

    private static class MultipleInputOperator extends AbstractStreamOperatorV2<Integer>
            implements MultipleInputStreamOperator<Integer> {

        public MultipleInputOperator(
                StreamOperatorParameters<Integer> parameters, int inputsCount) {
            super(parameters, inputsCount);
        }

        @Override
        @SuppressWarnings({"rawtypes"})
        public List<Input> getInputs() {
            return Collections.emptyList();
        }
    }

    private static final class InputSelectableMultipleInputOperator extends MultipleInputOperator
            implements InputSelectable {
        public InputSelectableMultipleInputOperator(
                StreamOperatorParameters<Integer> parameters, int inputsCount) {
            super(parameters, inputsCount);
        }

        @Override
        public InputSelection nextSelection() {
            return null;
        }
    }

    private static final class MultipleInputOperatorFactory
            extends AbstractStreamOperatorFactory<Integer> {

        private final int inputsCount;
        private final boolean selectable;

        private MultipleInputOperatorFactory(int inputsCount, boolean selectable) {
            this.inputsCount = inputsCount;
            this.selectable = selectable;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Integer>> T createStreamOperator(
                StreamOperatorParameters<Integer> parameters) {
            if (selectable) {
                return (T) new InputSelectableMultipleInputOperator(parameters, inputsCount);
            } else {
                return (T) new MultipleInputOperator(parameters, inputsCount);
            }
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            if (selectable) {
                return InputSelectableMultipleInputOperator.class;
            } else {
                return MultipleInputOperator.class;
            }
        }
    }
}
