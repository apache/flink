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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for generating correct properties for sorting inputs in {@link RuntimeExecutionMode#BATCH}
 * runtime mode.
 */
class StreamGraphGeneratorBatchExecutionTest {

    @Test
    void testShuffleMode() {
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
    void testBatchJobType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSink<Integer> sink = addDummyPipeline(env);

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        assertThat(graph.getJobType()).isEqualTo(JobType.BATCH);
    }

    @Test
    void testManagedMemoryWeights() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);
        StreamNode processNode = graph.getStreamNode(process.getId());

        final Map<ManagedMemoryUseCase, Integer> expectedOperatorWeights = new HashMap<>();
        expectedOperatorWeights.put(
                ManagedMemoryUseCase.OPERATOR,
                ExecutionOptions.SORTED_INPUTS_MEMORY.defaultValue().getMebiBytes());
        assertThat(processNode.getManagedMemoryOperatorScopeUseCaseWeights())
                .isEqualTo(expectedOperatorWeights);
        assertThat(processNode.getManagedMemorySlotScopeUseCases())
                .containsOnly(ManagedMemoryUseCase.STATE_BACKEND);
    }

    @Test
    void testCustomManagedMemoryWeights() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORTED_INPUTS_MEMORY, MemorySize.ofMebiBytes(42));

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);
        StreamNode processNode = graph.getStreamNode(process.getId());

        final Map<ManagedMemoryUseCase, Integer> expectedOperatorWeights = new HashMap<>();
        expectedOperatorWeights.put(ManagedMemoryUseCase.OPERATOR, 42);
        assertThat(processNode.getManagedMemoryOperatorScopeUseCaseWeights())
                .isEqualTo(expectedOperatorWeights);
        assertThat(processNode.getManagedMemorySlotScopeUseCases())
                .containsOnly(ManagedMemoryUseCase.STATE_BACKEND);
    }

    @Test
    void testOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(graph.getStateBackend()).isInstanceOf(BatchExecutionStateBackend.class);
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider()).isNotNull();
    }

    @Test
    void testDisablingStateBackendOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(graph.getStateBackend()).isNull();
        assertThat(graph.getTimerServiceProvider()).isNull();
    }

    @Test
    void testDisablingSortingInputsOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        StreamGraph graph = getStreamGraphInBatchMode(sink, configuration);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0)).isNull();
        assertThat(graph.getStateBackend()).isNull();
        assertThat(graph.getTimerServiceProvider()).isNull();
    }

    @Test
    void testDisablingSortingInputsWithoutBatchStateBackendOneInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Integer> process =
                env.fromData(1, 2).keyBy(Integer::intValue).process(DUMMY_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        assertThatThrownBy(() -> getStreamGraphInBatchMode(sink, configuration))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Batch state backend requires the sorted inputs to be enabled!");
    }

    @Test
    void testTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        StreamGraph graph = getStreamGraphInBatchMode(sink);

        StreamNode processNode = graph.getStreamNode(process.getId());
        assertThat(processNode.getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getInputRequirements().get(1))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(graph.getStateBackend()).isInstanceOf(BatchExecutionStateBackend.class);
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider()).isNotNull();
    }

    @Test
    void testDisablingStateBackendTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
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
        assertThat(processNode.getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getInputRequirements().get(1))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(processNode.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(graph.getStateBackend()).isNull();
        assertThat(graph.getTimerServiceProvider()).isNull();
    }

    @Test
    void testDisablingSortingInputsTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
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
        assertThat(processNode.getInputRequirements().get(0)).isNull();
        assertThat(processNode.getInputRequirements().get(1)).isNull();
        assertThat(graph.getStateBackend()).isNull();
        assertThat(graph.getTimerServiceProvider()).isNull();
    }

    @Test
    void testDisablingSortingInputsWithoutBatchStateBackendTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
        SingleOutputStreamOperator<Integer> process =
                elements1
                        .connect(elements2)
                        .keyBy(Integer::intValue, Integer::intValue)
                        .process(DUMMY_KEYED_CO_PROCESS_FUNCTION);
        DataStreamSink<Integer> sink = process.sinkTo(new DiscardingSink<>());

        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.SORT_INPUTS, false);

        assertThatThrownBy(() -> getStreamGraphInBatchMode(sink, configuration))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Batch state backend requires the sorted inputs to be enabled!");
    }

    @Test
    void testInputSelectableTwoInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
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

        assertThatThrownBy(() -> getStreamGraphInBatchMode(sink))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Batch state backend and sorting inputs are not supported in graphs with an InputSelectable operator.");
    }

    @Test
    void testMultiInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
        DataStreamSource<Integer> elements3 = env.fromData(1, 2);

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
        assertThat(operatorNode.getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(operatorNode.getInputRequirements().get(1))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(operatorNode.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(graph.getStateBackend()).isInstanceOf(BatchExecutionStateBackend.class);
        // the provider is passed as a lambda therefore we cannot assert the class of the provider
        assertThat(graph.getTimerServiceProvider()).isNotNull();
    }

    @Test
    void testInputSelectableMultiInputTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> elements1 = env.fromData(1, 2);
        DataStreamSource<Integer> elements2 = env.fromData(1, 2);
        DataStreamSource<Integer> elements3 = env.fromData(1, 2);

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

        assertThatThrownBy(() -> getStreamGraphInBatchMode(sink))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Batch state backend and sorting inputs are not supported in graphs with an InputSelectable operator.");
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

        assertThatThrownBy(streamGraphGenerator::generate)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Iterations are not supported in BATCH execution mode.");
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

        assertThat(graph.getGlobalStreamExchangeMode()).isEqualTo(expectedStreamExchangeMode);
    }

    private DataStreamSink<Integer> addDummyPipeline(StreamExecutionEnvironment env) {
        return env.fromData(1, 2)
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
