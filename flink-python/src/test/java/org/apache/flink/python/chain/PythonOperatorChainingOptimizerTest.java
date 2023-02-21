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

package org.apache.flink.python.chain;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunction;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonKeyedCoProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonKeyedProcessOperator;
import org.apache.flink.streaming.api.operators.python.process.ExternalPythonProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link PythonOperatorChainingOptimizer}. */
class PythonOperatorChainingOptimizerTest {

    @Test
    void testChainedTransformationPropertiesCorrectlySet() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator =
                createProcessOperator("f2", Types.STRING(), Types.STRING());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> keyedProcessTransformation =
                new OneInputTransformation(
                        sourceTransformation,
                        "keyedProcess",
                        keyedProcessOperator,
                        keyedProcessOperator.getProducedType(),
                        2);
        keyedProcessTransformation.setUid("uid");
        keyedProcessTransformation.setSlotSharingGroup("group");
        keyedProcessTransformation.setCoLocationGroupKey("col");
        keyedProcessTransformation.setMaxParallelism(64);
        keyedProcessTransformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, 5);
        keyedProcessTransformation.declareManagedMemoryUseCaseAtSlotScope(
                ManagedMemoryUseCase.PYTHON);
        keyedProcessTransformation.declareManagedMemoryUseCaseAtSlotScope(
                ManagedMemoryUseCase.STATE_BACKEND);
        keyedProcessTransformation.setBufferTimeout(1000L);
        keyedProcessTransformation.setChainingStrategy(ChainingStrategy.HEAD);
        Transformation<?> processTransformation =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process",
                        processOperator,
                        processOperator.getProducedType(),
                        2);
        processTransformation.setSlotSharingGroup("group");
        processTransformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, 10);
        processTransformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);
        processTransformation.setMaxParallelism(64);
        processTransformation.setBufferTimeout(500L);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(keyedProcessTransformation);
        transformations.add(processTransformation);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(2);

        OneInputTransformation<?, ?> chainedTransformation =
                (OneInputTransformation<?, ?>) optimized.get(1);
        assertThat(chainedTransformation.getParallelism()).isEqualTo(2);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation.getInputType());
        assertThat(processOperator.getProducedType())
                .isEqualTo(chainedTransformation.getOutputType());
        assertThat(keyedProcessTransformation.getUid()).isEqualTo(chainedTransformation.getUid());
        assertThat(chainedTransformation.getSlotSharingGroup().get().getName()).isEqualTo("group");
        assertThat(chainedTransformation.getCoLocationGroupKey()).isEqualTo("col");
        assertThat(chainedTransformation.getMaxParallelism()).isEqualTo(64);
        assertThat(chainedTransformation.getBufferTimeout()).isEqualTo(500L);
        assertThat(
                        (int)
                                chainedTransformation
                                        .getManagedMemoryOperatorScopeUseCaseWeights()
                                        .getOrDefault(ManagedMemoryUseCase.OPERATOR, 0))
                .isEqualTo(15);
        assertThat(chainedTransformation.getOperatorFactory().getChainingStrategy())
                .isEqualTo(ChainingStrategy.HEAD);
        assertThat(chainedTransformation.getManagedMemorySlotScopeUseCases())
                .contains(ManagedMemoryUseCase.PYTHON);
        assertThat(chainedTransformation.getManagedMemorySlotScopeUseCases())
                .contains(ManagedMemoryUseCase.STATE_BACKEND);

        OneInputStreamOperator<?, ?> chainedOperator = chainedTransformation.getOperator();
        assertThat(chainedOperator).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator).getPythonFunctionInfo(),
                "f2",
                "f1");
    }

    @Test
    void testChainingMultipleOperators() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator("f2", Types.STRING(), Types.LONG());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f3", Types.LONG(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> keyedProcessTransformation =
                new OneInputTransformation(
                        sourceTransformation,
                        "keyedProcess",
                        keyedProcessOperator,
                        keyedProcessOperator.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(keyedProcessTransformation);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(2);

        OneInputTransformation<?, ?> chainedTransformation =
                (OneInputTransformation<?, ?>) optimized.get(1);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation.getInputType());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation.getOutputType());

        OneInputStreamOperator<?, ?> chainedOperator = chainedTransformation.getOperator();
        assertThat(chainedOperator).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator).getPythonFunctionInfo(),
                "f3",
                "f2",
                "f1");
    }

    @Test
    void testChainingNonKeyedOperators() {
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f2", Types.STRING(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> processTransformation1 =
                new OneInputTransformation(
                        sourceTransformation,
                        "Process1",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process2",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(2);

        OneInputTransformation<?, ?> chainedTransformation =
                (OneInputTransformation<?, ?>) optimized.get(1);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation.getInputType());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation.getOutputType());

        OneInputStreamOperator<?, ?> chainedOperator = chainedTransformation.getOperator();
        assertThat(chainedOperator).isInstanceOf(ExternalPythonProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonProcessOperator<?, ?>) chainedOperator).getPythonFunctionInfo(),
                "f2",
                "f1");
    }

    @Test
    void testContinuousKeyedOperators() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator1 =
                createKeyedProcessOperator(
                        "f1",
                        new RowTypeInfo(Types.INT(), Types.INT()),
                        new RowTypeInfo(Types.INT(), Types.INT()));
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator2 =
                createKeyedProcessOperator(
                        "f2", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> processTransformation1 =
                new OneInputTransformation(
                        sourceTransformation,
                        "KeyedProcess1",
                        keyedProcessOperator1,
                        keyedProcessOperator1.getProducedType(),
                        2);
        OneInputTransformation<?, ?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "KeyedProcess2",
                        keyedProcessOperator2,
                        keyedProcessOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(3);

        assertThat(optimized.get(1)).isEqualTo(processTransformation1);
        assertThat(optimized.get(2)).isEqualTo(processTransformation2);
    }

    @Test
    void testMultipleChainedOperators() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator1 =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator(
                        "f2", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator(
                        "f3", new RowTypeInfo(Types.INT(), Types.INT()), Types.LONG());

        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator2 =
                createKeyedProcessOperator(
                        "f4", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator3 =
                createProcessOperator(
                        "f5", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> keyedProcessTransformation1 =
                new OneInputTransformation(
                        sourceTransformation,
                        "keyedProcess",
                        keyedProcessOperator1,
                        keyedProcessOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedProcessTransformation1,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        OneInputTransformation<?, ?> keyedProcessTransformation2 =
                new OneInputTransformation(
                        processTransformation2,
                        "keyedProcess",
                        keyedProcessOperator2,
                        keyedProcessOperator2.getProducedType(),
                        2);
        Transformation<?> processTransformation3 =
                new OneInputTransformation(
                        keyedProcessTransformation2,
                        "process",
                        processOperator3,
                        processOperator3.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(keyedProcessTransformation1);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);
        transformations.add(keyedProcessTransformation2);
        transformations.add(processTransformation3);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(3);

        OneInputTransformation<?, ?> chainedTransformation1 =
                (OneInputTransformation<?, ?>) optimized.get(1);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation1.getInputType());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation1.getOutputType());

        OneInputTransformation<?, ?> chainedTransformation2 =
                (OneInputTransformation<?, ?>) optimized.get(2);
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation2.getInputType());
        assertThat(processOperator3.getProducedType())
                .isEqualTo(chainedTransformation2.getOutputType());

        OneInputStreamOperator<?, ?> chainedOperator1 = chainedTransformation1.getOperator();
        assertThat(chainedOperator1).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator1).getPythonFunctionInfo(),
                "f3",
                "f2",
                "f1");

        OneInputStreamOperator<?, ?> chainedOperator2 = chainedTransformation2.getOperator();
        assertThat(chainedOperator2).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator2).getPythonFunctionInfo(),
                "f5",
                "f4");
    }

    @Test
    void testChainingTwoInputOperators() {
        ExternalPythonKeyedCoProcessOperator<?> keyedCoProcessOperator1 =
                createCoKeyedProcessOperator(
                        "f1",
                        new RowTypeInfo(Types.INT(), Types.STRING()),
                        new RowTypeInfo(Types.INT(), Types.INT()),
                        Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator(
                        "f2", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator(
                        "f3", new RowTypeInfo(Types.INT(), Types.INT()), Types.LONG());

        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator2 =
                createKeyedProcessOperator(
                        "f4", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator3 =
                createProcessOperator(
                        "f5", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());

        Transformation<?> sourceTransformation1 = mock(SourceTransformation.class);
        Transformation<?> sourceTransformation2 = mock(SourceTransformation.class);
        TwoInputTransformation<?, ?, ?> keyedCoProcessTransformation =
                new TwoInputTransformation(
                        sourceTransformation1,
                        sourceTransformation2,
                        "keyedCoProcess",
                        keyedCoProcessOperator1,
                        keyedCoProcessOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedCoProcessTransformation,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        OneInputTransformation<?, ?> keyedProcessTransformation =
                new OneInputTransformation(
                        processTransformation2,
                        "keyedProcess",
                        keyedProcessOperator2,
                        keyedProcessOperator2.getProducedType(),
                        2);
        Transformation<?> processTransformation3 =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process",
                        processOperator3,
                        processOperator3.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation1);
        transformations.add(sourceTransformation2);
        transformations.add(keyedCoProcessTransformation);
        transformations.add(processTransformation1);
        transformations.add(processTransformation2);
        transformations.add(keyedProcessTransformation);
        transformations.add(processTransformation3);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(4);

        TwoInputTransformation<?, ?, ?> chainedTransformation1 =
                (TwoInputTransformation<?, ?, ?>) optimized.get(2);
        assertThat(sourceTransformation1.getOutputType())
                .isEqualTo(chainedTransformation1.getInputType1());
        assertThat(sourceTransformation2.getOutputType())
                .isEqualTo(chainedTransformation1.getInputType2());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation1.getOutputType());

        OneInputTransformation<?, ?> chainedTransformation2 =
                (OneInputTransformation<?, ?>) optimized.get(3);
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation2.getInputType());
        assertThat(processOperator3.getProducedType())
                .isEqualTo(chainedTransformation2.getOutputType());

        TwoInputStreamOperator<?, ?, ?> chainedOperator1 = chainedTransformation1.getOperator();
        assertThat(chainedOperator1).isInstanceOf(ExternalPythonKeyedCoProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedCoProcessOperator<?>) chainedOperator1)
                        .getPythonFunctionInfo(),
                "f3",
                "f2",
                "f1");

        OneInputStreamOperator<?, ?> chainedOperator2 = chainedTransformation2.getOperator();
        assertThat(chainedOperator2).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator2).getPythonFunctionInfo(),
                "f5",
                "f4");
    }

    @Test
    void testChainingUnorderedTransformations() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator("f2", Types.STRING(), Types.LONG());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f3", Types.LONG(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> keyedProcessTransformation =
                new OneInputTransformation(
                        sourceTransformation,
                        "keyedProcess",
                        keyedProcessOperator,
                        keyedProcessOperator.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceTransformation);
        transformations.add(processTransformation2);
        transformations.add(processTransformation1);
        transformations.add(keyedProcessTransformation);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(2);

        OneInputTransformation<?, ?> chainedTransformation =
                (OneInputTransformation<?, ?>) optimized.get(1);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation.getInputType());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation.getOutputType());

        OneInputStreamOperator<?, ?> chainedOperator = chainedTransformation.getOperator();
        assertThat(chainedOperator).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator).getPythonFunctionInfo(),
                "f3",
                "f2",
                "f1");
    }

    @Test
    void testSingleTransformation() {
        ExternalPythonKeyedProcessOperator<?> keyedProcessOperator =
                createKeyedProcessOperator(
                        "f1", new RowTypeInfo(Types.INT(), Types.INT()), Types.STRING());
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator("f2", Types.STRING(), Types.LONG());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f3", Types.LONG(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        OneInputTransformation<?, ?> keyedProcessTransformation =
                new OneInputTransformation(
                        sourceTransformation,
                        "keyedProcess",
                        keyedProcessOperator,
                        keyedProcessOperator.getProducedType(),
                        2);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        keyedProcessTransformation,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(processTransformation2);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        assertThat(optimized).hasSize(2);

        OneInputTransformation<?, ?> chainedTransformation =
                (OneInputTransformation<?, ?>) optimized.get(0);
        assertThat(sourceTransformation.getOutputType())
                .isEqualTo(chainedTransformation.getInputType());
        assertThat(processOperator2.getProducedType())
                .isEqualTo(chainedTransformation.getOutputType());

        OneInputStreamOperator<?, ?> chainedOperator = chainedTransformation.getOperator();
        assertThat(chainedOperator).isInstanceOf(ExternalPythonKeyedProcessOperator.class);
        validateChainedPythonFunctions(
                ((ExternalPythonKeyedProcessOperator<?>) chainedOperator).getPythonFunctionInfo(),
                "f3",
                "f2",
                "f1");
    }

    @Test
    void testTransformationWithMultipleOutputs() {
        ExternalPythonProcessOperator<?, ?> processOperator1 =
                createProcessOperator("f1", Types.STRING(), Types.LONG());
        ExternalPythonProcessOperator<?, ?> processOperator2 =
                createProcessOperator("f2", Types.STRING(), Types.LONG());
        ExternalPythonProcessOperator<?, ?> processOperator3 =
                createProcessOperator("f3", Types.LONG(), Types.INT());

        Transformation<?> sourceTransformation = mock(SourceTransformation.class);
        Transformation<?> processTransformation1 =
                new OneInputTransformation(
                        sourceTransformation,
                        "process",
                        processOperator1,
                        processOperator1.getProducedType(),
                        2);
        Transformation<?> processTransformation2 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator2,
                        processOperator2.getProducedType(),
                        2);
        Transformation<?> processTransformation3 =
                new OneInputTransformation(
                        processTransformation1,
                        "process",
                        processOperator3,
                        processOperator3.getProducedType(),
                        2);

        List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(processTransformation2);
        transformations.add(processTransformation3);

        List<Transformation<?>> optimized =
                PythonOperatorChainingOptimizer.optimize(transformations);
        // no chaining optimization occurred
        assertThat(optimized).hasSize(4);
    }

    // ----------------------- Utility Methods -----------------------

    private void validateChainedPythonFunctions(
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            String... expectedChainedPythonFunctions) {
        for (String expectedPythonFunction : expectedChainedPythonFunctions) {
            assertThat(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction())
                    .isEqualTo(expectedPythonFunction.getBytes());
            Object[] inputs = pythonFunctionInfo.getInputs();
            if (inputs.length > 0) {
                assertThat(inputs).hasSize(1);
                pythonFunctionInfo = (DataStreamPythonFunctionInfo) inputs[0];
            } else {
                pythonFunctionInfo = null;
            }
        }

        assertThat(pythonFunctionInfo).isNull();
    }

    private static <OUT> ExternalPythonKeyedProcessOperator<OUT> createKeyedProcessOperator(
            String functionContent,
            RowTypeInfo inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        return new ExternalPythonKeyedProcessOperator<>(
                new Configuration(),
                new DataStreamPythonFunctionInfo(
                        new DataStreamPythonFunction(functionContent.getBytes(), null), -1),
                inputTypeInfo,
                outputTypeInfo);
    }

    private static <OUT> ExternalPythonKeyedCoProcessOperator<OUT> createCoKeyedProcessOperator(
            String functionContent,
            RowTypeInfo inputTypeInfo1,
            RowTypeInfo inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo) {
        return new ExternalPythonKeyedCoProcessOperator(
                new Configuration(),
                new DataStreamPythonFunctionInfo(
                        new DataStreamPythonFunction(functionContent.getBytes(), null), -1),
                inputTypeInfo1,
                inputTypeInfo2,
                outputTypeInfo);
    }

    private static <IN, OUT> ExternalPythonProcessOperator<IN, OUT> createProcessOperator(
            String functionContent,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        return new ExternalPythonProcessOperator<>(
                new Configuration(),
                new DataStreamPythonFunctionInfo(
                        new DataStreamPythonFunction(functionContent.getBytes(), null), -1),
                inputTypeInfo,
                outputTypeInfo);
    }
}
