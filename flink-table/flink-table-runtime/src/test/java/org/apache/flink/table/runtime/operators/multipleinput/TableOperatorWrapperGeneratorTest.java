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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TableOperatorWrapperGenerator}. */
public class TableOperatorWrapperGeneratorTest extends MultipleInputTestBase {

    /**
     * Test for simple sub-graph in a multiple input node.
     *
     * <pre>
     *
     * source1  source2
     *   |        |
     *  agg1     agg2
     *    \      /
     *      join
     *
     * </pre>
     */
    @Test
    public void testSimple() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");
        OneInputTransformation<RowData, RowData> agg1 =
                createOneInputTransform(
                        source1,
                        "agg1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        agg1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        OneInputTransformation<RowData, RowData> agg2 =
                createOneInputTransform(
                        source2,
                        "agg2",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        agg2.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);
        TwoInputTransformation<RowData, RowData, RowData> join =
                createTwoInputTransform(
                        agg1,
                        agg2,
                        "join",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));
        join.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 3);

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2), join, new int[] {1, 0});
        generator.generate();

        TableOperatorWrapper<?> headWrapper1 = createWrapper(agg1, 1, 1.0 / 6);
        TableOperatorWrapper<?> headWrapper2 = createWrapper(agg2, 2, 2.0 / 6);
        TableOperatorWrapper<?> outputWrapper = createWrapper(join, 0, 3.0 / 6);
        outputWrapper.addInput(headWrapper1, 1);
        outputWrapper.addInput(headWrapper2, 2);
        assertEquals(
                Arrays.asList(
                        Pair.of(source1, new InputSpec(1, 1, headWrapper1, 1)),
                        Pair.of(source2, new InputSpec(2, 0, headWrapper2, 1))),
                generator.getInputTransformAndInputSpecPairs());
        assertEquals(outputWrapper, generator.getTailWrapper());
        assertEquals(6, generator.getManagedMemoryWeight());
        assertEquals(10, generator.getParallelism());
        assertEquals(-1, generator.getMaxParallelism());
        assertEquals(ResourceSpec.UNKNOWN, generator.getMinResources());
        assertEquals(ResourceSpec.UNKNOWN, generator.getPreferredResources());
    }

    /**
     * Test for complex sub-graph in a multiple input node.
     *
     * <pre>
     *
     *                  source1  source2
     *                    |        |
     *                  agg1     agg2
     *                     \    /
     * source4  source5    join1  source3
     *      \    /           \    /
     *      join3            join2
     *          \             /
     *               join4
     *
     * </pre>
     */
    @Test
    public void testComplex() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");
        Transformation<RowData> source3 = createSource(env, "source3");
        Transformation<RowData> source4 = createSource(env, "source4");
        Transformation<RowData> source5 = createSource(env, "source5");

        OneInputTransformation<RowData, RowData> agg1 =
                createOneInputTransform(
                        source1,
                        "agg1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        agg1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);

        OneInputTransformation<RowData, RowData> agg2 =
                createOneInputTransform(
                        source2,
                        "agg2",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        agg2.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);

        TwoInputTransformation<RowData, RowData, RowData> join1 =
                createTwoInputTransform(
                        agg1,
                        agg2,
                        "join1",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));
        join1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 3);

        TwoInputTransformation<RowData, RowData, RowData> join2 =
                createTwoInputTransform(
                        join1,
                        source3,
                        "join2",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));
        join2.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 4);

        TwoInputTransformation<RowData, RowData, RowData> join3 =
                createTwoInputTransform(
                        source4,
                        source5,
                        "join3",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));
        join3.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 5);

        TwoInputTransformation<RowData, RowData, RowData> join4 =
                createTwoInputTransform(
                        join2,
                        join3,
                        "join4",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));
        join4.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 6);

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2, source3, source4, source5),
                        join4,
                        new int[] {2, 3, 4, 0, 1});
        generator.generate();

        TableOperatorWrapper<?> aggWrapper1 = createWrapper(agg1, 3, 1.0 / 21);
        TableOperatorWrapper<?> aggWrapper2 = createWrapper(agg2, 4, 2.0 / 21);
        TableOperatorWrapper<?> joinWrapper1 = createWrapper(join1, 2, 3.0 / 21);
        joinWrapper1.addInput(aggWrapper1, 1);
        joinWrapper1.addInput(aggWrapper2, 2);
        TableOperatorWrapper<?> joinWrapper2 = createWrapper(join2, 1, 4.0 / 21);
        joinWrapper2.addInput(joinWrapper1, 1);
        TableOperatorWrapper<?> joinWrapper3 = createWrapper(join3, 5, 5.0 / 21);

        TableOperatorWrapper<?> outputWrapper = createWrapper(join4, 0, 6.0 / 21);
        outputWrapper.addInput(joinWrapper2, 1);
        outputWrapper.addInput(joinWrapper3, 2);

        assertEquals(
                Arrays.asList(
                        Pair.of(source1, new InputSpec(1, 2, aggWrapper1, 1)),
                        Pair.of(source2, new InputSpec(2, 3, aggWrapper2, 1)),
                        Pair.of(source3, new InputSpec(3, 4, joinWrapper2, 2)),
                        Pair.of(source4, new InputSpec(4, 0, joinWrapper3, 1)),
                        Pair.of(source5, new InputSpec(5, 1, joinWrapper3, 2))),
                generator.getInputTransformAndInputSpecPairs());

        assertEquals(outputWrapper, generator.getTailWrapper());
        assertEquals(21, generator.getManagedMemoryWeight());
        assertEquals(10, generator.getParallelism());
        assertEquals(-1, generator.getMaxParallelism());
        assertEquals(ResourceSpec.UNKNOWN, generator.getMinResources());
        assertEquals(ResourceSpec.UNKNOWN, generator.getPreferredResources());
    }

    /**
     * Test for union nodes in a multiple input node.
     *
     * <pre>
     *
     *          source1  source2
     *               \    /
     *     source4   union1  source3
     *         \         \    /
     *         agg1      union2
     *            \      /
     * source5     join1
     *      \      /
     *       union3
     *
     * </pre>
     */
    @Test
    public void testWithUnion() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");
        Transformation<RowData> source3 = createSource(env, "source3");
        Transformation<RowData> source4 = createSource(env, "source4");
        Transformation<RowData> source5 = createSource(env, "source5");

        UnionTransformation<RowData> union1 = createUnionInputTransform("union1", source1, source2);
        UnionTransformation<RowData> union2 = createUnionInputTransform("union2", union1, source3);

        OneInputTransformation<RowData, RowData> agg1 =
                createOneInputTransform(
                        source4,
                        "agg1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        agg1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);

        TwoInputTransformation<RowData, RowData, RowData> join1 =
                createTwoInputTransform(
                        agg1,
                        union2,
                        "join1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        join1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);

        UnionTransformation<RowData> union3 = createUnionInputTransform("union3", source5, join1);

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2, source3, source4, source5),
                        union3,
                        new int[] {1, 1, 1, 0, 2});
        generator.generate();

        TableOperatorWrapper<?> unionWrapper1 = createWrapper(union1, 4);
        TableOperatorWrapper<?> unionWrapper2 = createWrapper(union2, 3);
        unionWrapper2.addInput(unionWrapper1, 1);
        TableOperatorWrapper<?> aggWrapper1 = createWrapper(agg1, 2, 1.0 / 3);
        TableOperatorWrapper<?> joinWrapper1 = createWrapper(join1, 1, 2.0 / 3);
        joinWrapper1.addInput(aggWrapper1, 1);
        joinWrapper1.addInput(unionWrapper2, 2);

        TableOperatorWrapper<?> outputWrapper = createWrapper(union3, 0);
        outputWrapper.addInput(joinWrapper1, 1);

        assertEquals(
                Arrays.asList(
                        Pair.of(source5, new InputSpec(1, 2, outputWrapper, 1)),
                        Pair.of(source4, new InputSpec(2, 0, aggWrapper1, 1)),
                        Pair.of(source1, new InputSpec(3, 1, unionWrapper1, 1)),
                        Pair.of(source2, new InputSpec(4, 1, unionWrapper1, 1)),
                        Pair.of(source3, new InputSpec(5, 1, unionWrapper2, 1))),
                generator.getInputTransformAndInputSpecPairs());

        assertEquals(outputWrapper, generator.getTailWrapper());
        assertEquals(3, generator.getManagedMemoryWeight());
        assertEquals(10, generator.getParallelism());
        assertEquals(-1, generator.getMaxParallelism());
        assertEquals(ResourceSpec.UNKNOWN, generator.getMinResources());
        assertEquals(ResourceSpec.UNKNOWN, generator.getPreferredResources());
    }

    /**
     * Test for nodes with different parallelisms in a multiple input node.
     *
     * <pre>
     *
     *       source1  source2
     *         |        |
     *        calc1   calc2
     *           \    /
     * source3   union
     *      \      /
     *        join
     *
     * </pre>
     */
    @Test
    public void testDifferentParallelisms() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");
        Transformation<RowData> source3 = createSource(env, "source3");

        OneInputTransformation<RowData, RowData> calc1 =
                createOneInputTransform(
                        source1,
                        "calc1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        calc1.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        calc1.setParallelism(100);

        OneInputTransformation<RowData, RowData> calc2 =
                createOneInputTransform(
                        source2,
                        "calc2",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        calc2.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        calc2.setParallelism(50);

        UnionTransformation<RowData> union = createUnionInputTransform("union1", calc1, calc2);

        TwoInputTransformation<RowData, RowData, RowData> join =
                createTwoInputTransform(
                        union,
                        source3,
                        "join1",
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        join.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        join.setParallelism(200);

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2, source3), join, new int[] {1, 1, 0});
        generator.generate();

        TableOperatorWrapper<?> calcWrapper1 = createWrapper(calc1, 2, 1.0 / 3);
        TableOperatorWrapper<?> calcWrapper2 = createWrapper(calc2, 3, 1.0 / 3);
        TableOperatorWrapper<?> unionWrapper = createWrapper(union, 1);
        unionWrapper.addInput(calcWrapper1, 1);
        unionWrapper.addInput(calcWrapper2, 2);
        TableOperatorWrapper<?> outputWrapper = createWrapper(join, 0, 1.0 / 3);
        outputWrapper.addInput(unionWrapper, 2);

        assertEquals(
                Arrays.asList(
                        Pair.of(source1, new InputSpec(1, 1, calcWrapper1, 1)),
                        Pair.of(source2, new InputSpec(2, 1, calcWrapper2, 1)),
                        Pair.of(source3, new InputSpec(3, 0, outputWrapper, 2))),
                generator.getInputTransformAndInputSpecPairs());
        assertEquals(
                Arrays.asList(
                        new TableOperatorWrapper.Edge(calcWrapper1, unionWrapper, 1),
                        new TableOperatorWrapper.Edge(calcWrapper2, unionWrapper, 2)),
                unionWrapper.getInputEdges());
        assertEquals(
                Collections.singletonList(
                        new TableOperatorWrapper.Edge(unionWrapper, outputWrapper, 2)),
                outputWrapper.getInputEdges());

        assertEquals(outputWrapper, generator.getTailWrapper());
        assertEquals(3, generator.getManagedMemoryWeight());
        assertEquals(200, generator.getParallelism());
        assertEquals(-1, generator.getMaxParallelism());
        assertEquals(ResourceSpec.UNKNOWN, generator.getMinResources());
        assertEquals(ResourceSpec.UNKNOWN, generator.getPreferredResources());
    }

    @Test(expected = RuntimeException.class)
    public void testUnsupportedTransformation() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");

        TestingTransformation<RowData> test = new TestingTransformation<>(source1, "test", 10);

        TwoInputTransformation<RowData, RowData, RowData> join =
                createTwoInputTransform(
                        test,
                        source2,
                        "join1",
                        InternalTypeInfo.of(
                                RowType.of(
                                        DataTypes.STRING().getLogicalType(),
                                        DataTypes.STRING().getLogicalType())));

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2), join, new int[] {0, 0});
        generator.generate();
    }

    @SafeVarargs
    private final UnionTransformation<RowData> createUnionInputTransform(
            String name, Transformation<RowData>... input) {
        UnionTransformation<RowData> transform = new UnionTransformation<>(Arrays.asList(input));
        transform.setName(name);
        return transform;
    }

    private TableOperatorWrapper<StreamOperator<RowData>> createWrapper(
            OneInputTransformation<RowData, RowData> transform,
            int index,
            double managedMemoryFraction) {
        TableOperatorWrapper<StreamOperator<RowData>> wrapper =
                new TableOperatorWrapper<>(
                        transform.getOperatorFactory(),
                        "SubOp" + index + "_" + transform.getName(),
                        Collections.singletonList(transform.getInputType()),
                        transform.getOutputType());
        wrapper.setManagedMemoryFraction(managedMemoryFraction);
        return wrapper;
    }

    private TableOperatorWrapper<StreamOperator<RowData>> createWrapper(
            TwoInputTransformation<RowData, RowData, RowData> transform,
            int index,
            double managedMemoryFraction) {
        TableOperatorWrapper<StreamOperator<RowData>> wrapper =
                new TableOperatorWrapper<>(
                        transform.getOperatorFactory(),
                        "SubOp" + index + "_" + transform.getName(),
                        Arrays.asList(transform.getInputType1(), transform.getInputType2()),
                        transform.getOutputType());
        wrapper.setManagedMemoryFraction(managedMemoryFraction);
        return wrapper;
    }

    private TableOperatorWrapper<StreamOperator<RowData>> createWrapper(
            UnionTransformation<RowData> transform, int index) {
        TableOperatorWrapper<StreamOperator<RowData>> wrapper =
                new TableOperatorWrapper<>(
                        SimpleOperatorFactory.of(new UnionStreamOperator()),
                        "SubOp" + index + "_" + transform.getName(),
                        transform.getInputs().stream()
                                .map(Transformation::getOutputType)
                                .collect(Collectors.toList()),
                        transform.getOutputType());
        wrapper.setManagedMemoryFraction(0D);
        return wrapper;
    }

    /** A test implementation of {@link Transformation}. */
    private static class TestingTransformation<T> extends Transformation<T> {
        private final Transformation<T> input;

        public TestingTransformation(Transformation<T> input, String name, int parallelism) {
            super(name, input.getOutputType(), parallelism);
            this.input = input;
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return Collections.emptyList();
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.singletonList(input);
        }
    }
}
