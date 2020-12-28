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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.operators.multipleinput.input.OneInput;
import org.apache.flink.table.runtime.operators.multipleinput.input.SecondInputOfTwoInput;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for {@link BatchMultipleInputStreamOperator}. */
public class BatchMultipleInputStreamOperatorTest extends MultipleInputTestBase {

    @Test
    public void testOpen() throws Exception {
        TestingBatchMultipleInputStreamOperator op = createMultipleInputStreamOperator();
        TestingTwoInputStreamOperator joinOp2 =
                (TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

        TableOperatorWrapper<?> joinWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
        TestingTwoInputStreamOperator joinOp1 =
                (TestingTwoInputStreamOperator) joinWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper1 = joinWrapper1.getInputWrappers().get(0);
        TestingOneInputStreamOperator aggOp1 =
                (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper2 = joinWrapper1.getInputWrappers().get(1);
        TestingOneInputStreamOperator aggOp2 =
                (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

        assertFalse(aggOp1.isOpened());
        assertFalse(aggOp2.isOpened());
        assertFalse(aggOp1.isOpened());
        assertFalse(joinOp2.isOpened());

        op.open();

        assertTrue(aggOp1.isOpened());
        assertTrue(aggOp2.isOpened());
        assertTrue(joinOp1.isOpened());
        assertTrue(joinOp2.isOpened());
    }

    @Test
    public void testNextSelectionAndEndInput() throws Exception {
        TestingBatchMultipleInputStreamOperator op = createMultipleInputStreamOperator();
        TestingTwoInputStreamOperator joinOp2 =
                (TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

        TableOperatorWrapper<?> joinWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
        TestingTwoInputStreamOperator joinOp1 =
                (TestingTwoInputStreamOperator) joinWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper1 = joinWrapper1.getInputWrappers().get(0);
        TestingOneInputStreamOperator aggOp1 =
                (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper2 = joinWrapper1.getInputWrappers().get(1);
        TestingOneInputStreamOperator aggOp2 =
                (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

        assertFalse(aggOp1.isEnd());
        assertFalse(aggOp2.isEnd());
        assertTrue(joinOp1.getEndInputs().isEmpty());
        assertTrue(joinOp2.getEndInputs().isEmpty());
        assertEquals(new InputSelection.Builder().select(3).build(3), op.nextSelection());

        op.endInput(3);
        assertFalse(aggOp1.isEnd());
        assertFalse(aggOp2.isEnd());
        assertTrue(joinOp1.getEndInputs().isEmpty());
        assertEquals(Collections.singletonList(2), joinOp2.getEndInputs());
        assertEquals(new InputSelection.Builder().select(1).build(3), op.nextSelection());

        op.endInput(1);
        assertTrue(aggOp1.isEnd());
        assertFalse(aggOp2.isEnd());
        assertEquals(Collections.singletonList(1), joinOp1.getEndInputs());
        assertEquals(Collections.singletonList(2), joinOp2.getEndInputs());
        assertEquals(new InputSelection.Builder().select(2).build(3), op.nextSelection());

        op.endInput(2);
        assertTrue(aggOp1.isEnd());
        assertTrue(aggOp2.isEnd());
        assertEquals(Arrays.asList(1, 2), joinOp1.getEndInputs());
        assertEquals(Arrays.asList(2, 1), joinOp2.getEndInputs());
        assertEquals(InputSelection.ALL, op.nextSelection());
    }

    @Test
    public void testDispose() throws Exception {
        TestingBatchMultipleInputStreamOperator op = createMultipleInputStreamOperator();
        TestingTwoInputStreamOperator joinOp2 =
                (TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

        TableOperatorWrapper<?> joinWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
        TestingTwoInputStreamOperator joinOp1 =
                (TestingTwoInputStreamOperator) joinWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper1 = joinWrapper1.getInputWrappers().get(0);
        TestingOneInputStreamOperator aggOp1 =
                (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper2 = joinWrapper1.getInputWrappers().get(1);
        TestingOneInputStreamOperator aggOp2 =
                (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

        assertFalse(aggOp1.isDisposed());
        assertFalse(aggOp2.isDisposed());
        assertFalse(aggOp1.isDisposed());
        assertFalse(joinOp2.isDisposed());

        op.dispose();

        assertTrue(aggOp1.isDisposed());
        assertTrue(aggOp2.isDisposed());
        assertTrue(joinOp1.isDisposed());
        assertTrue(joinOp2.isDisposed());
    }

    @Test
    public void testClose() throws Exception {
        TestingBatchMultipleInputStreamOperator op = createMultipleInputStreamOperator();
        TestingTwoInputStreamOperator joinOp2 =
                (TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

        TableOperatorWrapper<?> joinWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
        TestingTwoInputStreamOperator joinOp1 =
                (TestingTwoInputStreamOperator) joinWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper1 = joinWrapper1.getInputWrappers().get(0);
        TestingOneInputStreamOperator aggOp1 =
                (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper2 = joinWrapper1.getInputWrappers().get(1);
        TestingOneInputStreamOperator aggOp2 =
                (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

        assertFalse(aggOp1.isClosed());
        assertFalse(aggOp2.isClosed());
        assertFalse(aggOp1.isClosed());
        assertFalse(joinOp2.isClosed());

        op.close();

        assertTrue(aggOp1.isClosed());
        assertTrue(aggOp2.isClosed());
        assertTrue(joinOp1.isClosed());
        assertTrue(joinOp2.isClosed());
    }

    @Test
    public void testProcess() throws Exception {
        TestingBatchMultipleInputStreamOperator op = createMultipleInputStreamOperator();
        List<StreamElement> outputData = op.getOutputData();

        TestingTwoInputStreamOperator joinOp2 =
                (TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

        TableOperatorWrapper<?> joinWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
        TestingTwoInputStreamOperator joinOp1 =
                (TestingTwoInputStreamOperator) joinWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper1 = joinWrapper1.getInputWrappers().get(0);
        TestingOneInputStreamOperator aggOp1 =
                (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

        TableOperatorWrapper<?> aggWrapper2 = joinWrapper1.getInputWrappers().get(1);
        TestingOneInputStreamOperator aggOp2 =
                (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

        List<Input> inputs = op.getInputs();
        assertEquals(3, inputs.size());
        Input input1 = inputs.get(0);
        Input input2 = inputs.get(1);
        Input input3 = inputs.get(2);

        assertTrue(input1 instanceof OneInput);
        assertTrue(input2 instanceof OneInput);
        assertTrue(input3 instanceof SecondInputOfTwoInput);

        assertNull(joinOp2.getCurrentElement1());
        assertNull(joinOp2.getCurrentElement2());

        assertNull(joinOp1.getCurrentElement1());
        assertNull(joinOp1.getCurrentElement2());

        assertNull(aggOp1.getCurrentElement());
        assertNull(aggOp2.getCurrentElement());
        assertTrue(outputData.isEmpty());

        // process first input (input id is 3)
        StreamRecord<RowData> element1 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
        input3.processElement(element1);

        assertEquals(element1, joinOp2.getCurrentElement2());
        assertNull(joinOp2.getCurrentElement1());
        assertTrue(outputData.isEmpty());

        // finish first input
        assertTrue(joinOp2.getEndInputs().isEmpty());
        op.endInput(3);
        assertTrue(outputData.isEmpty());
        assertEquals(Collections.singletonList(2), joinOp2.getEndInputs());

        // process second input (input id is 1)
        StreamRecord<RowData> element2 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("124")), 457);
        input1.processElement(element2);

        assertEquals(element2, aggOp1.getCurrentElement());
        assertNull(joinOp1.getCurrentElement1());
        assertNull(joinOp2.getCurrentElement1());
        assertTrue(outputData.isEmpty());

        // finish second input
        assertTrue(joinOp1.getEndInputs().isEmpty());
        op.endInput(1);
        assertEquals(Collections.singletonList(1), joinOp1.getEndInputs());
        assertEquals(Collections.singletonList(2), joinOp2.getEndInputs());
        assertEquals(element2, joinOp1.getCurrentElement1());
        assertTrue(outputData.isEmpty());

        // process third input (input id is 2)
        StreamRecord<RowData> element3 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("125")), 458);
        input2.processElement(element3);

        assertEquals(element3, aggOp2.getCurrentElement());
        assertNull(joinOp1.getCurrentElement2());
        assertNull(joinOp2.getCurrentElement1());
        assertTrue(outputData.isEmpty());

        // finish third input
        assertEquals(Collections.singletonList(1), joinOp1.getEndInputs());
        op.endInput(2);
        assertEquals(Arrays.asList(1, 2), joinOp1.getEndInputs());
        assertEquals(Arrays.asList(2, 1), joinOp2.getEndInputs());
        assertEquals(element3, joinOp1.getCurrentElement2());
        assertEquals(3, outputData.size());
    }

    /**
     * Create a BatchMultipleInputStreamOperator which contains the following sub-graph.
     *
     * <pre>
     *
     * source1  source2
     *   |        |
     *  agg1     agg2
     * (b) \     / (p)
     *      join1   source3
     *     (p) \     / (b)
     *         join2
     * (b) is the build side of the join, (p) is the probe side of the join.
     * </pre>
     */
    private TestingBatchMultipleInputStreamOperator createMultipleInputStreamOperator()
            throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Transformation<RowData> source1 = createSource(env, "source1");
        Transformation<RowData> source2 = createSource(env, "source2");
        Transformation<RowData> source3 = createSource(env, "source3");
        OneInputTransformation<RowData, RowData> agg1 =
                createOneInputTransform(
                        source1,
                        "agg1",
                        new TestingOneInputStreamOperator(true),
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        OneInputTransformation<RowData, RowData> agg2 =
                createOneInputTransform(
                        source2,
                        "agg2",
                        new TestingOneInputStreamOperator(true),
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
        TwoInputTransformation<RowData, RowData, RowData> join1 =
                createTwoInputTransform(
                        agg1,
                        agg2,
                        "join1",
                        new TestingTwoInputStreamOperator(true),
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));

        TwoInputTransformation<RowData, RowData, RowData> join2 =
                createTwoInputTransform(
                        join1,
                        source3,
                        "join2",
                        new TestingTwoInputStreamOperator(true),
                        InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));

        TableOperatorWrapperGenerator generator =
                new TableOperatorWrapperGenerator(
                        Arrays.asList(source1, source2, source3), join2, new int[] {1, 2, 0});
        generator.generate();

        List<Pair<Transformation<?>, InputSpec>> inputTransformAndInputSpecPairs =
                generator.getInputTransformAndInputSpecPairs();

        List<StreamElement> outputData = new ArrayList<>();
        return new TestingBatchMultipleInputStreamOperator(
                createStreamOperatorParameters(new TestingOutput(outputData)),
                inputTransformAndInputSpecPairs.stream()
                        .map(Pair::getValue)
                        .collect(Collectors.toList()),
                generator.getHeadWrappers(),
                generator.getTailWrapper(),
                outputData);
    }

    /** A sub class of {@link BatchMultipleInputStreamOperator} for testing. */
    private static class TestingBatchMultipleInputStreamOperator
            extends BatchMultipleInputStreamOperator {
        private final TableOperatorWrapper<?> tailWrapper;
        private final List<StreamElement> outputData;

        public TestingBatchMultipleInputStreamOperator(
                StreamOperatorParameters<RowData> parameters,
                List<InputSpec> inputSpecs,
                List<TableOperatorWrapper<?>> headWrapper,
                TableOperatorWrapper<?> tailWrapper,
                List<StreamElement> outputData) {
            super(parameters, inputSpecs, headWrapper, tailWrapper);
            this.tailWrapper = tailWrapper;
            this.outputData = outputData;
        }

        public List<StreamElement> getOutputData() {
            return outputData;
        }

        public TableOperatorWrapper<?> getTailWrapper() {
            return tailWrapper;
        }
    }

    /**
     * A sub class of {@link CollectorOutput} for testing. Notes: the data to collect will not be
     * copied, object reuse should be disabled when using this class.
     */
    private static class TestingOutput extends CollectorOutput<RowData> {
        private final List<StreamElement> list;

        public TestingOutput(List<StreamElement> list) {
            super(list);
            this.list = list;
        }

        @Override
        public void collect(StreamRecord<RowData> record) {
            list.add(record);
        }
    }
}
