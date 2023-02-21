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
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(aggOp1.isOpened()).isFalse();
        assertThat(aggOp2.isOpened()).isFalse();
        assertThat(aggOp1.isOpened()).isFalse();
        assertThat(joinOp2.isOpened()).isFalse();

        op.open();

        assertThat(aggOp1.isOpened()).isTrue();
        assertThat(aggOp2.isOpened()).isTrue();
        assertThat(joinOp1.isOpened()).isTrue();
        assertThat(joinOp2.isOpened()).isTrue();
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

        assertThat(aggOp1.isEnd()).isFalse();
        assertThat(aggOp2.isEnd()).isFalse();
        assertThat(joinOp1.getEndInputs()).isEmpty();
        assertThat(joinOp2.getEndInputs()).isEmpty();
        assertThat(op.nextSelection()).isEqualTo(new InputSelection.Builder().select(3).build(3));

        op.endInput(3);
        assertThat(aggOp1.isEnd()).isFalse();
        assertThat(aggOp2.isEnd()).isFalse();
        assertThat(joinOp1.getEndInputs()).isEmpty();
        assertThat(joinOp2.getEndInputs()).containsExactly(2);
        assertThat(op.nextSelection()).isEqualTo(new InputSelection.Builder().select(1).build(3));

        op.endInput(1);
        assertThat(aggOp1.isEnd()).isTrue();
        assertThat(aggOp2.isEnd()).isFalse();
        assertThat(joinOp1.getEndInputs()).containsExactly(1);
        assertThat(joinOp2.getEndInputs()).containsExactly(2);
        assertThat(op.nextSelection()).isEqualTo(new InputSelection.Builder().select(2).build(3));

        op.endInput(2);
        assertThat(aggOp1.isEnd()).isTrue();
        assertThat(aggOp2.isEnd()).isTrue();
        assertThat(joinOp1.getEndInputs()).isEqualTo(Arrays.asList(1, 2));
        assertThat(joinOp2.getEndInputs()).isEqualTo(Arrays.asList(2, 1));
        assertThat(op.nextSelection()).isEqualTo(InputSelection.ALL);
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

        assertThat(aggOp1.isClosed()).isFalse();
        assertThat(aggOp2.isClosed()).isFalse();
        assertThat(aggOp1.isClosed()).isFalse();
        assertThat(joinOp2.isClosed()).isFalse();

        op.close();

        assertThat(aggOp1.isClosed()).isTrue();
        assertThat(aggOp2.isClosed()).isTrue();
        assertThat(joinOp1.isClosed()).isTrue();
        assertThat(joinOp2.isClosed()).isTrue();
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
        assertThat(inputs).hasSize(3);
        Input input1 = inputs.get(0);
        Input input2 = inputs.get(1);
        Input input3 = inputs.get(2);

        assertThat(input1).isInstanceOf(OneInput.class);
        assertThat(input2).isInstanceOf(OneInput.class);
        assertThat(input3).isInstanceOf(SecondInputOfTwoInput.class);

        assertThat(joinOp2.getCurrentElement1()).isNull();
        assertThat(joinOp2.getCurrentElement2()).isNull();

        assertThat(joinOp1.getCurrentElement1()).isNull();
        assertThat(joinOp1.getCurrentElement2()).isNull();

        assertThat(aggOp1.getCurrentElement()).isNull();
        assertThat(aggOp2.getCurrentElement()).isNull();
        assertThat(outputData).isEmpty();

        // process first input (input id is 3)
        StreamRecord<RowData> element1 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("123")), 456);
        input3.processElement(element1);

        assertThat(joinOp2.getCurrentElement2()).isEqualTo(element1);
        assertThat(joinOp2.getCurrentElement1()).isNull();
        assertThat(outputData).isEmpty();

        // finish first input
        assertThat(joinOp2.getEndInputs()).isEmpty();
        op.endInput(3);
        assertThat(outputData).isEmpty();
        assertThat(joinOp2.getEndInputs()).containsExactly(2);

        // process second input (input id is 1)
        StreamRecord<RowData> element2 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("124")), 457);
        input1.processElement(element2);

        assertThat(aggOp1.getCurrentElement()).isEqualTo(element2);
        assertThat(joinOp1.getCurrentElement1()).isNull();
        assertThat(joinOp2.getCurrentElement1()).isNull();
        assertThat(outputData).isEmpty();

        // finish second input
        assertThat(joinOp1.getEndInputs()).isEmpty();
        op.endInput(1);
        assertThat(joinOp1.getEndInputs()).containsExactly(1);
        assertThat(joinOp2.getEndInputs()).containsExactly(2);
        assertThat(joinOp1.getCurrentElement1()).isEqualTo(element2);
        assertThat(outputData).isEmpty();

        // process third input (input id is 2)
        StreamRecord<RowData> element3 =
                new StreamRecord<>(GenericRowData.of(StringData.fromString("125")), 458);
        input2.processElement(element3);

        assertThat(aggOp2.getCurrentElement()).isEqualTo(element3);
        assertThat(joinOp1.getCurrentElement2()).isNull();
        assertThat(joinOp2.getCurrentElement1()).isNull();
        assertThat(outputData).isEmpty();

        // finish third input
        assertThat(joinOp1.getEndInputs()).containsExactly(1);
        op.endInput(2);
        assertThat(joinOp1.getEndInputs()).isEqualTo(Arrays.asList(1, 2));
        assertThat(joinOp2.getEndInputs()).isEqualTo(Arrays.asList(2, 1));
        assertThat(joinOp1.getCurrentElement2()).isEqualTo(element3);
        assertThat(outputData).hasSize(3);
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
