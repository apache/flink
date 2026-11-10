/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.InputRef;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInput;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.utils.PassThroughPythonAggregateFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link BatchArrowPythonOverWindowAggregateFunctionOperator}. These test that:
 *
 * <ul>
 *   <li>FinishBundle is called when bundled element count reach to max bundle size
 *   <li>FinishBundle is called when bundled time reach to max bundle time
 * </ul>
 */
class BatchArrowPythonOverWindowAggregateFunctionOperatorTest
        extends AbstractBatchArrowPythonAggregateFunctionOperatorTest {

    @Test
    void testOverWindowAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));

        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.set(PythonOptions.MAX_BUNDLE_SIZE, 3);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredByTime() throws Exception {
        Configuration conf = new Configuration();
        conf.set(PythonOptions.MAX_BUNDLE_SIZE, 10);
        conf.set(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 0L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1000L);
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 0L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 2L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L, 3L)));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testUserDefinedFunctionsProto() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        testHarness.open();
        BatchArrowPythonOverWindowAggregateFunctionOperator operator =
                (BatchArrowPythonOverWindowAggregateFunctionOperator)
                        testHarness.getOneInputOperator();
        FlinkFnApi.UserDefinedFunctions functionsProto = operator.createUserDefinedFunctionsProto();
        List<FlinkFnApi.OverWindow> windows = functionsProto.getWindowsList();
        assertThat(windows).hasSize(2);

        // first window is a range sliding window.
        FlinkFnApi.OverWindow firstWindow = windows.get(0);
        assertThat(firstWindow.getWindowType())
                .isEqualTo(FlinkFnApi.OverWindow.WindowType.RANGE_SLIDING);

        // second window is a row unbounded preceding window.
        FlinkFnApi.OverWindow secondWindow = windows.get(1);
        assertThat(secondWindow.getWindowType())
                .isEqualTo(FlinkFnApi.OverWindow.WindowType.ROW_UNBOUNDED_PRECEDING);
        assertThat(secondWindow.getUpperBoundary()).isEqualTo(2L);
    }

    @Test
    void testRangeWindowOnIntOrderKey() throws Exception {
        // RANGE BETWEEN 1 PRECEDING AND CURRENT ROW, ordered by an INT column with negative values.
        RowType inputType =
                RowType.of(new VarCharType(), new VarCharType(), new BigIntType(), new IntType());
        RowType outputType =
                RowType.of(
                        new VarCharType(),
                        new VarCharType(),
                        new BigIntType(),
                        new IntType(),
                        new BigIntType());
        // Same contract as the generated "1 PRECEDING" comparator: 1 once the row reaches key - 1.
        GeneratedRecordComparator lowerBoundComparator =
                comparator(
                        (row, current) -> (long) row.getInt(3) - current.getInt(3) >= -1L ? 1 : -1);
        GeneratedRecordComparator upperBoundComparator =
                comparator((row, current) -> Integer.compare(row.getInt(3), current.getInt(3)));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(
                        createTestOperator(
                                new Configuration(),
                                new PythonFunctionInfo[] {
                                    new PythonFunctionInfo(
                                            PythonScalarFunctionOperatorTestBase.DummyPythonFunction
                                                    .INSTANCE,
                                            new PythonFunctionInput[] {new InputRef(0)})
                                },
                                inputType,
                                outputType,
                                new int[] {0},
                                new int[] {2},
                                new long[] {0L},
                                new long[] {0L},
                                new boolean[] {true},
                                new GeneratedRecordComparator[] {lowerBoundComparator},
                                new GeneratedRecordComparator[] {upperBoundComparator}),
                        outputType);

        testHarness.open();
        testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, -2)));
        testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, -1)));
        testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 0)));
        testHarness.processElement(new StreamRecord<>(newBinaryRow(true, "c1", "c8", 3L, 3)));
        testHarness.close();

        // The pass-through runner returns f2 of the first row in each frame, and f2 holds the row's
        // position, so the last field is where each row's frame starts.
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, -2, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, -1, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 0, 1L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c8", 3L, 3, 3L)));
        new RowDataHarnessAssertor(outputType.getChildren().toArray(new LogicalType[0]))
                .assertOutputEquals(
                        "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Override
    public LogicalType[] getOutputLogicalType() {
        return new LogicalType[] {
            DataTypes.STRING().getLogicalType(),
            DataTypes.STRING().getLogicalType(),
            DataTypes.BIGINT().getLogicalType(),
            DataTypes.BIGINT().getLogicalType(),
            DataTypes.BIGINT().getLogicalType()
        };
    }

    @Override
    public RowType getInputType() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("f1", new VarCharType()),
                        new RowType.RowField("f2", new VarCharType()),
                        new RowType.RowField("f3", new BigIntType()),
                        new RowType.RowField("rowTime", new BigIntType())));
    }

    @Override
    public RowType getOutputType() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("f1", new VarCharType()),
                        new RowType.RowField("f2", new VarCharType()),
                        new RowType.RowField("f3", new BigIntType()),
                        new RowType.RowField("rowTime", new BigIntType()),
                        new RowType.RowField("agg", new BigIntType())));
    }

    @Override
    public AbstractArrowPythonAggregateFunctionOperator getTestOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggregateFunctions,
            RowType inputRowType,
            RowType outputRowType,
            int[] groupingSet,
            int[] udafInputOffsets) {
        // The range window is RANGE BETWEEN CURRENT ROW AND CURRENT ROW, ordered by rowTime.
        GeneratedRecordComparator rowTimeComparator =
                comparator((row, current) -> Long.compare(row.getLong(3), current.getLong(3)));
        return createTestOperator(
                config,
                pandasAggregateFunctions,
                inputRowType,
                outputRowType,
                groupingSet,
                udafInputOffsets,
                new long[] {0L, Long.MIN_VALUE},
                new long[] {0L, 2L},
                new boolean[] {true, false},
                new GeneratedRecordComparator[] {rowTimeComparator, null},
                new GeneratedRecordComparator[] {rowTimeComparator, null});
    }

    private static AbstractArrowPythonAggregateFunctionOperator createTestOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggregateFunctions,
            RowType inputRowType,
            RowType outputRowType,
            int[] groupingSet,
            int[] udafInputOffsets,
            long[] lowerBoundary,
            long[] upperBoundary,
            boolean[] isRangeWindows,
            GeneratedRecordComparator[] lowerBoundComparators,
            GeneratedRecordComparator[] upperBoundComparators) {
        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputRowType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(
                                        inputRowType.getFieldCount(), outputRowType.getFieldCount())
                                .project(outputRowType);

        return new PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator(
                config,
                pandasAggregateFunctions,
                inputRowType,
                udfInputType,
                udfOutputType,
                lowerBoundary,
                upperBoundary,
                isRangeWindows,
                new int[] {0},
                lowerBoundComparators,
                upperBoundComparators,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "UdafInputProjection",
                        inputRowType,
                        udfInputType,
                        udafInputOffsets),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "GroupKey",
                        inputRowType,
                        (RowType) Projection.of(groupingSet).project(inputRowType),
                        groupingSet),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "GroupSet",
                        inputRowType,
                        (RowType) Projection.of(groupingSet).project(inputRowType),
                        groupingSet));
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            AbstractArrowPythonAggregateFunctionOperator operator, RowType outputType)
            throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);
        testHarness
                .getStreamConfig()
                .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.PYTHON, 0.5);
        testHarness.setup(new RowDataSerializer(outputType));
        return testHarness;
    }

    private static GeneratedRecordComparator comparator(RecordComparator comparator) {
        return new GeneratedRecordComparator("", "", new Object[0]) {
            @Override
            public RecordComparator newInstance(ClassLoader classLoader) {
                return comparator;
            }
        };
    }

    private static class PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator
            extends BatchArrowPythonOverWindowAggregateFunctionOperator {

        PassThroughBatchArrowPythonOverWindowAggregateFunctionOperator(
                Configuration config,
                PythonFunctionInfo[] pandasAggFunctions,
                RowType inputType,
                RowType udfInputType,
                RowType udfOutputType,
                long[] lowerBoundary,
                long[] upperBoundary,
                boolean[] isRangeWindow,
                int[] aggWindowIndex,
                GeneratedRecordComparator[] lowerBoundComparators,
                GeneratedRecordComparator[] upperBoundComparators,
                GeneratedProjection inputGeneratedProjection,
                GeneratedProjection groupKeyGeneratedProjection,
                GeneratedProjection groupSetGeneratedProjection) {
            super(
                    config,
                    pandasAggFunctions,
                    inputType,
                    udfInputType,
                    udfOutputType,
                    lowerBoundary,
                    upperBoundary,
                    isRangeWindow,
                    aggWindowIndex,
                    lowerBoundComparators,
                    upperBoundComparators,
                    inputGeneratedProjection,
                    groupKeyGeneratedProjection,
                    groupSetGeneratedProjection);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() {
            return new PassThroughPythonAggregateFunctionRunner(
                    getContainingTask().getEnvironment(),
                    getRuntimeContext().getTaskInfo().getTaskName(),
                    PythonTestUtils.createTestProcessEnvironmentManager(),
                    udfInputType,
                    udfOutputType,
                    getFunctionUrn(),
                    createUserDefinedFunctionsProto(),
                    PythonTestUtils.createMockFlinkMetricContainer(),
                    true);
        }
    }
}
