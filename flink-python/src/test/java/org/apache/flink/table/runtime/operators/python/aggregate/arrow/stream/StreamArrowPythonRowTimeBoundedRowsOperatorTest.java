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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.utils.PassThroughPythonAggregateFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test for {@link StreamArrowPythonRowTimeBoundedRowsOperator}. These test that:
 *
 * <ul>
 *   <li>FinishBundle is called when checkpoint is encountered
 *   <li>FinishBundle is called when bundled element count reach to max bundle size
 *   <li>FinishBundle is called when bundled time reach to max bundle time
 *   <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered
 * </ul>
 */
class StreamArrowPythonRowTimeBoundedRowsOperatorTest
        extends AbstractStreamArrowPythonAggregateFunctionOperatorTest {

    @Test
    void testOverWindowAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 1L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 1L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 2L), initialTime + 3));
        testHarness.processWatermark(Long.MAX_VALUE);

        testHarness.close();

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 2L, 3L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 1L)));
        expectedOutput.add(new Watermark(Long.MAX_VALUE));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredOnWatermark() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 1L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 1L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 2L), initialTime + 3));
        testHarness.processWatermark(new Watermark(10000L));

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 2L, 3L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 1L)));
        expectedOutput.add(new Watermark(10000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 6);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 1L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 1L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 2L), initialTime + 3));

        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1000L));

        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c2", 0L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c4", 1L, 1L, 0L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c2", "c8", 3L, 2L, 3L)));
        expectedOutput.add(new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10L, 1L)));
        expectedOutput.add(new Watermark(1000L));
        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
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
            RowType inputType,
            RowType outputType,
            int[] groupingSet,
            int[] udafInputOffsets) {
        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(inputType.getFieldCount(), outputType.getFieldCount())
                                .project(outputType);

        return new PassThroughStreamArrowPythonRowTimeBoundedRowsOperator(
                config,
                pandasAggregateFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                3,
                1,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "UdafInputProjection",
                        inputType,
                        udfInputType,
                        udafInputOffsets));
    }

    private static class PassThroughStreamArrowPythonRowTimeBoundedRowsOperator
            extends StreamArrowPythonRowTimeBoundedRowsOperator {

        PassThroughStreamArrowPythonRowTimeBoundedRowsOperator(
                Configuration config,
                PythonFunctionInfo[] pandasAggFunctions,
                RowType inputType,
                RowType udfInputType,
                RowType udfOutputType,
                int inputTimeFieldIndex,
                long lowerBoundary,
                GeneratedProjection inputGeneratedProjection) {
            super(
                    config,
                    1000,
                    2000,
                    pandasAggFunctions,
                    inputType,
                    udfInputType,
                    udfOutputType,
                    inputTimeFieldIndex,
                    lowerBoundary,
                    inputGeneratedProjection);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() {
            return new PassThroughPythonAggregateFunctionRunner(
                    getRuntimeContext().getTaskName(),
                    PythonTestUtils.createTestProcessEnvironmentManager(),
                    udfInputType,
                    udfOutputType,
                    getFunctionUrn(),
                    createUserDefinedFunctionsProto(),
                    PythonTestUtils.createMockFlinkMetricContainer(),
                    false);
        }
    }
}
