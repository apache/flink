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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.utils.PassThroughPythonAggregateFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test for {@link StreamArrowPythonGroupWindowAggregateFunctionOperator}. These test that:
 *
 * <ul>
 *   <li>Retraction flag is handled correctly
 *   <li>FinishBundle is called when checkpoint is encountered
 *   <li>FinishBundle is called when bundled element count reach to max bundle size
 *   <li>FinishBundle is called when bundled time reach to max bundle time
 *   <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered
 * </ul>
 */
class StreamArrowPythonGroupWindowAggregateFunctionOperatorTest
        extends AbstractStreamArrowPythonAggregateFunctionOperatorTest {

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    @Test
    void testGroupWindowAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 4));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c3", "c8", 3L, 0L), initialTime + 5));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(false, "c3", "c8", 3L, 0L), initialTime + 6));
        testHarness.processWatermark(Long.MAX_VALUE);
        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(Long.MAX_VALUE));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredOnCheckpoint() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 4));
        testHarness.processWatermark(new Watermark(10000L));
        // checkpoint trigger finishBundle
        testHarness.prepareSnapshotPreBarrier(0L);

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(new Watermark(10000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(20000L);

        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 4);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 4));

        testHarness.processWatermark(new Watermark(10000L));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(new Watermark(10000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(20000L);
        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Override
    public LogicalType[] getOutputLogicalType() {
        return new LogicalType[] {
            DataTypes.STRING().getLogicalType(), DataTypes.BIGINT().getLogicalType()
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
                        new RowType.RowField("f2", new BigIntType()),
                        new RowType.RowField("windowStart", new TimestampType(3)),
                        new RowType.RowField("windowEnd", new TimestampType(3))));
    }

    @Override
    public AbstractArrowPythonAggregateFunctionOperator getTestOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggregateFunctions,
            RowType inputType,
            RowType outputType,
            int[] groupingSet,
            int[] udafInputOffsets) {
        long size = 10000L;
        long slide = 5000L;
        SlidingWindowAssigner windowAssigner =
                SlidingWindowAssigner.of(Duration.ofMillis(size), Duration.ofMillis(slide))
                        .withEventTime();
        EventTimeTriggers.AfterEndOfWindow<Window> trigger = EventTimeTriggers.afterEndOfWindow();

        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(groupingSet.length, outputType.getFieldCount() - 2)
                                .project(outputType);

        return new PassThroughStreamArrowPythonGroupWindowAggregateFunctionOperator(
                config,
                pandasAggregateFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                3,
                windowAssigner,
                trigger,
                0,
                new NamedWindowProperty[] {
                    new NamedWindowProperty("start", new WindowStart(null)),
                    new NamedWindowProperty("end", new WindowEnd(null))
                },
                UTC_ZONE_ID,
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "UdafInputProjection",
                        inputType,
                        udfInputType,
                        udafInputOffsets));
    }

    private static class PassThroughStreamArrowPythonGroupWindowAggregateFunctionOperator
            extends StreamArrowPythonGroupWindowAggregateFunctionOperator {

        PassThroughStreamArrowPythonGroupWindowAggregateFunctionOperator(
                Configuration config,
                PythonFunctionInfo[] pandasAggFunctions,
                RowType inputType,
                RowType udfInputType,
                RowType udfOutputType,
                int inputTimeFieldIndex,
                WindowAssigner windowAssigner,
                Trigger trigger,
                long allowedLateness,
                NamedWindowProperty[] namedProperties,
                ZoneId shiftTimeZone,
                GeneratedProjection generatedProjection) {
            super(
                    config,
                    pandasAggFunctions,
                    inputType,
                    udfInputType,
                    udfOutputType,
                    inputTimeFieldIndex,
                    windowAssigner,
                    trigger,
                    allowedLateness,
                    namedProperties,
                    shiftTimeZone,
                    generatedProjection);
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
