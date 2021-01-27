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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.UpdatableRowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.expressions.PlannerWindowReference;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.utils.PassThroughStreamGroupWindowAggregatePythonFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Some;

import static org.apache.flink.table.expressions.ApiExpressionUtils.intervalOfMillis;

/**
 * Test for {@link PythonStreamGroupWindowAggregateOperator}. These test that:
 *
 * <ul>
 *   <li>Retraction flag is handled correctly
 *   <li>FinishBundle is called when checkpoint is encountered
 *   <li>FinishBundle is called when bundled element count reach to max bundle size
 *   <li>FinishBundle is called when bundled time reach to max bundle time
 *   <li>Watermarks are buffered and only sent to downstream when finishedBundle is triggered
 * </ul>
 */
public class PythonStreamGroupWindowAggregateOperatorTest
        extends AbstractPythonStreamAggregateOperatorTest {
    @Test
    public void testGroupWindowAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.open();
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L), initialTime + 4));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c3", "c8", 3L, 0L), initialTime + 5));
        testHarness.processElement(
                new StreamRecord<>(newRow(false, "c3", "c8", 3L, 0L), initialTime + 6));
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=-5000, end=5000}",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c3 : TimeWindow{start=-5000, end=5000}",
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=-5000, end=5000}",
                                0L,
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=0, end=10000}",
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=0, end=10000}",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c3 : TimeWindow{start=0, end=10000}",
                                0L,
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=5000, end=15000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=10000, end=20000}",
                                0L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(Long.MAX_VALUE));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testFinishBundleTriggeredOnCheckpoint() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L), initialTime + 4));
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=-5000, end=5000}",
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=-5000, end=5000}",
                                0L,
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=0, end=10000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=0, end=10000}",
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=5000, end=15000}",
                                0L,
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
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=10000, end=20000}",
                                0L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 4);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L), initialTime + 4));

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
                                "state_cleanup_triggered: c1 : TimeWindow{start=-5000, end=5000}",
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=-5000, end=5000}",
                                0L,
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=0, end=10000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=0, end=10000}",
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=5000, end=15000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=10000, end=20000}",
                                0L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testFinishBundleTriggeredByTime() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newRow(true, "c2", "c8", 3L, 0L), initialTime + 4));
        testHarness.processWatermark(new Watermark(20000L));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1000L);
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=-5000, end=5000}",
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=-5000, end=5000}",
                                0L,
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
                                "state_cleanup_triggered: c2 : TimeWindow{start=0, end=10000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=0, end=10000}",
                                0L,
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
                                "state_cleanup_triggered: c1 : TimeWindow{start=5000, end=15000}",
                                0L,
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

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "state_cleanup_triggered: c1 : TimeWindow{start=10000, end=20000}",
                                0L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
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
    OneInputStreamOperator getTestOperator(Configuration config) {
        long size = 10000L;
        long slide = 5000L;
        SlidingWindowAssigner windowAssigner =
                SlidingWindowAssigner.of(Duration.ofMillis(size), Duration.ofMillis(slide))
                        .withEventTime();
        PlannerWindowReference windowRef =
                new PlannerWindowReference("w$", new Some<>(new TimestampType(3)));
        LogicalWindow window =
                new SlidingGroupWindow(
                        windowRef,
                        new FieldReferenceExpression(
                                "rowtime",
                                new AtomicDataType(
                                        new TimestampType(true, TimestampKind.ROWTIME, 3)),
                                0,
                                3),
                        intervalOfMillis(size),
                        intervalOfMillis(slide));
        return new PassThroughPythonStreamGroupWindowAggregateOperator(
                config,
                getInputType(),
                getOutputType(),
                new PythonAggregateFunctionInfo[] {
                    new PythonAggregateFunctionInfo(
                            PythonScalarFunctionOperatorTestBase.DummyPythonFunction.INSTANCE,
                            new Integer[] {2},
                            -1,
                            false)
                },
                getGrouping(),
                -1,
                false,
                false,
                3,
                windowAssigner,
                window,
                0,
                new int[] {0, 1});
    }

    /** PassThroughPythonStreamGroupWindowAggregateOperator. */
    public static class PassThroughPythonStreamGroupWindowAggregateOperator<K>
            extends PythonStreamGroupWindowAggregateOperator<K, TimeWindow> {

        final MockPythonWindowOperator<K> mockPythonWindowOperator;
        private final int[] grouping;
        private final PythonAggregateFunctionInfo aggregateFunction;
        private final int[] namedProperties;
        InternalTimerServiceImpl<K, TimeWindow> mockPythonInternalService;
        Map<String, Map<TimeWindow, List<RowData>>> windowAccumulateData;
        Map<String, Map<TimeWindow, List<RowData>>> windowRetractData;
        transient UpdatableRowData reusePythonRowData;

        transient UpdatableRowData reusePythonTimerRowData;
        transient UpdatableRowData reusePythonTimerData;
        transient LinkedBlockingQueue<byte[]> resultBuffer;
        private Projection<RowData, BinaryRowData> groupKeyProjection;
        private Function<RowData, RowData> aggExtracter;
        private Function<TimeWindow, RowData> windowExtractor;
        private JoinedRowData reuseJoinedRow;
        private JoinedRowData windowAggResult;

        public PassThroughPythonStreamGroupWindowAggregateOperator(
                Configuration config,
                RowType inputType,
                RowType outputType,
                PythonAggregateFunctionInfo[] aggregateFunctions,
                int[] grouping,
                int indexOfCountStar,
                boolean generateUpdateBefore,
                boolean countStarInserted,
                int inputTimeFieldIndex,
                WindowAssigner<TimeWindow> windowAssigner,
                LogicalWindow window,
                long allowedLateness,
                int[] namedProperties) {
            super(
                    config,
                    inputType,
                    outputType,
                    aggregateFunctions,
                    new DataViewUtils.DataViewSpec[0][0],
                    grouping,
                    indexOfCountStar,
                    generateUpdateBefore,
                    countStarInserted,
                    inputTimeFieldIndex,
                    windowAssigner,
                    window,
                    allowedLateness,
                    namedProperties);
            this.mockPythonWindowOperator = new MockPythonWindowOperator<>();
            this.aggregateFunction = aggregateFunctions[0];
            this.namedProperties = namedProperties;
            this.grouping = grouping;
        }

        @Override
        public void open() throws Exception {
            super.open();
            reusePythonRowData =
                    new UpdatableRowData(GenericRowData.of(NORMAL_RECORD, null, null), 3);
            reusePythonTimerRowData =
                    new UpdatableRowData(GenericRowData.of(TRIGGER_TIMER, null, null), 3);
            reusePythonTimerData =
                    new UpdatableRowData(GenericRowData.of(0, null, null, null, null), 5);
            reuseJoinedRow = new JoinedRowData();
            windowAggResult = new JoinedRowData();
            reusePythonTimerRowData.setField(2, reusePythonTimerData);
            windowAccumulateData = new HashMap<>();
            windowRetractData = new HashMap<>();
            mockPythonInternalService =
                    (InternalTimerServiceImpl<K, TimeWindow>)
                            getInternalTimerService(
                                    "python-window-timers",
                                    windowSerializer,
                                    this.mockPythonWindowOperator);
            this.groupKeyProjection = createProjection("GroupKey", grouping);
            int inputFieldIndex = (int) aggregateFunction.getInputs()[0];
            this.aggExtracter =
                    input -> {
                        GenericRowData aggResult = new GenericRowData(1);
                        aggResult.setField(0, input.getLong(inputFieldIndex));
                        return aggResult;
                    };
            this.windowExtractor =
                    window -> {
                        GenericRowData windowProperty = new GenericRowData(namedProperties.length);
                        for (int i = 0; i < namedProperties.length; i++) {
                            switch (namedProperties[i]) {
                                case 0:
                                    windowProperty.setField(
                                            i, TimestampData.fromEpochMillis(window.getStart()));
                                    break;
                                case 1:
                                    windowProperty.setField(
                                            i, TimestampData.fromEpochMillis(window.getEnd()));
                                    break;
                                case 2:
                                    windowProperty.setField(
                                            i, TimestampData.fromEpochMillis(window.getEnd() - 1));
                                    break;
                                case 3:
                                    windowProperty.setField(i, TimestampData.fromEpochMillis(-1));
                            }
                        }
                        return windowProperty;
                    };
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
            return new PassThroughStreamGroupWindowAggregatePythonFunctionRunner(
                    getRuntimeContext().getTaskName(),
                    PythonTestUtils.createTestEnvironmentManager(),
                    userDefinedFunctionInputType,
                    userDefinedFunctionOutputType,
                    STREAM_GROUP_WINDOW_AGGREGATE_URN,
                    getUserDefinedFunctionsProto(),
                    FLINK_AGGREGATE_FUNCTION_SCHEMA_CODER_URN,
                    new HashMap<>(),
                    PythonTestUtils.createMockFlinkMetricContainer(),
                    getKeyedStateBackend(),
                    getKeySerializer(),
                    this);
        }

        public void processPythonElement(byte[] inputBytes) {
            try {
                RowData input =
                        udfInputTypeSerializer.deserialize(new DataInputDeserializer(inputBytes));
                if (input.getByte(0) == NORMAL_RECORD) {
                    // normal data
                    RowData inputRow = input.getRow(1, inputType.getFieldCount());
                    BinaryRowData key = groupKeyProjection.apply(inputRow).copy();
                    Map<TimeWindow, List<RowData>> curKeyWindowAccumulateData =
                            windowAccumulateData.computeIfAbsent(
                                    key.getString(0).toString(), k -> new HashMap<>());
                    Map<TimeWindow, List<RowData>> curKeyWindowRetractData =
                            windowRetractData.computeIfAbsent(
                                    key.getString(0).toString(), k -> new HashMap<>());

                    long watermark = input.getLong(2);
                    // advance watermark
                    mockPythonInternalService.advanceWatermark(watermark);

                    // get timestamp
                    long timestamp = inputRow.getLong(inputTimeFieldIndex);
                    Collection<TimeWindow> elementWindows =
                            windowAssigner.assignWindows(inputRow, timestamp);
                    for (TimeWindow window : elementWindows) {
                        if (RowDataUtil.isAccumulateMsg(inputRow)) {
                            List<RowData> currentWindowDatas =
                                    curKeyWindowAccumulateData.computeIfAbsent(
                                            window, k -> new LinkedList<>());
                            currentWindowDatas.add(inputRow);
                        } else {
                            List<RowData> currentWindowDatas =
                                    curKeyWindowRetractData.computeIfAbsent(
                                            window, k -> new LinkedList<>());
                            currentWindowDatas.add(inputRow);
                        }
                    }
                    List<TimeWindow> actualWindows = new ArrayList<>(elementWindows.size());
                    for (TimeWindow window : elementWindows) {
                        if (!isWindowLate(window)) {
                            actualWindows.add(window);
                        }
                    }
                    for (TimeWindow window : actualWindows) {
                        boolean triggerResult = onElement(key, window);
                        if (triggerResult) {
                            triggerWindowProcess(key, window);
                        }
                        // register a clean up timer for the window
                        registerCleanupTimer(key, window);
                    }
                } else {
                    RowData timerData = input.getRow(3, 4);
                    long timestamp = input.getLong(2);
                    RowData key = timerData.getRow(1, getKeyType().getFieldCount());
                    long start = timerData.getLong(2);
                    long end = timerData.getLong(3);
                    TimeWindow window = TimeWindow.of(start, end);
                    if (timestamp == window.maxTimestamp()) {
                        triggerWindowProcess(key, window);
                    }
                    cleanWindowIfNeeded(key, window, timestamp);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void setResultBuffer(LinkedBlockingQueue<byte[]> resultBuffer) {
            this.resultBuffer = resultBuffer;
        }

        private boolean isWindowLate(TimeWindow window) {
            return windowAssigner.isEventTime()
                    && (cleanupTime(window) <= mockPythonInternalService.currentWatermark());
        }

        private long cleanupTime(TimeWindow window) {
            if (windowAssigner.isEventTime()) {
                long cleanupTime = window.maxTimestamp() + allowedLateness;
                return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
            } else {
                return window.maxTimestamp();
            }
        }

        private boolean onElement(BinaryRowData key, TimeWindow window) throws IOException {
            if (window.maxTimestamp() <= mockPythonInternalService.currentWatermark()) {
                return true;
            } else {
                if (windowAssigner.isEventTime()) {
                    registerEventTimeTimer(key, window);
                } else {
                    registerProcessingTimeTimer(key, window);
                }
                return false;
            }
        }

        private void triggerWindowProcess(RowData key, TimeWindow window) throws Exception {
            DataOutputSerializer output = new DataOutputSerializer(1);
            Iterable<RowData> currentWindowAccumulateData =
                    windowAccumulateData.get(key.getString(0).toString()).get(window);
            Iterable<RowData> currentWindowRetractData =
                    windowRetractData.get(key.getString(0).toString()).get(window);
            if (currentWindowAccumulateData != null) {
                for (RowData accumulateData : currentWindowAccumulateData) {
                    if (!hasRetractData(accumulateData, currentWindowRetractData)) {
                        // only output first value in group window.
                        RowData aggResult = aggExtracter.apply(accumulateData);
                        RowData windowProperty = windowExtractor.apply(window);
                        windowAggResult.replace(key, aggResult);
                        reuseJoinedRow.replace(windowAggResult, windowProperty);
                        reusePythonRowData.setField(1, reuseJoinedRow);
                        udfOutputTypeSerializer.serialize(reusePythonRowData, output);
                        resultBuffer.add(output.getCopyOfBuffer());
                        break;
                    }
                }
            }
        }

        private boolean hasRetractData(
                RowData accumulateData, Iterable<RowData> currentWindowRetractData) {
            if (currentWindowRetractData != null) {
                for (RowData retractData : currentWindowRetractData) {
                    if (retractData.getRowKind() == RowKind.UPDATE_BEFORE) {
                        retractData.setRowKind(RowKind.UPDATE_AFTER);
                    } else {
                        retractData.setRowKind(RowKind.INSERT);
                    }
                    if (accumulateData.equals(retractData)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private Projection<RowData, BinaryRowData> createProjection(String name, int[] fields) {
            final RowType forwardedFieldType =
                    new RowType(
                            Arrays.stream(fields)
                                    .mapToObj(i -> inputType.getFields().get(i))
                                    .collect(Collectors.toList()));
            final GeneratedProjection generatedProjection =
                    ProjectionCodeGenerator.generateProjection(
                            CodeGeneratorContext.apply(new TableConfig()),
                            name,
                            inputType,
                            forwardedFieldType,
                            fields);
            // noinspection unchecked
            return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
        }

        private void registerCleanupTimer(RowData key, TimeWindow window) throws IOException {
            long cleanupTime = cleanupTime(window);
            if (cleanupTime == Long.MAX_VALUE) {
                // don't set a GC timer for "end of time"
                return;
            }
            if (windowAssigner.isEventTime()) {
                registerEventTimeTimer(key, window);
            } else {
                registerProcessingTimeTimer(key, window);
            }
        }

        private void registerEventTimeTimer(RowData key, TimeWindow window) throws IOException {
            reusePythonTimerData.setByte(
                    0, PythonStreamGroupWindowAggregateOperator.REGISTER_EVENT_TIMER);
            reusePythonTimerData.setField(1, key);
            reusePythonTimerData.setLong(2, window.maxTimestamp());
            reusePythonTimerData.setLong(3, window.getStart());
            reusePythonTimerData.setLong(4, window.getEnd());
            DataOutputSerializer output = new DataOutputSerializer(1);
            udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
            resultBuffer.add(output.getCopyOfBuffer());
        }

        private void deleteEventTimeTimer(RowData key, TimeWindow window) throws IOException {
            reusePythonTimerData.setByte(
                    0, PythonStreamGroupWindowAggregateOperator.DELETE_EVENT_TIMER);
            reusePythonTimerData.setField(1, key);
            reusePythonTimerData.setLong(2, window.maxTimestamp());
            reusePythonTimerData.setLong(3, window.getStart());
            reusePythonTimerData.setLong(4, window.getEnd());
            DataOutputSerializer output = new DataOutputSerializer(1);
            udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
            resultBuffer.add(output.getCopyOfBuffer());
        }

        private void registerProcessingTimeTimer(RowData key, TimeWindow window)
                throws IOException {
            reusePythonTimerData.setByte(
                    0, PythonStreamGroupWindowAggregateOperator.REGISTER_PROCESSING_TIMER);
            reusePythonTimerData.setField(1, key);
            reusePythonTimerData.setLong(2, window.maxTimestamp());
            reusePythonTimerData.setLong(3, window.getStart());
            reusePythonTimerData.setLong(4, window.getEnd());
            DataOutputSerializer output = new DataOutputSerializer(1);
            udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
            resultBuffer.add(output.getCopyOfBuffer());
        }

        private void deleteProcessingTimeTimer(RowData key, TimeWindow window) throws IOException {
            reusePythonTimerData.setByte(
                    0, PythonStreamGroupWindowAggregateOperator.DELETE_PROCESSING_TIMER);
            reusePythonTimerData.setField(1, key);
            reusePythonTimerData.setLong(2, window.maxTimestamp());
            reusePythonTimerData.setLong(3, window.getStart());
            reusePythonTimerData.setLong(4, window.getEnd());
            DataOutputSerializer output = new DataOutputSerializer(1);
            udfOutputTypeSerializer.serialize(reusePythonTimerRowData, output);
            resultBuffer.add(output.getCopyOfBuffer());
        }

        private void cleanWindowIfNeeded(RowData key, TimeWindow window, long currentTime)
                throws IOException {
            if (currentTime == cleanupTime(window)) {
                // 1. delete state
                // only output first value in group window.
                DataOutputSerializer output = new DataOutputSerializer(1);
                RowData windowProperty = windowExtractor.apply(window);
                windowAggResult.replace(
                        GenericRowData.of(
                                StringData.fromString(
                                        "state_cleanup_triggered: "
                                                + key.getString(0).toString()
                                                + " : "
                                                + window)),
                        GenericRowData.of(0L));
                reuseJoinedRow.replace(windowAggResult, windowProperty);
                reusePythonRowData.setField(1, reuseJoinedRow);
                udfOutputTypeSerializer.serialize(reusePythonRowData, output);
                resultBuffer.add(output.getCopyOfBuffer());
                // 2. delete window timer
                if (windowAssigner.isEventTime()) {
                    deleteEventTimeTimer(key, window);
                } else {
                    deleteProcessingTimeTimer(key, window);
                }
            }
        }
    }

    private static class MockPythonWindowOperator<K> implements Triggerable<K, TimeWindow> {

        MockPythonWindowOperator() {}

        @Override
        public void onEventTime(InternalTimer<K, TimeWindow> timer) throws Exception {}

        @Override
        public void onProcessingTime(InternalTimer<K, TimeWindow> timer) throws Exception {}
    }
}
