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
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty;
import org.apache.flink.table.planner.expressions.PlannerWindowEnd;
import org.apache.flink.table.planner.expressions.PlannerWindowReference;
import org.apache.flink.table.planner.expressions.PlannerWindowStart;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    @Test
    public void testGroupWindowAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.open();
        testHarness.processElement(newRecord(true, initialTime + 1, "c1", "c2", 0L, 0L));
        testHarness.processElement(newRecord(true, initialTime + 2, "c1", "c4", 1L, 6000L));
        testHarness.processElement(newRecord(true, initialTime + 3, "c1", "c6", 2L, 10000L));
        testHarness.processElement(newRecord(true, initialTime + 4, "c2", "c8", 3L, 0L));
        testHarness.processElement(newRecord(true, initialTime + 5, "c3", "c8", 3L, 0L));
        testHarness.processElement(newRecord(false, initialTime + 6, "c3", "c8", 3L, 0L));
        testHarness.processWatermark(Long.MAX_VALUE);
        testHarness.close();

        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c1"));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c3"));
        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c1"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c2"));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c3"));
        expectedOutput.add(newWindowRecord(5000L, 15000L, "c1", 1L));
        expectedOutput.add(newStateCleanupRecord(5000L, 15000L, "c1"));
        expectedOutput.add(newWindowRecord(10000L, 20000L, "c1", 2L));
        expectedOutput.add(newStateCleanupRecord(10000L, 20000L, "c1"));

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

        testHarness.processElement(newRecord(true, initialTime + 1, "c1", "c2", 0L, 0L));
        testHarness.processElement(newRecord(true, initialTime + 2, "c1", "c4", 1L, 6000L));
        testHarness.processElement(newRecord(true, initialTime + 3, "c1", "c6", 2L, 10000L));
        testHarness.processElement(newRecord(true, initialTime + 4, "c2", "c8", 3L, 0L));
        testHarness.processWatermark(new Watermark(10000L));
        // checkpoint trigger finishBundle
        testHarness.prepareSnapshotPreBarrier(0L);

        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c1"));
        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c1"));

        expectedOutput.add(new Watermark(10000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(20000L);

        testHarness.close();

        expectedOutput.add(newWindowRecord(5000L, 15000L, "c1", 1L));
        expectedOutput.add(newStateCleanupRecord(5000L, 15000L, "c1"));
        expectedOutput.add(newWindowRecord(10000L, 20000L, "c1", 2L));
        expectedOutput.add(newStateCleanupRecord(10000L, 20000L, "c1"));

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

        testHarness.processElement(newRecord(true, initialTime + 1, "c1", "c2", 0L, 0L));
        testHarness.processElement(newRecord(true, initialTime + 2, "c1", "c4", 1L, 6000L));
        testHarness.processElement(newRecord(true, initialTime + 3, "c1", "c6", 2L, 10000L));
        testHarness.processElement(newRecord(true, initialTime + 4, "c2", "c8", 3L, 0L));

        testHarness.processWatermark(new Watermark(10000L));

        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c1"));
        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c1"));

        expectedOutput.add(new Watermark(10000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(20000L);
        testHarness.close();

        expectedOutput.add(newWindowRecord(5000L, 15000L, "c1", 1L));
        expectedOutput.add(newStateCleanupRecord(5000L, 15000L, "c1"));
        expectedOutput.add(newWindowRecord(10000L, 20000L, "c1", 2L));
        expectedOutput.add(newStateCleanupRecord(10000L, 20000L, "c1"));

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

        testHarness.processElement(newRecord(true, initialTime + 1, "c1", "c2", 0L, 0L));
        testHarness.processElement(newRecord(true, initialTime + 2, "c1", "c4", 1L, 6000L));
        testHarness.processElement(newRecord(true, initialTime + 3, "c1", "c6", 2L, 10000L));
        testHarness.processElement(newRecord(true, initialTime + 4, "c2", "c8", 3L, 0L));
        testHarness.processWatermark(new Watermark(20000L));

        testHarness.setProcessingTime(1000L);
        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c1"));
        expectedOutput.add(newWindowRecord(-5000L, 5000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(-5000L, 5000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c2", 3L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c2"));
        expectedOutput.add(newWindowRecord(0, 10000L, "c1", 0L));
        expectedOutput.add(newStateCleanupRecord(0L, 10000L, "c1"));
        expectedOutput.add(newWindowRecord(5000L, 15000L, "c1", 1L));
        expectedOutput.add(newStateCleanupRecord(5000L, 15000L, "c1"));
        expectedOutput.add(newWindowRecord(10000L, 20000L, "c1", 2L));
        expectedOutput.add(newStateCleanupRecord(10000L, 20000L, "c1"));

        expectedOutput.add(new Watermark(20000L));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    private StreamRecord<RowData> newRecord(
            boolean accumulateMsg, long timestamp, Object... fields) {
        return new StreamRecord<>(newRow(accumulateMsg, fields), timestamp);
    }

    private StreamRecord<RowData> newWindowRecord(long start, long end, Object... fields) {
        Object[] rowFields = new Object[fields.length + 2];
        System.arraycopy(fields, 0, rowFields, 0, fields.length);
        rowFields[rowFields.length - 2] = TimestampData.fromEpochMillis(start);
        rowFields[rowFields.length - 1] = TimestampData.fromEpochMillis(end);
        return new StreamRecord<>(newRow(true, rowFields));
    }

    private StreamRecord newStateCleanupRecord(long start, long end, Object key) {
        String field =
                String.format(
                        "state_cleanup_triggered: %s : TimeWindow{start=%s, end=%s}",
                        key, start, end);
        return new StreamRecord<>(
                newRow(
                        true,
                        field,
                        0L,
                        TimestampData.fromEpochMillis(start),
                        TimestampData.fromEpochMillis(end)));
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
        PlannerWindowReference windowRef = new PlannerWindowReference("w$", new TimestampType(3));
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
                0L,
                new PlannerNamedWindowProperty[] {
                    new PlannerNamedWindowProperty("start", new PlannerWindowStart(null)),
                    new PlannerNamedWindowProperty("end", new PlannerWindowEnd(null))
                },
                UTC_ZONE_ID);
    }
}
