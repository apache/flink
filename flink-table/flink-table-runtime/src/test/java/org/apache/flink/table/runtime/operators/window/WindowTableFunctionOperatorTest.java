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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.assigners.CumulativeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;

/** Tests for {@link WindowTableFunctionOperator}. */
@RunWith(Parameterized.class)
public class WindowTableFunctionOperatorTest {

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    private static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");
    private final ZoneId shiftTimeZone;

    public WindowTableFunctionOperatorTest(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }

    private static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new IntType()),
                            new RowType.RowField("f2", new TimestampType(3))));

    private static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);
    private static final int ROW_TIME_INDEX = 2;

    private static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE),
                new IntType(),
                new TimestampType(3),
                new TimestampType(3),
                new TimestampType(3),
                new TimestampType(3)
            };

    private static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    private static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES, new GenericRowRecordSortComparator(4, new TimestampType()));

    @Test
    public void testTumblingWindows() throws Exception {
        final TumblingWindowAssigner assigner = TumblingWindowAssigner.of(Duration.ofSeconds(3));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processWatermark(new Watermark(999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3000L), localMills(6000L), 5999L));
        expectedOutput.add(new Watermark(999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element would not be dropped
        testHarness.processElement(insertRecord("key2", 1, 80L));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0L), localMills(3000L), 2999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testHopWindows() throws Exception {
        final SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofSeconds(3), Duration.ofSeconds(1));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processWatermark(new Watermark(999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord("key1", 1, 20L, localMills(-2000L), localMills(1000L), 999L));
        expectedOutput.add(
                insertRecord("key1", 1, 20L, localMills(-1000L), localMills(2000L), 1999L));
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(1000L), localMills(4000L), 3999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(2000L), localMills(5000L), 4999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3000L), localMills(6000L), 5999L));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element would not be dropped
        testHarness.processElement(insertRecord("key2", 1, 80L));
        expectedOutput.add(
                insertRecord("key2", 1, 80L, localMills(-2000L), localMills(1000L), 999L));
        expectedOutput.add(
                insertRecord("key2", 1, 80L, localMills(-1000L), localMills(2000L), 1999L));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0L), localMills(3000L), 2999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testCumulativeWindows() throws Exception {
        final CumulativeWindowAssigner assigner =
                CumulativeWindowAssigner.of(Duration.ofSeconds(3), Duration.ofSeconds(1));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processWatermark(new Watermark(999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(0), localMills(1000L), 999L));
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(0), localMills(2000L), 1999L));
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3000L), localMills(4000L), 3999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3000L), localMills(5000L), 4999L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3000L), localMills(6000L), 5999L));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element would not be dropped
        testHarness.processElement(insertRecord("key2", 1, 80L));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0), localMills(1000L), 999L));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0), localMills(2000L), 1999L));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0L), localMills(3000L), 2999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            WindowAssigner<TimeWindow> windowAssigner, ZoneId shiftTimeZone) throws Exception {
        WindowTableFunctionOperator operator =
                new WindowTableFunctionOperator(windowAssigner, ROW_TIME_INDEX, shiftTimeZone);
        return new OneInputStreamOperatorTestHarness<>(operator, INPUT_ROW_SER);
    }

    private StreamRecord<RowData> insertRecord(String f0, int f1, Long... f2) {
        Object[] fields = new Object[2 + f2.length];
        fields[0] = f0;
        fields[1] = f1;
        for (int idx = 0; idx < f2.length; idx++) {
            fields[2 + idx] = TimestampData.fromEpochMillis(f2[idx]);
        }
        return new StreamRecord<>(row(fields));
    }

    /** Get the timestamp in mills by given epoch mills and timezone. */
    private long localMills(long epochMills) {
        return toUtcTimestampMills(epochMills, shiftTimeZone);
    }
}
