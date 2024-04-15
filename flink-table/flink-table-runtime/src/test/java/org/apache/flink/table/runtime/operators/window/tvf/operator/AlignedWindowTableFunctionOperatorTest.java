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

package org.apache.flink.table.runtime.operators.window.tvf.operator;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.CumulativeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.TumblingWindowAssigner;
import org.apache.flink.types.RowKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AlignedWindowTableFunctionOperator}. */
@RunWith(Parameterized.class)
public class AlignedWindowTableFunctionOperatorTest extends WindowTableFunctionOperatorTestBase {

    public AlignedWindowTableFunctionOperatorTest(ZoneId shiftTimeZone) {
        super(shiftTimeZone);
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }

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
        // rowtime is null, should be dropped
        testHarness.processElement(insertRecord("key2", 1, ((Long) null)));
        expectedOutput.add(insertRecord("key2", 1, 80L, localMills(0L), localMills(3000L), 2999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(
                        ((AlignedWindowTableFunctionOperator) testHarness.getOperator())
                                .getNumNullRowTimeRecordsDropped()
                                .getCount())
                .isEqualTo(1);
        testHarness.close();
    }

    @Test
    public void testProcessingTimeTumblingWindows() throws Exception {
        final TumblingWindowAssigner assigner =
                TumblingWindowAssigner.of(Duration.ofSeconds(3)).withProcessingTime();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        // timestamp is ignored in processing time
        testHarness.setProcessingTime(20L);
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.setProcessingTime(3999L);
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3000L), localMills(6000L), 5999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
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
        testHarness.close();
    }

    @Test
    public void testProcessingTimeHopWindows() throws Exception {
        final SlidingWindowAssigner assigner =
                SlidingWindowAssigner.of(Duration.ofSeconds(3), Duration.ofSeconds(1))
                        .withProcessingTime();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        // timestamp is ignored in processing time
        testHarness.setProcessingTime(20L);
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.setProcessingTime(3999L);
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord(
                        "key1", 1, Long.MAX_VALUE, localMills(-2000L), localMills(1000L), 999L));
        expectedOutput.add(
                insertRecord(
                        "key1", 1, Long.MAX_VALUE, localMills(-1000L), localMills(2000L), 1999L));
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(1000L), localMills(4000L), 3999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(2000L), localMills(5000L), 4999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3000L), localMills(6000L), 5999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
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
        testHarness.close();
    }

    @Test
    public void testProcessingCumulativeWindows() throws Exception {
        final CumulativeWindowAssigner assigner =
                CumulativeWindowAssigner.of(Duration.ofSeconds(3), Duration.ofSeconds(1))
                        .withProcessingTime();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        // timestamp is ignored in processing time
        testHarness.setProcessingTime(20L);
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.setProcessingTime(3999L);
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(0), localMills(1000L), 999L));
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(0), localMills(2000L), 1999L));
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3000L), localMills(4000L), 3999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3000L), localMills(5000L), 4999L));
        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3000L), localMills(6000L), 5999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testConsumingChangelogRecords() throws Exception {
        final TumblingWindowAssigner assigner = TumblingWindowAssigner.of(Duration.ofSeconds(3));
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(assigner, shiftTimeZone);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, 20L));
        testHarness.processElement(binaryRecord(RowKind.UPDATE_BEFORE, "key1", 1, 30L));
        testHarness.processElement(binaryRecord(RowKind.UPDATE_AFTER, "key1", 1, 40L));
        testHarness.processWatermark(new Watermark(999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT, "key1", 1, 20L, localMills(0L), localMills(3000L), 2999L));
        expectedOutput.add(
                binaryRecord(
                        RowKind.UPDATE_BEFORE,
                        "key1",
                        1,
                        30L,
                        localMills(0L),
                        localMills(3000L),
                        2999L));
        expectedOutput.add(
                binaryRecord(
                        RowKind.UPDATE_AFTER,
                        "key1",
                        1,
                        40L,
                        localMills(0L),
                        localMills(3000L),
                        2999L));
        expectedOutput.add(new Watermark(999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(9999));
        expectedOutput.add(new Watermark(9999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late records would not be dropped
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key1", 1, 200L));

        expectedOutput.add(
                binaryRecord(
                        RowKind.DELETE, "key1", 1, 200L, localMills(0L), localMills(3000L), 2999L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            GroupWindowAssigner<TimeWindow> windowAssigner, ZoneId shiftTimeZone) throws Exception {
        AlignedWindowTableFunctionOperator operator =
                new AlignedWindowTableFunctionOperator(
                        windowAssigner, ROW_TIME_INDEX, shiftTimeZone);
        return new OneInputStreamOperatorTestHarness<>(operator, INPUT_ROW_SER);
    }
}
