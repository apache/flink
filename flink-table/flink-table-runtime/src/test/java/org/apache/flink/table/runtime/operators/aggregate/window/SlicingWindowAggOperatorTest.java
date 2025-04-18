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

package org.apache.flink.table.runtime.operators.aggregate.window;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for slicing window aggregate operators created by {@link WindowAggOperatorBuilder}. */
@ExtendWith(ParameterizedTestExtension.class)
class SlicingWindowAggOperatorTest extends WindowAggOperatorTestBase {

    public SlicingWindowAggOperatorTest(ZoneId shiftTimeZone, boolean enableAsyncState) {
        super(shiftTimeZone, enableAsyncState);
    }

    @Parameters(name = "TimeZone = {0}, EnableAsyncState = {1}")
    private static Collection<Object[]> runMode() {
        return Arrays.asList(
                new Object[] {UTC_ZONE_ID, false},
                new Object[] {UTC_ZONE_ID, true},
                new Object[] {SHANGHAI_ZONE_ID, false},
                new Object[] {SHANGHAI_ZONE_ID, true});
    }

    @TestTemplate
    void testEventTimeHoppingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, 1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(20L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(0L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1000L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-2000L), localMills(1000L)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 5L, 5L, localMills(1000L), localMills(4000L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element for [1K, 4K), but should be accumulated into [2K, 5K), [3K, 6K)
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3500L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(2000L), localMills(5000L)));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(2999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(getNumLateRecordsDroppedCount(operator)).isEqualTo(1);

        testHarness.close();
    }

    @TestTemplate
    public void testEventTimeHoppingWindowWithExpiredSliceAndRestore() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, 1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // 1. process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1020L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1001L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1999L)));

        testHarness.processWatermark(new Watermark(2001));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(new Watermark(2001));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // 2. do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        // 3. process elements
        // Expired slice but belong to other window.
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1500L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2000L)));

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 4L, 4L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @TestTemplate
    public void testEventTimeHoppingWindowWithExpiredSliceAndNoRestore() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, 1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // 1. process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1020L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1001L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(1999L)));

        testHarness.processWatermark(new Watermark(2001));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(new Watermark(2001));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // 2. process elements
        // Expired slice but belong to other window.
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1500L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2000L)));

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 4L, 4L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @TestTemplate
    void testProcessingTimeHoppingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(-1, shiftTimeZone, Duration.ofHours(3), Duration.ofHours(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, 1);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T01:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1969-12-31T22:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T02:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1969-12-31T23:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T02:00:00")));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T03:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T07:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T04:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        5L,
                        5L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T04:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        5L,
                        5L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T02:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T06:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);
    }

    @TestTemplate
    void testEventTimeCumulativeWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.cumulative(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, null);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(20L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(0L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1000L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(1000L)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(2000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(2000L)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();
        // the late event would not trigger window [0, 2000L) again even if the job restore from
        // savepoint
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1000L)));
        testHarness.processWatermark(new Watermark(1999));

        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 5L, 5L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(4000L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element for [3K, 4K), but should be accumulated into [3K, 5K) [3K, 6K)
        testHarness.processElement(insertRecord("key1", 2, fromEpochMillis(3500L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(5000L)));
        expectedOutput.add(insertRecord("key1", 2L, 1L, localMills(3000L), localMills(5000L)));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(2999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(insertRecord("key1", 2L, 1L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(getNumLateRecordsDroppedCount(operator)).isEqualTo(1);

        testHarness.close();
    }

    @TestTemplate
    void testProcessingTimeCumulativeWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.cumulative(
                        -1, shiftTimeZone, Duration.ofDays(1), Duration.ofHours(8));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, null);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T08:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T08:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T16:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T16:00:00")));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-02T00:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(Long.MAX_VALUE)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-03T08:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T08:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T08:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T16:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T16:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-03T00:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-03T00:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);
    }

    @TestTemplate
    void testEventTimeTumblingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.tumbling(2, shiftTimeZone, Duration.ofSeconds(3));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, null);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(20L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(0L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1998L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1999L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(1000L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(2500L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(2999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 2L, 2L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(getNumLateRecordsDroppedCount(operator)).isEqualTo(2);

        testHarness.close();
    }

    @TestTemplate
    void testProcessingTimeTumblingWindows() throws Exception {

        final SliceAssigner assigner =
                SliceAssigners.tumbling(-1, shiftTimeZone, Duration.ofHours(5));
        // the assigned windows should like as following, e.g. the given timeZone is GMT+08:00:
        //  local windows(timestamp in GMT+08:00)   <=>  epoch windows(timestamp in UTC)
        // [1970-01-01 00:00, 1970-01-01 05:00] <=> [1969-12-31 16:00, 1969-12-31 21:00]
        // [1970-01-01 05:00, 1970-01-01 10:00] <=> [1969-12-31 21:00, 1970-01-01 02:00]

        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);
        OneInputStreamOperator<RowData, RowData> operator =
                buildWindowOperator(assigner, aggsFunction, null);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));

        // timestamp is ignored in processing time
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T05:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));

        testHarness.endInput();
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T10:00:01"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T10:00:00")));

        assertThat(getWatermarkLatency(operator)).isEqualTo(Long.valueOf(0L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @TestTemplate
    void testInvalidWindows() {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SlicingSumAndCountAggsFunction aggsFunction =
                new SlicingSumAndCountAggsFunction(assigner);

        // hopping window without specifying count star index
        assertThatThrownBy(() -> buildWindowOperator(assigner, aggsFunction, null))
                .hasMessageContaining(
                        "Hopping window requires a COUNT(*) in the aggregate functions.");
    }

    /** A test agg function for {@link SlicingWindowAggOperatorTest}. */
    protected static class SlicingSumAndCountAggsFunction
            extends SumAndCountAggsFunctionBase<Long> {

        private final SliceAssigner assigner;

        public SlicingSumAndCountAggsFunction(SliceAssigner assigner) {
            this.assigner = assigner;
        }

        @Override
        protected long getWindowStart(Long window) {
            return assigner.getWindowStart(window);
        }

        @Override
        protected long getWindowEnd(Long window) {
            return window;
        }
    }
}
