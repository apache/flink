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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
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

/** Test for {@link UnalignedWindowTableFunctionOperator}. */
@RunWith(Parameterized.class)
public class UnalignedWindowTableFunctionOperatorTest extends WindowTableFunctionOperatorTestBase {

    public UnalignedWindowTableFunctionOperatorTest(ZoneId shiftTimeZone) {
        super(shiftTimeZone);
    }

    @Test
    public void testEventTimeSessionWindows() throws Exception {
        final SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofSeconds(3));
        UnalignedWindowTableFunctionOperator operator = createOperator(assigner, ROW_TIME_INDEX);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new Watermark(2999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(insertRecord("key1", 1, 20L, localMills(20L), localMills(3020L), 3019L));
        expectedOutput.add(new Watermark(3999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // the window end is 5000L, so it's not a late record
        testHarness.processElement(insertRecord("key1", 1, 2000L));
        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(
                insertRecord("key1", 1, 2000L, localMills(2000L), localMills(5000L), 4999L));
        expectedOutput.add(new Watermark(4999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // test out-of-order records
        testHarness.processElement(insertRecord("key2", 2, 7999L));
        testHarness.processElement(insertRecord("key2", 3, 5999L));
        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new Watermark(5999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(6999));
        expectedOutput.add(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(7999));
        testHarness.processWatermark(new Watermark(8999));
        expectedOutput.add(new Watermark(8999));
        testHarness.processWatermark(new Watermark(9999));
        expectedOutput.add(new Watermark(9999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(10999));

        // late record should be dropped
        testHarness.processElement(insertRecord("key1", 1, 999L));

        // rowtime is null, should be dropped
        testHarness.processElement(insertRecord("key1", 1, ((Long) null)));

        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(3999L), localMills(10999L), 10998L));
        expectedOutput.add(
                insertRecord("key2", 2, 7999L, localMills(3999L), localMills(10999L), 10998L));
        expectedOutput.add(
                insertRecord("key2", 3, 5999L, localMills(3999L), localMills(10999L), 10998L));
        expectedOutput.add(new Watermark(10999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(operator.getNumLateRecordsDropped().getCount()).isEqualTo(1);
        assertThat(operator.getNumNullRowTimeRecordsDropped().getCount()).isEqualTo(1);

        testHarness.close();
    }

    @Test
    public void testEventTimeSessionWindowsWithChangelog() throws Exception {
        final SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofSeconds(3));
        UnalignedWindowTableFunctionOperator operator = createOperator(assigner, ROW_TIME_INDEX);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, 20L));
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key2", 1, 1999L));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key3", 1, 2999L));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key4", 1, 1999L));
        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(binaryRecord(RowKind.DELETE, "key4", 1, 2999L));

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));

        testHarness.processElement(binaryRecord(RowKind.UPDATE_BEFORE, "key3", 1, 3999L));

        testHarness.processElement(binaryRecord(RowKind.UPDATE_AFTER, "key3", 1, 4999L));

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new Watermark(2999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT, "key1", 1, 20L, localMills(20L), localMills(3020L), 3019L));
        expectedOutput.add(new Watermark(3999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(
                binaryRecord(
                        RowKind.DELETE,
                        "key2",
                        1,
                        1999L,
                        localMills(1999L),
                        localMills(4999L),
                        4998L));
        expectedOutput.add(new Watermark(4999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key4",
                        1,
                        1999L,
                        localMills(1999L),
                        localMills(5999L),
                        5998L));
        expectedOutput.add(
                binaryRecord(
                        RowKind.DELETE,
                        "key4",
                        1,
                        2999L,
                        localMills(1999L),
                        localMills(5999L),
                        5998L));
        expectedOutput.add(new Watermark(5999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(7999));

        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key3",
                        1,
                        2999L,
                        localMills(2999L),
                        localMills(7999L),
                        7998L));
        expectedOutput.add(
                binaryRecord(
                        RowKind.UPDATE_BEFORE,
                        "key3",
                        1,
                        3999L,
                        localMills(2999L),
                        localMills(7999L),
                        7998L));
        expectedOutput.add(
                binaryRecord(
                        RowKind.UPDATE_AFTER,
                        "key3",
                        1,
                        4999L,
                        localMills(2999L),
                        localMills(7999L),
                        7998L));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testProcessTimeSessionWindows() throws Exception {
        final SessionWindowAssigner assigner =
                SessionWindowAssigner.withGap(Duration.ofSeconds(3)).withProcessingTime();
        UnalignedWindowTableFunctionOperator operator = createOperator(assigner, -1);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(20L);
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1999);
        testHarness.setProcessingTime(2999);

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(3999L);
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));

        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord("key1", 1, Long.MAX_VALUE, localMills(20L), localMills(3020L), 3019L));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(4999);

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // test records with second field out of order
        testHarness.setProcessingTime(5999);
        testHarness.processElement(insertRecord("key2", 3, Long.MAX_VALUE));

        testHarness.setProcessingTime(7999);
        testHarness.processElement(insertRecord("key2", 2, Long.MAX_VALUE));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(6999);
        testHarness.setProcessingTime(7999);
        testHarness.setProcessingTime(8999);
        testHarness.setProcessingTime(9999);

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(10999);

        expectedOutput.add(
                insertRecord(
                        "key2", 1, Long.MAX_VALUE, localMills(3999L), localMills(10999L), 10998L));
        expectedOutput.add(
                insertRecord(
                        "key2", 3, Long.MAX_VALUE, localMills(3999L), localMills(10999L), 10998L));
        expectedOutput.add(
                insertRecord(
                        "key2", 2, Long.MAX_VALUE, localMills(3999L), localMills(10999L), 10998L));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(operator.getWatermarkLatency().getValue()).isEqualTo(Long.valueOf(0L));

        testHarness.close();
    }

    @Test
    public void testProcessTimeSessionWindowsWithChangelog() throws Exception {
        final SessionWindowAssigner assigner =
                SessionWindowAssigner.withGap(Duration.ofSeconds(3)).withProcessingTime();
        UnalignedWindowTableFunctionOperator operator = createOperator(assigner, -1);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(20L);
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, Long.MAX_VALUE));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, Long.MAX_VALUE));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1999);
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key3", 1, Long.MAX_VALUE));
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key2", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(2999);

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(3999L);

        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key1",
                        1,
                        Long.MAX_VALUE,
                        localMills(20L),
                        localMills(3020L),
                        3019L));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(4999);

        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key2",
                        1,
                        Long.MAX_VALUE,
                        localMills(20L),
                        localMills(4999L),
                        4998L));

        expectedOutput.add(
                binaryRecord(
                        RowKind.DELETE,
                        "key3",
                        1,
                        Long.MAX_VALUE,
                        localMills(1999L),
                        localMills(4999L),
                        4998L));

        expectedOutput.add(
                binaryRecord(
                        RowKind.DELETE,
                        "key2",
                        1,
                        Long.MAX_VALUE,
                        localMills(20L),
                        localMills(4999L),
                        4998L));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    public void testSessionWindowsWithoutPartitionKeys() throws Exception {
        final SessionWindowAssigner assigner = SessionWindowAssigner.withGap(Duration.ofSeconds(3));
        UnalignedWindowTableFunctionOperator operator = createOperator(assigner, ROW_TIME_INDEX);

        final EmptyRowDataKeySelector keySelector = EmptyRowDataKeySelector.INSTANCE;

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector.getProducedType());
        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(insertRecord("key1", 1, 1999L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new Watermark(3999));
        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new Watermark(5999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        expectedOutput.clear();
        testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector.getProducedType());
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(6999));

        // append 3 fields: window_start, window_end, window_time
        expectedOutput.add(
                insertRecord("key1", 1, 1999L, localMills(1999L), localMills(6999L), 6998L));
        expectedOutput.add(
                insertRecord("key2", 1, 3999L, localMills(1999L), localMills(6999L), 6998L));
        expectedOutput.add(new Watermark(6999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    protected static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            UnalignedWindowTableFunctionOperator operator) throws Exception {

        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR.getProducedType());
    }

    private UnalignedWindowTableFunctionOperator createOperator(
            GroupWindowAssigner<TimeWindow> windowAssigner, int rowTimeIndex) {
        return new UnalignedWindowTableFunctionOperator(
                windowAssigner,
                windowAssigner.getWindowSerializer(new ExecutionConfig()),
                new RowDataSerializer(INPUT_ROW_TYPE),
                rowTimeIndex,
                shiftTimeZone);
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }
}
