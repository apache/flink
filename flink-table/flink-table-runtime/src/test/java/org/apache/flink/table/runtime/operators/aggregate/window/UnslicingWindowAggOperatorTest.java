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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceAssigners;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for unslicing window aggregate operators created by {@link WindowAggOperatorBuilder}. */
@RunWith(Parameterized.class)
public class UnslicingWindowAggOperatorTest extends WindowAggOperatorTestBase {

    public UnslicingWindowAggOperatorTest(ZoneId shiftTimeZone) {
        super(shiftTimeZone);
    }

    @Test
    public void testEventTimeSessionWindows() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(2, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        WindowAggOperator<RowData, ?> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(createGeneratedAggsHandle(aggsFunction), ACC_SER)
                        .build();

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
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3999L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3500L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(6999));
        expectedOutput.add(insertRecord("key2", 6L, 6L, localMills(1000L), localMills(6999L)));
        expectedOutput.add(new Watermark(6999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(7999));
        testHarness.processWatermark(new Watermark(8999));
        expectedOutput.add(new Watermark(7999));
        expectedOutput.add(new Watermark(8999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(operator.getNumLateRecordsDropped().getCount()).isEqualTo(1);

        testHarness.close();
    }

    @Test
    public void testEventTimeSessionWindowsWithChangelog() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(2, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        WindowAggOperator<RowData, ?> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(createGeneratedAggsHandle(aggsFunction), ACC_SER)
                        .countStarIndex(1)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(3999L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(3000L)));

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(20L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(0L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(999L)));

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(1998L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(1999L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(1000L)));

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key3", 1, fromEpochMillis(1999L)));

        testHarness.processElement(binaryRecord(RowKind.DELETE, "key4", 1, fromEpochMillis(999L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(binaryRecord(RowKind.DELETE, "key3", 1, fromEpochMillis(2900L)));
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
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // TODO split the session if necessary when receiving -D after fixing FLINK-34039
        // session window does not re-split correctly as expected when consuming -D, only acc is
        // retracted
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key2", 1, fromEpochMillis(3999L)));
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key2", 1, fromEpochMillis(3000L)));

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(
                binaryRecord(RowKind.INSERT, "key1", 3L, 3L, localMills(0L), localMills(3999L)));
        // TODO align the behavior when receiving single -D within a window after fixing FLINK-33760
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT, "key4", null, -1L, localMills(999L), localMills(3999L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(3500L)));
        testHarness.processElement(
                binaryRecord(RowKind.UPDATE_BEFORE, "key2", 1, fromEpochMillis(3000L)));
        testHarness.processElement(
                binaryRecord(RowKind.UPDATE_AFTER, "key2", 1, fromEpochMillis(3800L)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(999L)));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(6999));
        expectedOutput.add(
                binaryRecord(RowKind.INSERT, "key2", 4L, 4L, localMills(1000L), localMills(6999L)));
        expectedOutput.add(new Watermark(6999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(7999));
        testHarness.processWatermark(new Watermark(8999));
        expectedOutput.add(new Watermark(7999));
        expectedOutput.add(new Watermark(8999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertThat(operator.getNumLateRecordsDropped().getCount()).isEqualTo(1);

        testHarness.close();
    }

    @Test
    public void testProcessingTimeSessionWindows() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(-1, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        WindowAggOperator<RowData, ?> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(createGeneratedAggsHandle(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:03"));

        // timestamp is ignored in processing time
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(7000L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:06"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:07"));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:08"));
        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(7000L)));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:12"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:07"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:11")));

        assertThat(operator.getWatermarkLatency().getValue()).isEqualTo(Long.valueOf(0L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testProcessingTimeSessionWindowsWithChangelog() throws Exception {
        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(-1, shiftTimeZone, Duration.ofSeconds(3));

        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        WindowAggOperator<RowData, ?> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(createGeneratedAggsHandle(aggsFunction), ACC_SER)
                        .countStarIndex(1)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:03"));

        // timestamp is ignored in processing time
        testHarness.processElement(
                binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(Long.MAX_VALUE)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(7000L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key2", 1, fromEpochMillis(7000L)));

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(7000L)));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(7000L)));

        testHarness.processElement(binaryRecord(RowKind.INSERT, "key3", 1, fromEpochMillis(7000L)));
        testHarness.processElement(binaryRecord(RowKind.DELETE, "key3", 1, fromEpochMillis(7000L)));

        testHarness.processElement(binaryRecord(RowKind.DELETE, "key4", 1, fromEpochMillis(7000L)));

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:06"));

        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));

        // TODO align the behavior when receiving single -D within a window after fixing FLINK-33760
        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key4",
                        null,
                        -1L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:03"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:06")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:07"));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(7000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:08"));
        testHarness.processElement(binaryRecord(RowKind.INSERT, "key1", 1, fromEpochMillis(8000L)));

        // TODO split the session if necessary when receiving -D after fixing FLINK-34039
        // session window does not re-split correctly as expected when consuming -D, only acc is
        // retracted
        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:10"));
        testHarness.processElement(
                binaryRecord(RowKind.UPDATE_BEFORE, "key1", 1, fromEpochMillis(8000L)));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:11"));
        testHarness.processElement(
                binaryRecord(RowKind.UPDATE_AFTER, "key1", 1, fromEpochMillis(6000L)));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:12"));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:13"));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:14"));

        expectedOutput.add(
                binaryRecord(
                        RowKind.INSERT,
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:07"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:14")));

        assertThat(operator.getWatermarkLatency().getValue()).isEqualTo(Long.valueOf(0L));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testSessionWindowsWithoutPartitionKey() throws Exception {
        // there is no key (type string) in the output
        final LogicalType[] outputTypes =
                new LogicalType[] {
                    new BigIntType(), new BigIntType(), new BigIntType(), new BigIntType()
                };
        final RowDataHarnessAssertor asserter =
                new RowDataHarnessAssertor(
                        outputTypes,
                        new GenericRowRecordSortComparator(0, VarCharType.STRING_TYPE));

        final UnsliceAssigner<TimeWindow> assigner =
                UnsliceAssigners.session(2, shiftTimeZone, Duration.ofSeconds(3));

        final EmptyRowDataKeySelector keySelector = EmptyRowDataKeySelector.INSTANCE;
        final UnslicingSumAndCountAggsFunction aggsFunction =
                new UnslicingSumAndCountAggsFunction();
        WindowAggOperator<RowData, ?> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        keySelector.getProducedType().toSerializer())
                        .assigner(assigner)
                        .aggregate(createGeneratedAggsHandle(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector.getProducedType());

        testHarness.setup(new RowDataSerializer(outputTypes));
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements with multi keys
        testHarness.processElement(insertRecord("key2", 1, fromEpochMillis(3999L)));

        testHarness.processElement(insertRecord("key1", 1, fromEpochMillis(20L)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        asserter.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key3", 1, fromEpochMillis(2990L)));

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        asserter.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertThat(aggsFunction.closeCalled.get()).as("Close was not called.").isGreaterThan(0);

        expectedOutput.clear();
        testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector.getProducedType());
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(6999));
        expectedOutput.add(insertRecord(3L, 3L, localMills(20L), localMills(6999L)));
        expectedOutput.add(new Watermark(6999));
        asserter.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    /** A test agg function for {@link UnslicingWindowAggOperatorTest}. */
    protected static class UnslicingSumAndCountAggsFunction
            extends SumAndCountAggsFunctionBase<TimeWindow> {

        @Override
        protected long getWindowStart(TimeWindow window) {
            return window.getStart();
        }

        @Override
        protected long getWindowEnd(TimeWindow window) {
            return window.getEnd();
        }
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }
}
