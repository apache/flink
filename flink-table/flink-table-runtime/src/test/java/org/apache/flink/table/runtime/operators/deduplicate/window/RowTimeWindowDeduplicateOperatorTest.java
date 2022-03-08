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

package org.apache.flink.table.runtime.operators.deduplicate.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.junit.Assert.assertEquals;

/**
 * Tests for window deduplicate operators created by {@link
 * RowTimeWindowDeduplicateOperatorBuilder}.
 */
@RunWith(Parameterized.class)
public class RowTimeWindowDeduplicateOperatorTest {

    private static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new BigIntType()),
                            new RowType.RowField("f2", new BigIntType())));

    private static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    private static final PagedTypeSerializer<RowData> KEY_SER =
            (PagedTypeSerializer<RowData>) KEY_SELECTOR.getProducedType().toSerializer();

    private static final int WINDOW_END_INDEX = 2;

    private static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE), new BigIntType(), new BigIntType()
            };

    private static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    private static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES, new GenericRowRecordSortComparator(0, VarCharType.STRING_TYPE));

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    private static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");
    private final ZoneId shiftTimeZone;

    public RowTimeWindowDeduplicateOperatorTest(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            SlicingWindowOperator<RowData, ?> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR.getProducedType());
    }

    @Test
    public void testRowTimeWindowDeduplicateKeepFirstRow() throws Exception {
        SlicingWindowOperator<RowData, ?> operator =
                RowTimeWindowDeduplicateOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .keepLastRow(false)
                        .rowtimeIndex(1)
                        .windowEndIndex(WINDOW_END_INDEX)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(
                insertRecord("key2", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 4L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 5L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 1002L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3007L, toUtcTimestampMills(3999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3008L, toUtcTimestampMills(3999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3001L, toUtcTimestampMills(3999L, shiftTimeZone)));

        testHarness.processElement(
                insertRecord("key1", 2L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1004L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1006L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1007L, toUtcTimestampMills(1999L, shiftTimeZone)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        expectedOutput.add(insertRecord("key2", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 1004L, toUtcTimestampMills(1999L, shiftTimeZone)));
        expectedOutput.add(insertRecord("key2", 1002L, toUtcTimestampMills(1999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 3001L, toUtcTimestampMills(3999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(
                insertRecord("key2", 3001L, toUtcTimestampMills(3500L, shiftTimeZone)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(1, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }

    @Test
    public void testRowTimeWindowDeduplicateKeepLastRow() throws Exception {
        SlicingWindowOperator<RowData, ?> operator =
                RowTimeWindowDeduplicateOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .keepLastRow(true)
                        .rowtimeIndex(1)
                        .windowEndIndex(WINDOW_END_INDEX)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(
                insertRecord("key2", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 4L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 5L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 1002L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3007L, toUtcTimestampMills(3999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3008L, toUtcTimestampMills(3999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key2", 3001L, toUtcTimestampMills(3999L, shiftTimeZone)));

        testHarness.processElement(
                insertRecord("key1", 2L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1004L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1006L, toUtcTimestampMills(1999L, shiftTimeZone)));
        testHarness.processElement(
                insertRecord("key1", 1007L, toUtcTimestampMills(1999L, shiftTimeZone)));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 3L, toUtcTimestampMills(999L, shiftTimeZone)));
        expectedOutput.add(insertRecord("key2", 5L, toUtcTimestampMills(999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 1007L, toUtcTimestampMills(1999L, shiftTimeZone)));
        expectedOutput.add(insertRecord("key2", 1002L, toUtcTimestampMills(1999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 3008L, toUtcTimestampMills(3999L, shiftTimeZone)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(
                insertRecord("key2", 3001L, toUtcTimestampMills(3500L, shiftTimeZone)));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(1, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }
}
