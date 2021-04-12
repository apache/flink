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

package org.apache.flink.table.runtime.operators.rank.window;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/** Tests for window rank operators created by {@link WindowRankOperatorBuilder}. */
public class WindowRankOperatorTest {

    private static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new IntType()),
                            new RowType.RowField("f2", new BigIntType())));

    private static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);
    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    private static final PagedTypeSerializer<RowData> KEY_SER =
            (PagedTypeSerializer<RowData>) KEY_SELECTOR.getProducedType().toSerializer();

    private static final GeneratedRecordComparator GENERATED_SORT_KEY_COMPARATOR =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return IntRecordComparator.INSTANCE;
                }
            };

    private static final int SORT_KEY_IDX = 1;

    private static final RowDataKeySelector SORT_KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {SORT_KEY_IDX},
                    INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    private static final int WINDOW_END_INDEX = 2;

    private static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE),
                new IntType(),
                new BigIntType(),
                new BigIntType()
            };

    private static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    private static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES,
                    new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

    private static final LogicalType[] OUTPUT_TYPES_WITHOUT_RANK_NUMBER =
            new LogicalType[] {new VarCharType(Integer.MAX_VALUE), new IntType(), new BigIntType()};

    private static final TypeSerializer<RowData> OUT_SERIALIZER_WITHOUT_RANK_NUMBER =
            new RowDataSerializer(OUTPUT_TYPES_WITHOUT_RANK_NUMBER);

    private static final RowDataHarnessAssertor ASSERTER_WITHOUT_RANK_NUMBER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES_WITHOUT_RANK_NUMBER,
                    new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

    @Test
    public void testTop2Windows() throws Exception {
        SlicingWindowOperator<RowData, ?> operator =
                WindowRankOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(UTC_ZONE_ID)
                        .keySerializer(KEY_SER)
                        .sortKeyComparator(GENERATED_SORT_KEY_COMPARATOR)
                        .sortKeySelector(SORT_KEY_SELECTOR)
                        .outputRankNumber(true)
                        .rankStart(1)
                        .rankEnd(2)
                        .windowEndIndex(WINDOW_END_INDEX)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 999L));
        testHarness.processElement(insertRecord("key2", 4, 999L));
        testHarness.processElement(insertRecord("key2", 5, 999L));
        testHarness.processElement(insertRecord("key2", 3, 999L));
        testHarness.processElement(insertRecord("key2", 2, 1999L));
        testHarness.processElement(insertRecord("key2", 7, 3999L));
        testHarness.processElement(insertRecord("key2", 8, 3999L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));

        testHarness.processElement(insertRecord("key1", 2, 999L));
        testHarness.processElement(insertRecord("key1", 1, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 4, 1999L));
        testHarness.processElement(insertRecord("key1", 6, 1999L));
        testHarness.processElement(insertRecord("key1", 7, 1999L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 1, 999L, 1L));
        expectedOutput.add(insertRecord("key1", 2, 999L, 2L));
        expectedOutput.add(insertRecord("key2", 1, 999L, 1L));
        expectedOutput.add(insertRecord("key2", 3, 999L, 2L));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 4, 1999L, 1L));
        expectedOutput.add(insertRecord("key1", 6, 1999L, 2L));
        expectedOutput.add(insertRecord("key2", 2, 1999L, 1L));
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
        expectedOutput.add(insertRecord("key2", 1, 3999L, 1L));
        expectedOutput.add(insertRecord("key2", 7, 3999L, 2L));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(insertRecord("key2", 1, 3500L));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(1, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }

    @Test
    public void testTop2WindowsWithOffset() throws Exception {
        SlicingWindowOperator<RowData, ?> operator =
                WindowRankOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(UTC_ZONE_ID)
                        .keySerializer(KEY_SER)
                        .sortKeyComparator(GENERATED_SORT_KEY_COMPARATOR)
                        .sortKeySelector(SORT_KEY_SELECTOR)
                        .outputRankNumber(true)
                        .rankStart(2)
                        .rankEnd(2)
                        .windowEndIndex(WINDOW_END_INDEX)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 999L));
        testHarness.processElement(insertRecord("key2", 4, 999L));
        testHarness.processElement(insertRecord("key2", 5, 999L));
        testHarness.processElement(insertRecord("key2", 3, 999L));
        testHarness.processElement(insertRecord("key2", 2, 1999L));
        testHarness.processElement(insertRecord("key2", 7, 3999L));
        testHarness.processElement(insertRecord("key2", 8, 3999L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));

        testHarness.processElement(insertRecord("key1", 2, 999L));
        testHarness.processElement(insertRecord("key1", 1, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 4, 1999L));
        testHarness.processElement(insertRecord("key1", 6, 1999L));
        testHarness.processElement(insertRecord("key1", 7, 1999L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 2, 999L, 2L));
        expectedOutput.add(insertRecord("key2", 3, 999L, 2L));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 6, 1999L, 2L));
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
        expectedOutput.add(insertRecord("key2", 7, 3999L, 2L));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTop2WindowsWithoutRankNumber() throws Exception {
        SlicingWindowOperator<RowData, ?> operator =
                WindowRankOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(UTC_ZONE_ID)
                        .keySerializer(KEY_SER)
                        .sortKeyComparator(GENERATED_SORT_KEY_COMPARATOR)
                        .sortKeySelector(SORT_KEY_SELECTOR)
                        .outputRankNumber(false)
                        .rankStart(1)
                        .rankEnd(2)
                        .windowEndIndex(WINDOW_END_INDEX)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER_WITHOUT_RANK_NUMBER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 999L));
        testHarness.processElement(insertRecord("key2", 4, 999L));
        testHarness.processElement(insertRecord("key2", 5, 999L));
        testHarness.processElement(insertRecord("key2", 3, 999L));
        testHarness.processElement(insertRecord("key2", 2, 1999L));
        testHarness.processElement(insertRecord("key2", 7, 3999L));
        testHarness.processElement(insertRecord("key2", 8, 3999L));
        testHarness.processElement(insertRecord("key2", 1, 3999L));

        testHarness.processElement(insertRecord("key1", 2, 999L));
        testHarness.processElement(insertRecord("key1", 1, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 3, 999L));
        testHarness.processElement(insertRecord("key1", 4, 1999L));
        testHarness.processElement(insertRecord("key1", 6, 1999L));
        testHarness.processElement(insertRecord("key1", 7, 1999L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 1, 999L));
        expectedOutput.add(insertRecord("key1", 2, 999L));
        expectedOutput.add(insertRecord("key2", 1, 999L));
        expectedOutput.add(insertRecord("key2", 3, 999L));
        expectedOutput.add(new Watermark(999));
        ASSERTER_WITHOUT_RANK_NUMBER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 4, 1999L));
        expectedOutput.add(insertRecord("key1", 6, 1999L));
        expectedOutput.add(insertRecord("key2", 2, 1999L));
        expectedOutput.add(new Watermark(1999));
        ASSERTER_WITHOUT_RANK_NUMBER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER_WITHOUT_RANK_NUMBER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 1, 3999L));
        expectedOutput.add(insertRecord("key2", 7, 3999L));
        expectedOutput.add(new Watermark(3999));
        ASSERTER_WITHOUT_RANK_NUMBER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            SlicingWindowOperator<RowData, ?> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR.getProducedType());
    }
}
