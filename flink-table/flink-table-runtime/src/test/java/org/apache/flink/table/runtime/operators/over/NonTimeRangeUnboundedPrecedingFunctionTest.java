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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NonTimeRangeUnboundedPrecedingFunction}. */
public class NonTimeRangeUnboundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    private static final int SORT_KEY_IDX = 1;

    protected LogicalType[] SORT_KEY_TYPES = new LogicalType[] {new BigIntType()};

    private static final InternalTypeInfo<RowData> INPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, new BigIntType(), new IntType());

    private RowDataKeySelector SORT_KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {SORT_KEY_IDX}, INPUT_ROW_TYPE.toRowFieldTypes());

    private static final GeneratedRecordComparator GENERATED_SORT_KEY_COMPARATOR_ASC =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return new LongRecordComparator(0, true);
                }
            };

    private static final GeneratedRecordComparator GENERATED_SORT_KEY_COMPARATOR_DESC =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return new LongRecordComparator(0, false);
                }
            };

    private static final GeneratedRecordEqualiser GENERATED_ROW_VALUE_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRowValueEqualiser();
                }
            };

    private static final GeneratedRecordEqualiser GENERATED_SORT_KEY_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestSortKeyEqualiser();
                }
            };

    /** Custom test comparator for comparing numbers. */
    public static class LongRecordComparator implements RecordComparator {

        private int pos;
        private boolean isAscending;

        public LongRecordComparator(int pos, boolean isAscending) {
            this.pos = pos;
            this.isAscending = isAscending;
        }

        @Override
        public int compare(RowData o1, RowData o2) {
            boolean null0At1 = o1.isNullAt(pos);
            boolean null0At2 = o2.isNullAt(pos);
            int cmp0 =
                    null0At1 && null0At2
                            ? 0
                            : (null0At1
                                    ? -1
                                    : (null0At2
                                            ? 1
                                            : Long.compare(o1.getLong(pos), o2.getLong(pos))));
            if (cmp0 != 0) {
                if (isAscending) {
                    return cmp0;
                } else {
                    return -cmp0;
                }
            }
            return 0;
        }
    }

    /** Custom test row equaliser for comparing rows. */
    public static class TestRowValueEqualiser implements RecordEqualiser {

        private static final long serialVersionUID = -6706336100425614942L;

        @Override
        public boolean equals(RowData row1, RowData row2) {
            if (row1 instanceof BinaryRowData && row2 instanceof BinaryRowData) {
                return row1.equals(row2);
            } else if (row1 instanceof GenericRowData && row2 instanceof GenericRowData) {
                return row1.getString(0).equals(row2.getString(0))
                        && row1.getLong(1) == row2.getLong(1)
                        && row1.getLong(2) == row2.getLong(2);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    /** Custom test sortKey equaliser for comparing sort keys. */
    public static class TestSortKeyEqualiser implements RecordEqualiser {

        private static final long serialVersionUID = -6706336100425614942L;

        @Override
        public boolean equals(RowData row1, RowData row2) {
            if (row1 instanceof BinaryRowData && row2 instanceof BinaryRowData) {
                return row1.equals(row2);
            } else if (row1 instanceof GenericRowData && row2 instanceof GenericRowData) {
                return row1.getLong(0) == row2.getLong(0);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Test
    public void testInsertOnlyRecordsWithCustomSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(insertRecord("key2", 1L, 100L));
        testHarness.processElement(insertRecord("key2", 2L, 200L));

        testHarness.processWatermark(new Watermark(500L));

        // out of order record should trigger updates for all records after its inserted position
        testHarness.processElement(insertRecord("key1", 4L, 400L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 14L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 7L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 12L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 14L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testInsertOnlyRecordsWithDuplicateSortKeys() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(insertRecord("key2", 1L, 100L));
        testHarness.processElement(insertRecord("key2", 2L, 200L));

        testHarness.processWatermark(new Watermark(500L));

        // out of order record should trigger updates for all records after its inserted position
        testHarness.processElement(insertRecord("key1", 2L, 203L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 4L, 400L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 502L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 501L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 24L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 203L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 18L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 24L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 7L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 203L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 203L),
                                GenericRowData.ofKind(RowKind.INSERT, 7L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 7L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 28L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 11L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 22L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 28L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 32L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractingRecordsWithCustomSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 2L, 200L));
        testHarness.processElement(updateAfterRecord("key1", 3L, 200L));
        testHarness.processElement(insertRecord("key2", 1L, 100L));
        testHarness.processElement(insertRecord("key2", 2L, 200L));
        testHarness.processElement(insertRecord("key3", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 4L, 400L));
        testHarness.processElement(updateBeforeRecord("key1", 3L, 200L));
        testHarness.processElement(updateAfterRecord("key1", 3L, 300L));

        // Watermark has no impact and should be ignored
        testHarness.processWatermark(new Watermark(500L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 14L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 6L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 14L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 12L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        3L,
                                        200L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 6L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 12L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key2"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key3"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 19L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 3L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 19L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 16L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        3L,
                                        300L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 4L, 400L),
                                GenericRowData.ofKind(RowKind.INSERT, 8L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 16L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 19L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithFirstDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 5L, 500L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 21L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithMiddleDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 5L, 502L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 21L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithLastDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 5L, 501L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        6L,
                                        600L),
                                GenericRowData.ofKind(RowKind.INSERT, 21L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithDescendingSort() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_DESC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 2L, 200L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 2L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 2L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 4L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 6L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 11L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 9L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 16L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 13L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 16L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 14L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithEarlyOut() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
                        2000,
                        aggsHandleFunction,
                        GENERATED_ROW_VALUE_EQUALISER,
                        GENERATED_SORT_KEY_EQUALISER,
                        GENERATED_SORT_KEY_COMPARATOR_ASC,
                        accTypes,
                        inputFieldTypes,
                        SORT_KEY_TYPES,
                        SORT_KEY_SELECTOR) {};
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.open();

        // put some records
        testHarness.processElement(insertRecord("key1", 0L, 100L));
        testHarness.processElement(insertRecord("key1", 0L, 101L));
        testHarness.processElement(insertRecord("key1", 0L, 102L));
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        testHarness.processElement(updateBeforeRecord("key1", 0L, 100L));

        List<RowData> expectedRows =
                Arrays.asList(
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 0L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 0L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 0L, 101L),
                                GenericRowData.ofKind(RowKind.INSERT, 0L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 0L, 102L),
                                GenericRowData.ofKind(RowKind.INSERT, 0L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 1L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 1L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 3L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 200L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 2L, 201L),
                                GenericRowData.ofKind(RowKind.INSERT, 5L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 10L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        500L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_BEFORE,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 15L)),
                        new JoinedRowData(
                                RowKind.UPDATE_AFTER,
                                GenericRowData.ofKind(
                                        RowKind.UPDATE_AFTER,
                                        StringData.fromString("key1"),
                                        5L,
                                        502L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 5L, 501L),
                                GenericRowData.ofKind(RowKind.INSERT, 20L)),
                        new JoinedRowData(
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 6L, 600L),
                                GenericRowData.ofKind(RowKind.INSERT, 26L)),
                        new JoinedRowData(
                                RowKind.UPDATE_BEFORE,
                                GenericRowData.ofKind(
                                        RowKind.INSERT, StringData.fromString("key1"), 0L, 100L),
                                GenericRowData.ofKind(RowKind.INSERT, 0L)));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    private void validateRows(List<RowData> actualRows, List<RowData> expectedRows) {
        // Validate size of rows emitted
        assertThat(actualRows.size()).isEqualTo(expectedRows.size());

        // Validate the contents of rows emitted
        for (int i = 0; i < actualRows.size(); i++) {
            assertThat(actualRows.get(i).getRowKind()).isEqualTo(expectedRows.get(i).getRowKind());
            assertThat(actualRows.get(i).getString(0)).isEqualTo(expectedRows.get(i).getString(0));
            assertThat(actualRows.get(i).getLong(1)).isEqualTo(expectedRows.get(i).getLong(1));
            assertThat(actualRows.get(i).getLong(2)).isEqualTo(expectedRows.get(i).getLong(2));
            // Aggregated value
            assertThat(actualRows.get(i).getLong(3)).isEqualTo(expectedRows.get(i).getLong(3));
        }
    }
}
