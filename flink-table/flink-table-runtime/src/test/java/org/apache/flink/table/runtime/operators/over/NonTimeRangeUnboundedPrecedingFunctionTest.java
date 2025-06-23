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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NonTimeRangeUnboundedPrecedingFunction}. */
public class NonTimeRangeUnboundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    private static final int SORT_KEY_IDX = 1;

    private static final LogicalType[] SORT_KEY_TYPES = new LogicalType[] {new BigIntType()};

    private static final InternalTypeInfo<RowData> INPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, new BigIntType(), new BigIntType());

    private static final RowDataKeySelector SORT_KEY_SELECTOR =
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

        // out of order record should trigger updates for all records after its inserted position
        testHarness.processElement(insertRecord("key1", 4L, 400L));

        List<RowData> expectedRows =
                Arrays.asList(
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 14L),
                        outputRecord(RowKind.INSERT, "key2", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key2", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key1", 4L, 400L, 7L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 12L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 18L));

        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testInsertOnlyRecordsWithDuplicateSortKeys() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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

        // out of order record should trigger updates for all records after its inserted position
        testHarness.processElement(insertRecord("key1", 2L, 203L));
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        testHarness.processElement(insertRecord("key1", 4L, 400L));

        List<RowData> expectedRows =
                Arrays.asList(
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 13L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 18L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 13L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 18L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 18L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 24L),
                        outputRecord(RowKind.INSERT, "key2", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key2", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 203L, 5L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 18L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 18L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 18L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 24L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 7L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 203L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 203L, 7L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 7L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 22L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 22L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 22L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 28L),
                        outputRecord(RowKind.INSERT, "key1", 4L, 400L, 11L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 22L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 22L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 22L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 28L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 32L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractingRecordsWithCustomSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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

        List<RowData> expectedRows =
                Arrays.asList(
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 14L),
                        outputRecord(RowKind.DELETE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 6L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 14L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 12L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 3L, 200L, 4L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 6L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 9L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 12L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 15L),
                        outputRecord(RowKind.INSERT, "key2", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key2", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key3", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 4L, 400L, 8L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 9L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L),
                        outputRecord(RowKind.DELETE, "key1", 3L, 200L, 4L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 8L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 5L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 13L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 19L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 16L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 3L, 300L, 4L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 4L, 400L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 4L, 400L, 8L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 16L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 19L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithFirstDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithMiddleDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithLastDuplicateSortKey() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithDescendingSort() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 2L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 2L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 4L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 4L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 5L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 4L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 9L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 4L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 9L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 6L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 11L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 9L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 9L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 16L),
                        outputRecord(RowKind.DELETE, "key1", 2L, 200L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 16L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 14L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithEarlyOut() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
                        outputRecord(RowKind.INSERT, "key1", 0L, 100L, 0L),
                        outputRecord(RowKind.INSERT, "key1", 0L, 101L, 0L),
                        outputRecord(RowKind.INSERT, "key1", 0L, 102L, 0L),
                        outputRecord(RowKind.INSERT, "key1", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 0L, 100L, 0L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testInsertAndRetractAllWithStateValidation() throws Exception {
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        0,
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
        GenericRowData firstRecord = GenericRowData.of("key1", 1L, 100L);
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        validateState(function, firstRecord, 0, 1, 0, 1, 0, 1, true);

        GenericRowData secondRecord = GenericRowData.of("key1", 2L, 200L);
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        validateState(function, secondRecord, 1, 2, 0, 1, 1, 2, true);

        GenericRowData thirdRecord = GenericRowData.of("key1", 2L, 201L);
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        validateState(function, thirdRecord, 1, 2, 1, 2, 2, 3, true);

        GenericRowData fourthRecord = GenericRowData.of("key1", 5L, 500L);
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        validateState(function, fourthRecord, 2, 3, 0, 1, 3, 4, true);

        GenericRowData fifthRecord = GenericRowData.of("key1", 5L, 502L);
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        validateState(function, fifthRecord, 2, 3, 1, 2, 4, 5, true);

        GenericRowData sixthRecord = GenericRowData.of("key1", 5L, 501L);
        testHarness.processElement(insertRecord("key1", 5L, 501L));
        validateState(function, sixthRecord, 2, 3, 2, 3, 5, 6, true);

        GenericRowData seventhRecord = GenericRowData.of("key1", 6L, 600L);
        testHarness.processElement(insertRecord("key1", 6L, 600L));
        validateState(function, seventhRecord, 3, 4, 0, 1, 6, 7, true);

        testHarness.processElement(updateBeforeRecord("key1", 5L, 502L));
        validateState(function, fifthRecord, 2, 4, 1, 2, 4, 6, false);

        testHarness.processElement(updateBeforeRecord("key1", 6L, 600L));
        validateState(function, seventhRecord, 3, 3, 0, 0, 6, 5, false);

        testHarness.processElement(updateBeforeRecord("key1", 2L, 201L));
        validateState(function, thirdRecord, 1, 3, 1, 1, 2, 4, false);

        testHarness.processElement(updateBeforeRecord("key1", 2L, 200L));
        validateState(function, secondRecord, 1, 2, -1, 0, 1, 3, false);

        testHarness.processElement(updateBeforeRecord("key1", 5L, 500L));
        validateState(function, fourthRecord, 1, 2, 0, 1, 3, 2, false);

        testHarness.processElement(updateBeforeRecord("key1", 5L, 501L));
        validateState(function, sixthRecord, 1, 1, -1, 0, 5, 1, false);

        testHarness.processElement(updateBeforeRecord("key1", 1L, 100L));
        validateState(function, firstRecord, 0, 0, -1, 0, 0, 0, false);

        List<RowData> actualRows = testHarness.extractOutputValues();
        assertThat(actualRows.size()).isEqualTo(40);
        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(0L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    @Test
    public void testInsertWithStateTTLExpiration() throws Exception {
        Duration stateTtlTime = Duration.ofMillis(10);
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        stateTtlTime.toMillis(),
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
        GenericRowData firstRecord = GenericRowData.of("key1", 1L, 100L);
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        validateState(function, firstRecord, 0, 1, 0, 1, 0, 1, true);

        GenericRowData secondRecord = GenericRowData.of("key1", 2L, 200L);
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        validateState(function, secondRecord, 1, 2, 0, 1, 1, 2, true);

        GenericRowData thirdRecord = GenericRowData.of("key1", 2L, 201L);
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        validateState(function, thirdRecord, 1, 2, 1, 2, 2, 3, true);

        // expire the state
        testHarness.setStateTtlProcessingTime(stateTtlTime.toMillis() + 1);

        // After insertion of the following record, there should be only 1 record in state
        // After insertion of the following record, there should be only 1 record in state
        GenericRowData fourthRecord = GenericRowData.of("key1", 5L, 500L);
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        validateState(function, fourthRecord, 0, 1, 0, 1, 0, 1, true);

        List<RowData> actualRows = testHarness.extractOutputValues();
        assertThat(actualRows.size()).isEqualTo(6);

        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(0L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    @Test
    public void testInsertAndRetractWithStateTTLExpiration() throws Exception {
        Duration stateTtlTime = Duration.ofMillis(10);
        NonTimeRangeUnboundedPrecedingFunction<RowData> function =
                new NonTimeRangeUnboundedPrecedingFunction<RowData>(
                        stateTtlTime.toMillis(),
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
        GenericRowData firstRecord = GenericRowData.of("key1", 1L, 100L);
        testHarness.processElement(insertRecord("key1", 1L, 100L));
        validateState(function, firstRecord, 0, 1, 0, 1, 0, 1, true);

        GenericRowData secondRecord = GenericRowData.of("key1", 2L, 200L);
        testHarness.processElement(insertRecord("key1", 2L, 200L));
        validateState(function, secondRecord, 1, 2, 0, 1, 1, 2, true);

        GenericRowData thirdRecord = GenericRowData.of("key1", 2L, 201L);
        testHarness.processElement(insertRecord("key1", 2L, 201L));
        validateState(function, thirdRecord, 1, 2, 1, 2, 2, 3, true);

        GenericRowData fourthRecord = GenericRowData.of("key1", 5L, 500L);
        testHarness.processElement(insertRecord("key1", 5L, 500L));
        validateState(function, fourthRecord, 2, 3, 0, 1, 3, 4, true);

        GenericRowData fifthRecord = GenericRowData.of("key1", 5L, 502L);
        testHarness.processElement(insertRecord("key1", 5L, 502L));
        validateState(function, fifthRecord, 2, 3, 1, 2, 4, 5, true);

        // expire the state
        testHarness.setStateTtlProcessingTime(stateTtlTime.toMillis() + 1);

        // Retract a non-existent record due to state ttl expiration
        testHarness.processElement(updateBeforeRecord("key1", 5L, 502L));

        // Ensure state is null/empty
        List<Tuple2<RowData, List<Long>>> sortedList =
                function.getRuntimeContext().getState(function.sortedListStateDescriptor).value();
        assertThat(sortedList).isNull();
        MapState<RowData, RowData> mapState =
                function.getRuntimeContext().getMapState(function.accStateDescriptor);
        assertThat(mapState.isEmpty()).isTrue();
        Long idValue = function.getRuntimeContext().getState(function.idStateDescriptor).value();
        assertThat(idValue).isNull();

        List<RowData> actualRows = testHarness.extractOutputValues();
        assertThat(actualRows.size()).isEqualTo(9);

        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(1L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    private void validateState(
            NonTimeRangeUnboundedPrecedingFunction<RowData> function,
            RowData record,
            int listPos,
            int expectedSortedListSize,
            int idPos,
            int expectedNumOfIds,
            int idOffset,
            int totalRows,
            boolean isInsertion)
            throws Exception {
        List<Tuple2<RowData, List<Long>>> sortedList =
                function.getRuntimeContext().getState(function.sortedListStateDescriptor).value();
        // Validate sortedList size
        assertThat(sortedList.size()).isEqualTo(expectedSortedListSize);
        if (isInsertion) {
            // Validate number of ids
            assertThat(sortedList.get(listPos).f1.size()).isEqualTo(expectedNumOfIds);
            // Validate if id was inserted in the correct position
            assertThat(sortedList.get(listPos).f1.get(idPos)).isEqualTo(Long.MIN_VALUE + idOffset);
        } else {
            if (listPos < sortedList.size()) {
                Tuple2<RowData, List<Long>> rowDataListTuple = sortedList.get(listPos);
                if (rowDataListTuple != null) {
                    // Validate if ids does not contain the removed id
                    assertThat(rowDataListTuple.f1).doesNotContain(Long.MIN_VALUE + idOffset);
                }
            } else {
                assertThat(
                                function.getRuntimeContext()
                                        .getMapState(function.accStateDescriptor)
                                        .get(SORT_KEY_SELECTOR.getKey(record)))
                        .isNull();
            }
        }

        // Validate total number of rows in the valueMapState
        AtomicInteger numRows = new AtomicInteger();
        function.getRuntimeContext()
                .getMapState(function.valueStateDescriptor)
                .keys()
                .forEach(row -> numRows.getAndIncrement());
        assertThat(numRows.get()).isEqualTo(totalRows);

        if (isInsertion) {
            // Validate if record was successfully inserted in the valueMapState
            assertThat(
                            function.getRuntimeContext()
                                    .getMapState(function.valueStateDescriptor)
                                    .get(Long.MIN_VALUE + idOffset)
                                    .toString())
                    .isEqualTo(record.toString());
        } else {
            // Validate if record was successfully removed from the valueMapState
            assertThat(
                            function.getRuntimeContext()
                                    .getMapState(function.valueStateDescriptor)
                                    .get(Long.MIN_VALUE + idOffset))
                    .isNull();
        }

        // Validate number of entries in the accMap state
        AtomicInteger numAccRows = new AtomicInteger();
        function.getRuntimeContext()
                .getMapState(function.accStateDescriptor)
                .keys()
                .forEach(row -> numAccRows.getAndIncrement());

        assertThat(numAccRows.get()).isEqualTo(expectedSortedListSize);

        if (isInsertion) {
            // Validate if an entry exists for the sortKey
            assertThat(
                            function.getRuntimeContext()
                                    .getMapState(function.accStateDescriptor)
                                    .get(SORT_KEY_SELECTOR.getKey(record)))
                    .isNotNull();
        }
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

    private JoinedRowData outputRecord(
            RowKind rowKind, String key, Long val, Long ts, Long aggVal) {
        return new JoinedRowData(
                rowKind,
                GenericRowData.ofKind(RowKind.INSERT, StringData.fromString(key), val, ts),
                GenericRowData.ofKind(RowKind.INSERT, aggVal));
    }
}
