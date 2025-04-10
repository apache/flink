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

import org.apache.flink.api.java.tuple.Tuple2;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Base class for non-time over window test. */
public abstract class NonTimeOverWindowTestBase extends RowTimeOverWindowTestBase {

    private static final int SORT_KEY_IDX = 1;

    static final LogicalType[] SORT_KEY_TYPES = new LogicalType[] {new BigIntType()};

    private static final InternalTypeInfo<RowData> INPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, new BigIntType(), new BigIntType());

    static final RowDataKeySelector SORT_KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {SORT_KEY_IDX}, INPUT_ROW_TYPE.toRowFieldTypes());

    static final GeneratedRecordComparator GENERATED_SORT_KEY_COMPARATOR_ASC =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return new LongRecordComparator(0, true);
                }
            };

    static final GeneratedRecordComparator GENERATED_SORT_KEY_COMPARATOR_DESC =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return new LongRecordComparator(0, false);
                }
            };

    static final GeneratedRecordEqualiser GENERATED_ROW_VALUE_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                private static final long serialVersionUID = 1L;

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRowValueEqualiser();
                }
            };

    static final GeneratedRecordEqualiser GENERATED_SORT_KEY_EQUALISER =
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

    abstract void validateEntry(
            AbstractNonTimeUnboundedPrecedingOver<RowData> function, RowData record, int idOffset)
            throws Exception;

    abstract void validateNumAccRows(int numAccRows, int expectedSortedListSize, int totalRows);

    /** Helper method to validate state contents. */
    void validateState(
            AbstractNonTimeUnboundedPrecedingOver<RowData> function,
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

        validateNumAccRows(numAccRows.get(), expectedSortedListSize, totalRows);

        if (isInsertion) {
            validateEntry(function, record, idOffset);
        }
    }

    /** Helper method to validate rows emitted. */
    void validateRows(List<RowData> actualRows, List<RowData> expectedRows) {
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

    /** Helper method to create a new record. */
    JoinedRowData outputRecord(RowKind rowKind, String key, Long val, Long ts, Long aggVal) {
        return new JoinedRowData(
                rowKind,
                GenericRowData.ofKind(RowKind.INSERT, StringData.fromString(key), val, ts),
                GenericRowData.ofKind(RowKind.INSERT, aggVal));
    }
}
