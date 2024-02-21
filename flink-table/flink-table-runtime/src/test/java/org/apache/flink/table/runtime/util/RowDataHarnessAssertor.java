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

package org.apache.flink.table.runtime.util;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for working with the various window test harnesses. */
public class RowDataHarnessAssertor {

    private final LogicalType[] types;
    private final Comparator<GenericRowData> comparator;

    public RowDataHarnessAssertor(LogicalType[] types, Comparator<GenericRowData> comparator) {
        this.types = types;
        this.comparator = comparator;
    }

    public RowDataHarnessAssertor(LogicalType[] types) {
        this(types, new StringComparator());
    }

    /** Assert the test harness should not emit any records. */
    public void shouldEmitNothing(AbstractStreamOperatorTestHarness<RowData> harness) {
        assertThat(getEmittedRows(harness)).isEmpty();
    }

    /** Assert the test harness should emit records exactly same as the expected records. */
    public void shouldEmit(
            AbstractStreamOperatorTestHarness<RowData> harness, RowData... expected) {
        assertThat(getEmittedRows(harness)).containsExactly(expected);
    }

    /**
     * Compare the two queues containing operator/task output by converting them to an array first.
     * Asserts two converted array should be same.
     */
    public void assertOutputEquals(
            String message, Collection<Object> expected, Collection<Object> actual) {
        assertOutputEquals(message, expected, actual, false);
    }

    /**
     * Compare the two queues containing operator/task output by converting them to an array first,
     * sort array by comparator. Assertes two sorted converted array should be same.
     */
    public void assertOutputEqualsSorted(
            String message, Collection<Object> expected, Collection<Object> actual) {
        assertOutputEquals(message, expected, actual, true);
    }

    private List<RowData> getEmittedRows(AbstractStreamOperatorTestHarness<RowData> harness) {
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[types.length];
        for (int i = 0; i < types.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(types[i], i);
        }
        final List<RowData> rows = new ArrayList<>();
        Object o;
        while ((o = harness.getOutput().poll()) != null) {
            RowData value = (RowData) ((StreamRecord<?>) o).getValue();
            Object[] row = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                row[i] = fieldGetters[i].getFieldOrNull(value);
            }
            GenericRowData newRow = GenericRowData.of(row);
            newRow.setRowKind(value.getRowKind());
            rows.add(newRow);
        }
        return rows;
    }

    private void assertOutputEquals(
            String message,
            Collection<Object> expected,
            Collection<Object> actual,
            boolean needSort) {
        if (needSort) {
            Preconditions.checkArgument(comparator != null, "Comparator should not be null!");
        }
        assertThat(actual).hasSize(expected.size());

        // first, compare only watermarks, their position should be deterministic
        Iterator<Object> exIt = expected.iterator();
        Iterator<Object> actIt = actual.iterator();
        while (exIt.hasNext()) {
            Object nextEx = exIt.next();
            Object nextAct = actIt.next();
            if (nextEx instanceof Watermark) {
                assertThat(nextAct).isEqualTo(nextEx);
            }
        }

        List<GenericRowData> expectedRecords = new ArrayList<>();
        List<GenericRowData> actualRecords = new ArrayList<>();

        for (Object ex : expected) {
            if (ex instanceof StreamRecord) {
                RowData row = (RowData) ((StreamRecord) ex).getValue();
                if (row instanceof GenericRowData) {
                    expectedRecords.add((GenericRowData) row);
                } else {
                    GenericRowData genericRow = RowDataTestUtil.toGenericRowDeeply(row, types);
                    expectedRecords.add(genericRow);
                }
            }
        }

        for (Object act : actual) {
            if (act instanceof StreamRecord) {
                RowData actualOutput = (RowData) ((StreamRecord) act).getValue();
                // joined row can't equals to generic row, so cast joined row to generic row first
                GenericRowData actualRow = RowDataTestUtil.toGenericRowDeeply(actualOutput, types);
                actualRecords.add(actualRow);
            }
        }

        GenericRowData[] sortedExpected =
                expectedRecords.toArray(new GenericRowData[expectedRecords.size()]);
        GenericRowData[] sortedActual =
                actualRecords.toArray(new GenericRowData[actualRecords.size()]);

        if (needSort) {
            Arrays.sort(sortedExpected, comparator);
            Arrays.sort(sortedActual, comparator);
        }

        assertThat(sortedActual).as(message).isEqualTo(sortedExpected);
    }

    private static class StringComparator implements Comparator<GenericRowData> {
        @Override
        public int compare(GenericRowData o1, GenericRowData o2) {
            return o1.toString().compareTo(o2.toString());
        }
    }
}
