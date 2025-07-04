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
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link NonTimeRowsUnboundedPrecedingFunction}. */
public class NonTimeRowsUnboundedPrecedingFunctionTest extends NonTimeOverWindowTestBase {

    private NonTimeRowsUnboundedPrecedingFunction<RowData> getNonTimeRowsUnboundedPrecedingFunction(
            long retentionTime, GeneratedRecordComparator generatedSortKeyComparator) {
        return new NonTimeRowsUnboundedPrecedingFunction<RowData>(
                retentionTime,
                aggsHandleFunction,
                GENERATED_ROW_VALUE_EQUALISER,
                GENERATED_SORT_KEY_EQUALISER,
                generatedSortKeyComparator,
                accTypes,
                inputFieldTypes,
                SORT_KEY_TYPES,
                SORT_KEY_SELECTOR) {};
    }

    @Test
    public void testInsertOnlyRecordsWithCustomSortKey() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
    public void testInsertOnlyRecordsWithCustomSortKeyAndLongSumAgg() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        new NonTimeRowsUnboundedPrecedingFunction<RowData>(
                                0L,
                                aggsSumLongHandleFunction,
                                GENERATED_ROW_VALUE_EQUALISER,
                                GENERATED_SORT_KEY_EQUALISER,
                                GENERATED_SORT_KEY_COMPARATOR_ASC,
                                accTypes,
                                inputFieldTypes,
                                SORT_KEY_TYPES,
                                SORT_KEY_SELECTOR) {});

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
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 13L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 18L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 24L),
                        outputRecord(RowKind.INSERT, "key2", 1L, 100L, 1L),
                        outputRecord(RowKind.INSERT, "key2", 2L, 200L, 3L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 203L, 5L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 8L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 13L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 18L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 24L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 7L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 12L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 17L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 22L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 28L),
                        outputRecord(RowKind.INSERT, "key1", 4L, 400L, 11L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 12L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 16L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 17L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 21L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 22L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 26L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 28L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 32L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractingRecordsWithCustomSortKey() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 502L, 10L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithMiddleDuplicateSortKey() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 501L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithLastDuplicateSortKey() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 6L, 600L, 21L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithDescendingSort() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_DESC));

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
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 4L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 3L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 5L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 2L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 7L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 4L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 9L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 6L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 5L, 500L, 5L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 5L, 500L, 11L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 200L, 7L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 200L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 9L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 15L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 10L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 16L),
                        outputRecord(RowKind.DELETE, "key1", 2L, 200L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 2L, 201L, 15L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 2L, 201L, 13L),
                        outputRecord(RowKind.UPDATE_BEFORE, "key1", 1L, 100L, 16L),
                        outputRecord(RowKind.UPDATE_AFTER, "key1", 1L, 100L, 14L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testRetractWithEarlyOut() throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(
                        getNonTimeRowsUnboundedPrecedingFunction(
                                0L, GENERATED_SORT_KEY_COMPARATOR_ASC));

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
                        outputRecord(RowKind.INSERT, "key1", 2L, 201L, 5L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 500L, 10L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 502L, 15L),
                        outputRecord(RowKind.INSERT, "key1", 5L, 501L, 20L),
                        outputRecord(RowKind.INSERT, "key1", 6L, 600L, 26L),
                        outputRecord(RowKind.DELETE, "key1", 0L, 100L, 0L));
        List<RowData> actualRows = testHarness.extractOutputValues();

        validateRows(actualRows, expectedRows);
    }

    @Test
    public void testInsertAndRetractAllWithStateValidation() throws Exception {
        NonTimeRowsUnboundedPrecedingFunction<RowData> function =
                getNonTimeRowsUnboundedPrecedingFunction(0L, GENERATED_SORT_KEY_COMPARATOR_ASC);
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
        assertThat(actualRows.size()).isEqualTo(28);
        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(0L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    @Test
    public void testInsertWithStateTTLExpiration() throws Exception {
        Duration stateTtlTime = Duration.ofMillis(10);
        NonTimeRowsUnboundedPrecedingFunction<RowData> function =
                getNonTimeRowsUnboundedPrecedingFunction(
                        stateTtlTime.toMillis(), GENERATED_SORT_KEY_COMPARATOR_ASC);
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
        assertThat(actualRows.size()).isEqualTo(4);

        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(0L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    @Test
    public void testInsertAndRetractWithStateTTLExpiration() throws Exception {
        Duration stateTtlTime = Duration.ofMillis(10);
        NonTimeRowsUnboundedPrecedingFunction<RowData> function =
                getNonTimeRowsUnboundedPrecedingFunction(
                        stateTtlTime.toMillis(), GENERATED_SORT_KEY_COMPARATOR_ASC);
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
        assertThat(actualRows.size()).isEqualTo(5);

        assertThat(function.getNumOfSortKeysNotFound().getCount()).isEqualTo(1L);
        assertThat(function.getNumOfIdsNotFound().getCount()).isEqualTo(0L);
    }

    void validateNumAccRows(int numAccRows, int expectedNumAccRows, int totalRows) {
        assertThat(numAccRows).isEqualTo(totalRows);
    }

    void validateEntry(
            AbstractNonTimeUnboundedPrecedingOver<RowData> function, RowData record, int idOffset)
            throws Exception {
        assertThat(
                        function.getRuntimeContext()
                                .getMapState(function.accStateDescriptor)
                                .get(GenericRowData.of(Long.MIN_VALUE + idOffset)))
                .isNotNull();
    }
}
