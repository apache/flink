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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.RowDataRecordEqualiser;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Base Tests for all subclass of {@link AbstractTopNFunction}. */
abstract class TopNFunctionTestBase {

    Time minTime = Time.milliseconds(10);
    Time maxTime = Time.milliseconds(20);
    long cacheSize = 10000L;

    InternalTypeInfo<RowData> inputRowType =
            InternalTypeInfo.ofFields(
                    new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(), new IntType());

    static GeneratedRecordComparator generatedSortKeyComparator =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = 1434685115916728955L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return IntRecordComparator.INSTANCE;
                }
            };

    static ComparableRecordComparator comparableRecordComparator =
            new ComparableRecordComparator(
                    generatedSortKeyComparator,
                    new int[] {0},
                    new LogicalType[] {new IntType()},
                    new boolean[] {true},
                    new boolean[] {true});

    private int sortKeyIdx = 2;

    BinaryRowDataKeySelector sortKeySelector =
            new BinaryRowDataKeySelector(new int[] {sortKeyIdx}, inputRowType.toRowFieldTypes());

    static GeneratedRecordEqualiser generatedEqualiser =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                private static final long serialVersionUID = 8932460173848746733L;

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new RowDataRecordEqualiser();
                }
            };

    private int partitionKeyIdx = 0;

    private BinaryRowDataKeySelector keySelector =
            new BinaryRowDataKeySelector(
                    new int[] {partitionKeyIdx}, inputRowType.toRowFieldTypes());

    private InternalTypeInfo<RowData> outputTypeWithoutRowNumber = inputRowType;

    private InternalTypeInfo<RowData> outputTypeWithRowNumber =
            InternalTypeInfo.ofFields(
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BigIntType(),
                    new IntType(),
                    new BigIntType());

    RowDataHarnessAssertor assertorWithoutRowNumber =
            new RowDataHarnessAssertor(
                    outputTypeWithoutRowNumber.toRowFieldTypes(),
                    new GenericRowRecordSortComparator(
                            sortKeyIdx, outputTypeWithoutRowNumber.toRowFieldTypes()[sortKeyIdx]));

    RowDataHarnessAssertor assertorWithRowNumber =
            new RowDataHarnessAssertor(
                    outputTypeWithRowNumber.toRowFieldTypes(),
                    new GenericRowRecordSortComparator(
                            sortKeyIdx, outputTypeWithRowNumber.toRowFieldTypes()[sortKeyIdx]));

    // rowKey only used in UpdateRankFunction
    private int rowKeyIdx = 1;
    BinaryRowDataKeySelector rowKeySelector =
            new BinaryRowDataKeySelector(new int[] {rowKeyIdx}, inputRowType.toRowFieldTypes());

    /** RankEnd column must be long, int or short type, but could not be string type yet. */
    @Test(expected = UnsupportedOperationException.class)
    public void testInvalidVariableRankRangeWithIntType() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(0), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportRank() throws Exception {
        createFunction(RankType.RANK, new ConstantRankRange(1, 10), true, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportDenseRank() throws Exception {
        createFunction(RankType.DENSE_RANK, new ConstantRankRange(1, 10), true, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportWithoutRankEnd() throws Exception {
        createFunction(RankType.ROW_NUMBER, new ConstantRankRangeWithoutEnd(1), true, true);
    }

    @Test
    public void testDisableGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("book", 5L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        // Notes: Delete message will be sent even disable generate retraction when not output
        // rankNumber.
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 4L, 11));
        expectedOutput.add(deleteRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 5L, 11));
        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(insertRecord("fruit", 3L, 44));
        expectedOutput.add(deleteRecord("fruit", 3L, 44));
        expectedOutput.add(insertRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testDisableGenerateUpdateBeforeAndOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("book", 5L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        // Notes: Retract message will not be sent if disable generate retraction and output
        // rankNumber.
        // Because partition key + rankNumber decomposes a uniqueKey.
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testOutputRankNumberWithConstantRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("book", 5L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 2L));
        expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
        expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 3L, 44, 2L));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testConstantRankRangeWithOffset() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(2, 2), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("fruit", 3L, 44));
        expectedOutput.add(updateBeforeRecord("fruit", 3L, 44));
        expectedOutput.add(updateAfterRecord("fruit", 4L, 33));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testOutputRankNumberWithVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 33));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(insertRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 12, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 12, 2L));
        expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testConstantRankRangeWithoutOffset() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 1L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 4L, 11));
        testHarness.processElement(insertRecord("fruit", 4L, 33));
        testHarness.processElement(insertRecord("fruit", 3L, 44));
        testHarness.processElement(insertRecord("fruit", 5L, 22));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 4L, 11));
        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(insertRecord("fruit", 3L, 44));
        expectedOutput.add(deleteRecord("fruit", 3L, 44));
        expectedOutput.add(insertRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());

        // do a snapshot, data could be recovered from state
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        testHarness = createTestHarness(func);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 5L, 10));
        testHarness.close();

        expectedOutput.add(deleteRecord("book", 1L, 12));
        expectedOutput.add(insertRecord("book", 5L, 10));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            AbstractTopNFunction rankFunction) throws Exception {
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(rankFunction);
        rankFunction.setKeyContext(operator);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, keySelector, keySelector.getProducedType());
    }

    abstract AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber);
}
