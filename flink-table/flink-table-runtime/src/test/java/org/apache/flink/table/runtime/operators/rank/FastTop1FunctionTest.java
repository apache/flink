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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.async.AsyncStateFastTop1Function;

import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Tests for {@link FastTop1Function} and {@link AsyncStateFastTop1Function}. */
public class FastTop1FunctionTest extends TopNFunctionTestBase {

    @Override
    AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            boolean enableAsyncState) {
        if (enableAsyncState) {
            return new AsyncStateFastTop1Function(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        } else {
            return new FastTop1Function(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        }
    }

    @Override
    boolean supportedAsyncState() {
        return true;
    }

    @Override
    void testDisableGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), false, false);
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

        // Notes: partition key is unique key if only top-1 is desired.
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 4L, 11));
        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    void testOutputRankNumberWithConstantRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, true);
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
        expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    void testDisableGenerateUpdateBeforeAndOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), false, true);
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

        // Notes: partition key is unique key if only top-1 is desired.
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 1L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
        expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    void testConstantRankRangeWithoutOffset() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, false);
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
        expectedOutput.add(updateBeforeRecord("book", 1L, 12));
        expectedOutput.add(updateAfterRecord("book", 4L, 11));

        expectedOutput.add(insertRecord("fruit", 4L, 33));
        expectedOutput.add(updateBeforeRecord("fruit", 4L, 33));
        expectedOutput.add(updateAfterRecord("fruit", 5L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());

        // do a snapshot, data could be recovered from state
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();
        expectedOutput.clear();

        func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, false);
        testHarness = createTestHarness(func);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 5L, 10));
        testHarness.close();

        expectedOutput.add(updateBeforeRecord("book", 4L, 11));
        expectedOutput.add(updateAfterRecord("book", 5L, 10));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    // ------------ Tests with UPDATE input records ---------------

    @TestTemplate
    void testVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(updateAfterRecord("book", 2L, 18));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 33));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 2L, 18));
        expectedOutput.add(insertRecord("fruit", 1L, 44));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 44));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 33));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testOutputRankNumberWithUpdateInputs() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(updateAfterRecord("book", 2L, 12));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 33));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(insertRecord("fruit", 1L, 44, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 44, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        // ("book", 3L, 16)
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 1L));
        // U ("book", 2L, 11)
        expectedOutput.add(updateBeforeRecord("book", 3L, 16, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        // ("book", 4L, 2)
        expectedOutput.add(updateBeforeRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 1L));
        // U ("book", 2L, 1)
        expectedOutput.add(updateBeforeRecord("book", 4L, 2, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 1, 1L));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenOutputRankNumberAndNotGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        // ("book", 3L, 16)
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 1L));
        // U ("book", 2L, 11)
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        // ("book", 4L, 2)
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 1L));
        // U ("book", 2L, 1)
        expectedOutput.add(updateAfterRecord("book", 2L, 1, 1L));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenNotOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 3L, 16));
        expectedOutput.add(updateBeforeRecord("book", 3L, 16));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateBeforeRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 4L, 2));
        expectedOutput.add(updateBeforeRecord("book", 4L, 2));
        expectedOutput.add(updateAfterRecord("book", 2L, 1));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenNotOutputRankNumberAndNotGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 1), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 3L, 16));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 4L, 2));
        expectedOutput.add(updateAfterRecord("book", 2L, 1));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    void testConstantRankRangeWithOffset() throws Exception {
        // skip
    }

    @Override
    void testOutputRankNumberWithVariableRankRange() throws Exception {
        // skip
    }
}
