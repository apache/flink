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

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/** Tests for {@link AppendOnlyFirstNFunction}. */
public class AppendOnlyFirstNFunctionTest extends TopNFunctionTestBase {
    @Override
    AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber) {
        return new AppendOnlyFirstNFunction(
                ttlConfig,
                inputRowType,
                generatedSortKeyComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
    }

    @Override
    @Test
    public void testDisableGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, false);
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
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 44));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @Test
    public void testDisableGenerateUpdateBeforeAndOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
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
        expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 1L, 44, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @Test
    public void testOutputRankNumberWithConstantRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
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
        expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(insertRecord("fruit", 1L, 44, 2L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @Test
    public void testConstantRankRangeWithOffset() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(2, 2), true, false);
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
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 44));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @Test
    public void testConstantRankRangeWithoutOffset() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
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
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 44));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @Test
    public void testOutputRankNumberWithVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), false, true);
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
        expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }
}
