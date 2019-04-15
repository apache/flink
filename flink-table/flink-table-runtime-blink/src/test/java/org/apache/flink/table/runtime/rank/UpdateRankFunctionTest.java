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

package org.apache.flink.table.runtime.rank;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Tests for {@link UpdateRankFunction}.
 */
public class UpdateRankFunctionTest extends BaseRankFunctionTest {

	@Override
	protected AbstractRankFunction createRankFunction(RankType rankType, RankRange rankRange,
			boolean generateRetraction, boolean outputRankNumber) {
		return new UpdateRankFunction(minTime.toMilliseconds(), maxTime.toMilliseconds(),
				inputRowType, rowKeySelector, sortKeyComparator, sortKeySelector, rankType, rankRange,
				generatedEqualiser, generateRetraction, outputRankNumber, cacheSize);
	}

	@Test
	public void testVariableRankRange() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new VariableRankRange(1), true, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 2L, 18));
		testHarness.processElement(record("fruit", 1L, 44));
		testHarness.processElement(record("fruit", 1L, 33));
		testHarness.processElement(record("fruit", 1L, 22));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(retractRecord("book", 2L, 19));
		expectedOutputOutput.add(record("book", 2L, 18));
		expectedOutputOutput.add(record("fruit", 1L, 44));
		expectedOutputOutput.add(retractRecord("fruit", 1L, 44));
		expectedOutputOutput.add(record("fruit", 1L, 33));
		expectedOutputOutput.add(retractRecord("fruit", 1L, 33));
		expectedOutputOutput.add(record("fruit", 1L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Override
	@Test
	public void testOutputRankNumberWithVariableRankRange() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new VariableRankRange(1), true, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 2L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("fruit", 1L, 44));
		testHarness.processElement(record("fruit", 1L, 33));
		testHarness.processElement(record("fruit", 1L, 22));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 19, 1L));
		expectedOutputOutput.add(record("book", 2L, 12, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 12, 1L));
		expectedOutputOutput.add(record("book", 2L, 11, 1L));
		expectedOutputOutput.add(record("fruit", 1L, 44, 1L));
		expectedOutputOutput.add(retractRecord("fruit", 1L, 44, 1L));
		expectedOutputOutput.add(record("fruit", 1L, 33, 1L));
		expectedOutputOutput.add(retractRecord("fruit", 1L, 33, 1L));
		expectedOutputOutput.add(record("fruit", 1L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testSortKeyChangesWhenOutputRankNumber() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new ConstantRankRange(1, 2), true, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 3L, 16));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 15));
		testHarness.processElement(record("book", 4L, 2));
		testHarness.processElement(record("book", 2L, 1));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 19, 1L));
		expectedOutputOutput.add(record("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 3L, 16, 1L));
		expectedOutputOutput.add(retractRecord("book", 3L, 16, 1L));
		expectedOutputOutput.add(record("book", 3L, 16, 2L));
		expectedOutputOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 2L, 11, 1L));
		expectedOutputOutput.add(retractRecord("book", 3L, 16, 2L));
		expectedOutputOutput.add(record("book", 3L, 15, 2L));
		expectedOutputOutput.add(retractRecord("book", 3L, 15, 2L));
		expectedOutputOutput.add(retractRecord("book", 2L, 11, 1L));
		expectedOutputOutput.add(record("book", 2L, 11, 2L));
		expectedOutputOutput.add(record("book", 4L, 2, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 11, 2L));
		expectedOutputOutput.add(retractRecord("book", 4L, 2, 1L));
		expectedOutputOutput.add(record("book", 2L, 1, 1L));
		expectedOutputOutput.add(record("book", 4L, 2, 2L));

		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testSortKeyChangesWhenOutputRankNumberAndNotGenerateRetraction() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new ConstantRankRange(1, 2), false, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 3L, 16));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 15));
		testHarness.processElement(record("book", 4L, 2));
		testHarness.processElement(record("book", 2L, 1));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19, 1L));
		expectedOutputOutput.add(record("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 3L, 16, 1L));
		expectedOutputOutput.add(record("book", 3L, 16, 2L));
		expectedOutputOutput.add(record("book", 2L, 11, 1L));
		expectedOutputOutput.add(record("book", 3L, 15, 2L));
		expectedOutputOutput.add(record("book", 2L, 11, 2L));
		expectedOutputOutput.add(record("book", 4L, 2, 1L));
		expectedOutputOutput.add(record("book", 2L, 1, 1L));
		expectedOutputOutput.add(record("book", 4L, 2, 2L));

		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testSortKeyChangesWhenNotOutputRankNumber() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new ConstantRankRange(1, 2), true, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 3L, 16));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 15));
		testHarness.processElement(record("book", 4L, 2));
		testHarness.processElement(record("book", 2L, 1));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(record("book", 3L, 16));
		expectedOutputOutput.add(retractRecord("book", 2L, 19));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(retractRecord("book", 3L, 16));
		expectedOutputOutput.add(record("book", 3L, 15));
		expectedOutputOutput.add(record("book", 4L, 2));
		expectedOutputOutput.add(retractRecord("book", 3L, 15));
		expectedOutputOutput.add(record("book", 2L, 1));
		expectedOutputOutput.add(retractRecord("book", 2L, 11));

		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testSortKeyChangesWhenNotOutputRankNumberAndNotGenerateRetraction() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER,
				new ConstantRankRange(1, 2), false, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 3L, 16));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 15));
		testHarness.processElement(record("book", 4L, 2));
		testHarness.processElement(record("book", 2L, 1));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(record("book", 3L, 16));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 3L, 15));
		expectedOutputOutput.add(record("book", 4L, 2));
		expectedOutputOutput.add(deleteRecord("book", 3L, 15));
		expectedOutputOutput.add(record("book", 2L, 1));

		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}
}
