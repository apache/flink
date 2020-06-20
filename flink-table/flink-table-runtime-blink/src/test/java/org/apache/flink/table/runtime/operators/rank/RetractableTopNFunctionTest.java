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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/**
 * Tests for {@link RetractableTopNFunction}.
 */
public class RetractableTopNFunctionTest extends TopNFunctionTestBase {

	@Override
	protected AbstractTopNFunction createFunction(RankType rankType, RankRange rankRange,
			boolean generateUpdateBefore, boolean outputRankNumber) {
		return new RetractableTopNFunction(
			minTime.toMilliseconds(),
			maxTime.toMilliseconds(),
			inputRowType,
			sortKeyComparator,
			sortKeySelector,
			rankType,
			rankRange,
			generatedEqualiser,
			generateUpdateBefore,
			outputRankNumber);
	}

	@Test
	public void testProcessRetractMessageWithNotGenerateUpdateBefore() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				true);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 2L, 19));
		testHarness.processElement(insertRecord("book", 4L, 11));
		testHarness.processElement(updateBeforeRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 5L, 11));
		testHarness.processElement(insertRecord("fruit", 4L, 33));
		testHarness.processElement(insertRecord("fruit", 3L, 44));
		testHarness.processElement(insertRecord("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		// ("book", 1L, 12)
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		expectedOutput.add(insertRecord("book", 2L, 19, 2L));
		// ("book", 4L, 11)
		expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
		expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
		// UB ("book", 1L, 12)
		expectedOutput.add(updateAfterRecord("book", 2L, 19, 2L));
		// ("book", 5L, 11)
		expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
		// ("fruit", 4L, 33)
		// ("fruit", 3L, 44)
		expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
		// ("fruit", 5L, 22)
		expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testProcessRetractMessageWithGenerateUpdateBefore() throws Exception {
		AbstractTopNFunction func = createFunction(
			RankType.ROW_NUMBER,
			new ConstantRankRange(1, 2),
			true,
			true);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 2L, 19));
		testHarness.processElement(insertRecord("book", 4L, 11));
		testHarness.processElement(updateBeforeRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 5L, 11));
		testHarness.processElement(insertRecord("fruit", 4L, 33));
		testHarness.processElement(insertRecord("fruit", 3L, 44));
		testHarness.processElement(insertRecord("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		// ("book", 1L, 12)
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		expectedOutput.add(insertRecord("book", 2L, 19, 2L));
		// ("book", 4L, 11)
		expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
		expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
		expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
		expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
		// UB ("book", 1L, 12)
		expectedOutput.add(updateBeforeRecord("book", 1L, 12, 2L));
		expectedOutput.add(updateAfterRecord("book", 2L, 19, 2L));
		expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
		expectedOutput.add(updateAfterRecord("book", 5L, 11, 2L));
		// ("fruit", 4L, 33)
		// ("fruit", 3L, 44)
		expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
		// ("fruit", 5L, 22)
		expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
		expectedOutput.add(updateBeforeRecord("fruit", 3L, 44, 2L));
		expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testConstantRankRangeWithoutOffsetWithRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				true);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 2L, 19));
		testHarness.processElement(insertRecord("book", 4L, 11));
		testHarness.processElement(insertRecord("fruit", 4L, 33));
		testHarness.processElement(insertRecord("fruit", 3L, 44));
		testHarness.processElement(insertRecord("fruit", 5L, 22));

		List<Object> expectedOutput = new ArrayList<>();
		// ("book", 1L, 12)
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		expectedOutput.add(insertRecord("book", 2L, 19, 2L));
		// ("book", 4L, 11)
		expectedOutput.add(updateBeforeRecord("book", 1L, 12, 1L));
		expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
		expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
		expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
		// ("fruit", 4L, 33)
		// ("fruit", 3L, 44)
		expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
		// ("fruit", 5L, 22)
		expectedOutput.add(updateBeforeRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
		expectedOutput.add(updateBeforeRecord("fruit", 3L, 44, 2L));
		expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
		assertorWithRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
		testHarness = createTestHarness(func);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 10));

		expectedOutput.add(updateBeforeRecord("book", 4L, 11, 1L));
		expectedOutput.add(updateAfterRecord("book", 1L, 10, 1L));
		expectedOutput.add(updateBeforeRecord("book", 1L, 12, 2L));
		expectedOutput.add(updateAfterRecord("book", 4L, 11, 2L));
		assertorWithRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testConstantRankRangeWithoutOffsetWithoutRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				false);
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
		assertorWithoutRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
		testHarness = createTestHarness(func);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 10));

		expectedOutput.add(deleteRecord("book", 1L, 12));
		expectedOutput.add(insertRecord("book", 1L, 10));
		assertorWithoutRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testVariableRankRangeWithRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
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
		// ("book", 2L, 12)
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 2L, 12, 1L));
		expectedOutput.add(insertRecord("book", 2L, 19, 2L));
		// ("book", 2L, 11)
		expectedOutput.add(updateBeforeRecord("book", 2L, 12, 1L));
		expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
		expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
		expectedOutput.add(updateAfterRecord("book", 2L, 12, 2L));
		// ("fruit", 1L, 33)
		expectedOutput.add(insertRecord("fruit", 1L, 33, 1L));

		// ("fruit", 1L, 44)
		// nothing, because it's Top-1

		// ("fruit", 1L, 22)
		expectedOutput.add(updateBeforeRecord("fruit", 1L, 33, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 1L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testVariableRankRangeWithoutRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
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
		expectedOutput.add(deleteRecord("book", 2L, 19));
		expectedOutput.add(insertRecord("book", 2L, 11));
		expectedOutput.add(insertRecord("fruit", 1L, 33));
		expectedOutput.add(deleteRecord("fruit", 1L, 33));
		expectedOutput.add(insertRecord("fruit", 1L, 22));
		assertorWithoutRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testDisableGenerateUpdateBeforeWithRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				true);
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
		// ("book", 1L, 12)
		// ("book", 2L, 19)
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		expectedOutput.add(insertRecord("book", 2L, 19, 2L));
		// ("book", 4L, 11)
		expectedOutput.add(updateAfterRecord("book", 4L, 11, 1L));
		expectedOutput.add(updateAfterRecord("book", 1L, 12, 2L));
		// ("fruit", 4L, 33)
		// ("fruit", 3L, 44)
		expectedOutput.add(insertRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(insertRecord("fruit", 3L, 44, 2L));
		// ("fruit", 5L, 22)
		expectedOutput.add(updateAfterRecord("fruit", 5L, 22, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 4L, 33, 2L));
		assertorWithRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testDisableGenerateUpdateBeforeWithoutRowNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				false);
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
		expectedOutput.add(insertRecord("book", 1L, 12));
		expectedOutput.add(insertRecord("book", 2L, 19));
		expectedOutput.add(deleteRecord("book", 2L, 19));
		expectedOutput.add(insertRecord("book", 4L, 11));
		expectedOutput.add(insertRecord("fruit", 4L, 33));
		expectedOutput.add(insertRecord("fruit", 3L, 44));
		expectedOutput.add(deleteRecord("fruit", 3L, 44));
		expectedOutput.add(insertRecord("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testCleanIdleState() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
			true);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.open();
		// register cleanup timer with 20L
		testHarness.setProcessingTime(0L);
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("fruit", 5L, 22));

		// register cleanup timer with 29L
		testHarness.setProcessingTime(9L);
		testHarness.processElement(updateBeforeRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("fruit", 4L, 11));

		// trigger the first cleanup timer and register cleanup timer with 4000
		testHarness.setProcessingTime(20L);
		testHarness.processElement(insertRecord("fruit", 8L, 100));
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		// ("book", 1L, 12)
		// ("fruit", 5L, 22)
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		expectedOutput.add(insertRecord("fruit", 5L, 22, 1L));
		// UB ("book", 1L, 12)
		expectedOutput.add(deleteRecord("book", 1L, 12, 1L));
		// ("fruit", 4L, 11)
		expectedOutput.add(updateBeforeRecord("fruit", 5L, 22, 1L));
		expectedOutput.add(updateAfterRecord("fruit", 4L, 11, 1L));
		expectedOutput.add(insertRecord("fruit", 5L, 22, 2L));

		// after idle state expired
		// ("fruit", 8L, 100)
		// ("book", 1L, 12)
		expectedOutput.add(insertRecord("fruit", 8L, 100, 1L));
		expectedOutput.add(insertRecord("book", 1L, 12, 1L));
		assertorWithRowNumber.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}
}
