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
import org.apache.flink.table.dataformat.BaseRow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Tests for {@link RetractableTopNFunction}.
 */
public class RetractableTopNFunctionTest extends TopNFunctionTestBase {

	@Override
	protected AbstractTopNFunction createFunction(RankType rankType, RankRange rankRange,
			boolean generateRetraction, boolean outputRankNumber) {
		return new RetractableTopNFunction(minTime.toMilliseconds(), maxTime.toMilliseconds(),
				inputRowType, sortKeyComparator, sortKeySelector, rankType, rankRange, generatedEqualiser,
				generateRetraction, outputRankNumber);
	}

	@Test
	public void testProcessRetractMessageWithNotGenerateRetraction() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(retractRecord("book", 1L, 12));
		testHarness.processElement(record("book", 5L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12, 1L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 4L, 11, 1L));
		expectedOutput.add(record("book", 1L, 12, 2L));
		expectedOutput.add(deleteRecord("book", 1L, 12, 2L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 5L, 11, 2L));
		expectedOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutput.add(record("fruit", 5L, 22, 1L));
		expectedOutput.add(record("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testProcessRetractMessageWithGenerateRetraction() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(retractRecord("book", 1L, 12));
		testHarness.processElement(record("book", 5L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12, 1L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutput.add(retractRecord("book", 1L, 12, 1L));
		expectedOutput.add(record("book", 4L, 11, 1L));
		expectedOutput.add(record("book", 1L, 12, 2L));
		expectedOutput.add(retractRecord("book", 1L, 12, 2L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 5L, 11, 2L));
		expectedOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutput.add(retractRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(retractRecord("fruit", 3L, 44, 2L));
		expectedOutput.add(record("fruit", 5L, 22, 1L));
		expectedOutput.add(record("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	// TODO RetractRankFunction could be sent less retraction message when does not need to retract row_number
	@Override
	@Test
	public void testConstantRankRangeWithoutOffset() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("book", 2L, 19));
		expectedOutput.add(retractRecord("book", 1L, 12));
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(retractRecord("book", 2L, 19));
		expectedOutput.add(record("book", 4L, 11));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 3L, 44));
		expectedOutput.add(retractRecord("fruit", 4L, 33));
		expectedOutput.add(retractRecord("fruit", 3L, 44));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutput.clear();

		func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
		testHarness = createTestHarness(func);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 10));

		expectedOutput.add(retractRecord("book", 1L, 12));
		expectedOutput.add(retractRecord("book", 4L, 11));
		expectedOutput.add(record("book", 4L, 11));
		expectedOutput.add(record("book", 1L, 10));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	// TODO RetractRankFunction could be sent less retraction message when does not need to retract row_number
	@Test
	public void testVariableRankRange() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("fruit", 1L, 33));
		testHarness.processElement(record("fruit", 1L, 44));
		testHarness.processElement(record("fruit", 1L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 2L, 12));
		expectedOutput.add(record("book", 2L, 19));
		expectedOutput.add(retractRecord("book", 2L, 19));
		expectedOutput.add(retractRecord("book", 2L, 12));
		expectedOutput.add(record("book", 2L, 12));
		expectedOutput.add(record("book", 2L, 11));
		expectedOutput.add(record("fruit", 1L, 33));
		expectedOutput.add(retractRecord("fruit", 1L, 33));
		expectedOutput.add(record("fruit", 1L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	// TODO
	@Test
	public void testDisableGenerateRetraction() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("book", 2L, 19));
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("book", 4L, 11));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 3L, 44));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

}
