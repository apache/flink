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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BaseRowRecordEqualiser;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Base Tests for all subclass of {@link AbstractTopNFunction}.
 */
abstract class TopNFunctionTestBase {

	Time minTime = Time.milliseconds(10);
	Time maxTime = Time.milliseconds(20);
	long cacheSize = 10000L;

	BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new IntType());

	static GeneratedRecordComparator sortKeyComparator = new GeneratedRecordComparator("", "", new Object[0]) {

		private static final long serialVersionUID = 1434685115916728955L;

		@Override
		public RecordComparator newInstance(ClassLoader classLoader) {

			return IntRecordComparator.INSTANCE;
		}
	};

	private int sortKeyIdx = 2;

	BinaryRowKeySelector sortKeySelector = new BinaryRowKeySelector(new int[] { sortKeyIdx },
			inputRowType.getLogicalTypes());

	static GeneratedRecordEqualiser generatedEqualiser = new GeneratedRecordEqualiser("", "", new Object[0]) {

		private static final long serialVersionUID = 8932460173848746733L;

		@Override
		public RecordEqualiser newInstance(ClassLoader classLoader) {
			return new BaseRowRecordEqualiser();
		}
	};

	private int partitionKeyIdx = 0;

	private BinaryRowKeySelector keySelector = new BinaryRowKeySelector(new int[] { partitionKeyIdx },
			inputRowType.getLogicalTypes());

	private BaseRowTypeInfo outputTypeWithoutRowNumber = inputRowType;

	private BaseRowTypeInfo outputTypeWithRowNumber = new BaseRowTypeInfo(
			new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(),
			new IntType(),
			new BigIntType());

	BaseRowHarnessAssertor assertorWithoutRowNumber = new BaseRowHarnessAssertor(
			outputTypeWithoutRowNumber.getFieldTypes(),
			new GenericRowRecordSortComparator(sortKeyIdx, outputTypeWithoutRowNumber.getLogicalTypes()[sortKeyIdx]));

	BaseRowHarnessAssertor assertorWithRowNumber = new BaseRowHarnessAssertor(
			outputTypeWithRowNumber.getFieldTypes(),
			new GenericRowRecordSortComparator(sortKeyIdx, outputTypeWithRowNumber.getLogicalTypes()[sortKeyIdx]));

	// rowKey only used in UpdateRankFunction
	private int rowKeyIdx = 1;
	BinaryRowKeySelector rowKeySelector = new BinaryRowKeySelector(new int[] { rowKeyIdx },
			inputRowType.getLogicalTypes());

	/** RankEnd column must be long, int or short type, but could not be string type yet. */
	@Test(expected = UnsupportedOperationException.class)
	public void testInvalidVariableRankRangeWithIntType() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new VariableRankRange(0), true, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
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
	public void testDisableGenerateRetraction() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 3L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("book", 5L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		// Notes: Delete message will be sent even disable generate retraction when not output rankNumber.
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("book", 2L, 19));
		expectedOutput.add(deleteRecord("book", 2L, 19));
		expectedOutput.add(record("book", 4L, 11));
		expectedOutput.add(deleteRecord("book", 1L, 12));
		expectedOutput.add(record("book", 5L, 11));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 3L, 44));
		expectedOutput.add(deleteRecord("fruit", 3L, 44));
		expectedOutput.add(record("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testDisableGenerateRetractionAndOutputRankNumber() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
				true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("book", 5L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		// Notes: Retract message will not be sent if disable generate retraction and output rankNumber.
		// Because partition key + rankNumber decomposes a uniqueKey.
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12, 1L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 4L, 11, 1L));
		expectedOutput.add(record("book", 1L, 12, 2L));
		expectedOutput.add(record("book", 5L, 11, 2L));
		expectedOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutput.add(record("fruit", 5L, 22, 1L));
		expectedOutput.add(record("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testOutputRankNumberWithConstantRankRange() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("book", 5L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12, 1L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(retractRecord("book", 1L, 12, 1L));
		expectedOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 4L, 11, 1L));
		expectedOutput.add(record("book", 1L, 12, 2L));
		expectedOutput.add(retractRecord("book", 1L, 12, 2L));
		expectedOutput.add(record("book", 5L, 11, 2L));
		expectedOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutput.add(retractRecord("fruit", 4L, 33, 1L));
		expectedOutput.add(retractRecord("fruit", 3L, 44, 2L));
		expectedOutput.add(record("fruit", 4L, 33, 2L));
		expectedOutput.add(record("fruit", 5L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testConstantRankRangeWithOffset() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new ConstantRankRange(2, 2), true,
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
		expectedOutput.add(record("book", 2L, 19));
		expectedOutput.add(retractRecord("book", 2L, 19));
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("fruit", 3L, 44));
		expectedOutput.add(retractRecord("fruit", 3L, 44));
		expectedOutput.add(record("fruit", 4L, 33));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testOutputRankNumberWithVariableRankRange() throws Exception {
		AbstractTopNFunction func = createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
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
		expectedOutput.add(record("book", 2L, 12, 1L));
		expectedOutput.add(record("book", 2L, 19, 2L));
		expectedOutput.add(retractRecord("book", 2L, 12, 1L));
		expectedOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutput.add(record("book", 2L, 11, 1L));
		expectedOutput.add(record("book", 2L, 12, 2L));
		expectedOutput.add(record("fruit", 1L, 33, 1L));
		expectedOutput.add(retractRecord("fruit", 1L, 33, 1L));
		expectedOutput.add(record("fruit", 1L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

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
		expectedOutput.add(retractRecord("book", 2L, 19));
		expectedOutput.add(record("book", 4L, 11));
		expectedOutput.add(record("fruit", 4L, 33));
		expectedOutput.add(record("fruit", 3L, 44));
		expectedOutput.add(retractRecord("fruit", 3L, 44));
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
		testHarness.close();

		expectedOutput.add(retractRecord("book", 1L, 12));
		expectedOutput.add(record("book", 1L, 10));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			AbstractTopNFunction rankFunction)
			throws Exception {
		KeyedProcessOperator<BaseRow, BaseRow, BaseRow> operator = new KeyedProcessOperator<>(rankFunction);
		rankFunction.setKeyContext(operator);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keySelector.getProducedType());
	}

	abstract AbstractTopNFunction createFunction(RankType rankType, RankRange rankRange,
			boolean generateRetraction, boolean outputRankNumber);

}
