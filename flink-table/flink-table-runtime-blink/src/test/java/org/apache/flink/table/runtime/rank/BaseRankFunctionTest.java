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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.sort.IntRecordComparator;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BaseRowRecordEqualiser;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Base Tests for all subclass of {@link AbstractRankFunction}.
 */
abstract class BaseRankFunctionTest {

	Time minTime = Time.milliseconds(10);
	Time maxTime = Time.milliseconds(20);
	long cacheSize = 10000L;

	BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
			InternalTypes.STRING,
			InternalTypes.LONG,
			InternalTypes.INT);

	GeneratedRecordComparator sortKeyComparator = new GeneratedRecordComparator("", "", new Object[0]) {

		private static final long serialVersionUID = 1434685115916728955L;

		@Override
		public RecordComparator newInstance(ClassLoader classLoader) {

			return IntRecordComparator.INSTANCE;
		}
	};

	private int sortKeyIdx = 2;

	BinaryRowKeySelector sortKeySelector = new BinaryRowKeySelector(new int[] { sortKeyIdx },
			inputRowType.getInternalTypes());

	GeneratedRecordEqualiser generatedEqualiser = new GeneratedRecordEqualiser("", "", new Object[0]) {

		private static final long serialVersionUID = 8932460173848746733L;

		@Override
		public RecordEqualiser newInstance(ClassLoader classLoader) {
			return new BaseRowRecordEqualiser();
		}
	};

	private int partitionKeyIdx = 0;

	private BinaryRowKeySelector keySelector = new BinaryRowKeySelector(new int[] { partitionKeyIdx },
			inputRowType.getInternalTypes());

	private BaseRowTypeInfo outputTypeWithoutRowNumber = inputRowType;

	private BaseRowTypeInfo outputTypeWithRowNumber = new BaseRowTypeInfo(
			InternalTypes.STRING,
			InternalTypes.LONG,
			InternalTypes.INT,
			InternalTypes.LONG);

	BaseRowHarnessAssertor assertorWithoutRowNumber = new BaseRowHarnessAssertor(
			outputTypeWithoutRowNumber.getFieldTypes(),
			new GenericRowRecordSortComparator(sortKeyIdx, outputTypeWithoutRowNumber.getInternalTypes()[sortKeyIdx]));

	BaseRowHarnessAssertor assertorWithRowNumber = new BaseRowHarnessAssertor(
			outputTypeWithRowNumber.getFieldTypes(),
			new GenericRowRecordSortComparator(sortKeyIdx, outputTypeWithRowNumber.getInternalTypes()[sortKeyIdx]));

	// rowKey only used in UpdateRankFunction
	private int rowKeyIdx = 1;
	BinaryRowKeySelector rowKeySelector = new BinaryRowKeySelector(new int[] { rowKeyIdx },
			inputRowType.getInternalTypes());

	@Test(expected = UnsupportedOperationException.class)
	public void testInvalidVariableRankRangeWithIntType() throws Exception {
		createRankFunction(RankType.ROW_NUMBER, new VariableRankRange(2), true, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotSupportRank() throws Exception {
		createRankFunction(RankType.RANK, new ConstantRankRange(1, 10), true, true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotSupportDenseRank() throws Exception {
		createRankFunction(RankType.DENSE_RANK, new ConstantRankRange(1, 10), true, true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotSupportWithoutRankEnd() throws Exception {
		createRankFunction(RankType.ROW_NUMBER, new ConstantRankRangeWithoutEnd(1), true, true);
	}

	@Test
	public void testDisableGenerateRetraction() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
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
		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(deleteRecord("book", 2L, 19));
		expectedOutputOutput.add(record("book", 4L, 11));
		expectedOutputOutput.add(deleteRecord("book", 1L, 12));
		expectedOutputOutput.add(record("book", 5L, 11));
		expectedOutputOutput.add(record("fruit", 4L, 33));
		expectedOutputOutput.add(record("fruit", 3L, 44));
		expectedOutputOutput.add(deleteRecord("fruit", 3L, 44));
		expectedOutputOutput.add(record("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testDisableGenerateRetractionAndOutputRankNumber() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false,
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
		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12, 1L));
		expectedOutputOutput.add(record("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 4L, 11, 1L));
		expectedOutputOutput.add(record("book", 1L, 12, 2L));
		expectedOutputOutput.add(record("book", 5L, 11, 2L));
		expectedOutputOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutputOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutputOutput.add(record("fruit", 5L, 22, 1L));
		expectedOutputOutput.add(record("fruit", 4L, 33, 2L));
		assertorWithRowNumber.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testOutputRankNumberWithConstantRankRange() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
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

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12, 1L));
		expectedOutputOutput.add(record("book", 2L, 19, 2L));
		expectedOutputOutput.add(retractRecord("book", 1L, 12, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 4L, 11, 1L));
		expectedOutputOutput.add(record("book", 1L, 12, 2L));
		expectedOutputOutput.add(retractRecord("book", 1L, 12, 2L));
		expectedOutputOutput.add(record("book", 5L, 11, 2L));
		expectedOutputOutput.add(record("fruit", 4L, 33, 1L));
		expectedOutputOutput.add(record("fruit", 3L, 44, 2L));
		expectedOutputOutput.add(retractRecord("fruit", 4L, 33, 1L));
		expectedOutputOutput.add(retractRecord("fruit", 3L, 44, 2L));
		expectedOutputOutput.add(record("fruit", 4L, 33, 2L));
		expectedOutputOutput.add(record("fruit", 5L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testConstantRankRangeWithOffset() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(2, 2), true,
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

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(retractRecord("book", 2L, 19));
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("fruit", 3L, 44));
		expectedOutputOutput.add(retractRecord("fruit", 3L, 44));
		expectedOutputOutput.add(record("fruit", 4L, 33));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testOutputRankNumberWithVariableRankRange() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 2L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("fruit", 1L, 33));
		testHarness.processElement(record("fruit", 1L, 44));
		testHarness.processElement(record("fruit", 1L, 22));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 12, 1L));
		expectedOutputOutput.add(record("book", 2L, 19, 2L));
		expectedOutputOutput.add(retractRecord("book", 2L, 12, 1L));
		expectedOutputOutput.add(retractRecord("book", 2L, 19, 2L));
		expectedOutputOutput.add(record("book", 2L, 11, 1L));
		expectedOutputOutput.add(record("book", 2L, 12, 2L));
		expectedOutputOutput.add(record("fruit", 1L, 33, 1L));
		expectedOutputOutput.add(retractRecord("fruit", 1L, 33, 1L));
		expectedOutputOutput.add(record("fruit", 1L, 22, 1L));
		assertorWithRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testConstantRankRangeWithoutOffset() throws Exception {
		AbstractRankFunction func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true,
				false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 19));
		testHarness.processElement(record("book", 4L, 11));
		testHarness.processElement(record("fruit", 4L, 33));
		testHarness.processElement(record("fruit", 3L, 44));
		testHarness.processElement(record("fruit", 5L, 22));

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 19));
		expectedOutputOutput.add(retractRecord("book", 2L, 19));
		expectedOutputOutput.add(record("book", 4L, 11));
		expectedOutputOutput.add(record("fruit", 4L, 33));
		expectedOutputOutput.add(record("fruit", 3L, 44));
		expectedOutputOutput.add(retractRecord("fruit", 3L, 44));
		expectedOutputOutput.add(record("fruit", 5L, 22));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();
		expectedOutputOutput.clear();

		func = createRankFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
		testHarness = createTestHarness(func);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 10));
		testHarness.close();

		expectedOutputOutput.add(retractRecord("book", 1L, 12));
		expectedOutputOutput.add(record("book", 1L, 10));
		assertorWithoutRowNumber
				.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			AbstractRankFunction rankFunction)
			throws Exception {
		KeyedProcessOperator<BaseRow, BaseRow, BaseRow> operator = new KeyedProcessOperator<>(rankFunction);
		rankFunction.setKeyContext(operator);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keySelector.getProducedType());
	}

	protected abstract AbstractRankFunction createRankFunction(RankType rankType, RankRange rankRange,
			boolean generateRetraction, boolean outputRankNumber);

}
