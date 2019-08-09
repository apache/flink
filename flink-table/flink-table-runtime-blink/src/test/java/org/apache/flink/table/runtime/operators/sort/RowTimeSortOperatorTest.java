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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Tests for {@link RowTimeSortOperator}.
 */
public class RowTimeSortOperatorTest {

	@Test
	public void testSortOnTwoFields() throws Exception {
		BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
				new IntType(),
				new BigIntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new IntType());

		// Note: RowTimeIdx must be 0 in product environment, the value is 1 here just for simplify the testing
		int rowTimeIdx = 1;
		GeneratedRecordComparator gComparator = new GeneratedRecordComparator("", "", new Object[0]) {

			private static final long serialVersionUID = -6067266199060901331L;

			@Override
			public RecordComparator newInstance(ClassLoader classLoader) {

				return IntRecordComparator.INSTANCE;
			}
		};

		BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(inputRowType.getFieldTypes());

		RowTimeSortOperator operator = createSortOperator(inputRowType, rowTimeIdx, gComparator);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);
		testHarness.open();
		testHarness.processElement(record(3, 3L, "Hello world", 3));
		testHarness.processElement(record(2, 2L, "Hello", 2));
		testHarness.processElement(record(6, 2L, "Luke Skywalker", 6));
		testHarness.processElement(record(5, 3L, "I am fine.", 5));
		testHarness.processElement(record(7, 1L, "Comment#1", 7));
		testHarness.processElement(record(9, 4L, "Comment#3", 9));
		testHarness.processElement(record(10, 4L, "Comment#4", 10));
		testHarness.processElement(record(8, 4L, "Comment#2", 8));
		testHarness.processElement(record(1, 1L, "Hi", 2));
		testHarness.processElement(record(1, 1L, "Hi", 1));
		testHarness.processElement(record(4, 3L, "Helloworld, how are you?", 4));
		testHarness.processElement(record(4, 5L, "Hello, how are you?", 4));
		testHarness.processWatermark(new Watermark(4L));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, 1L, "Hi", 2));
		expectedOutput.add(record(1, 1L, "Hi", 1));
		expectedOutput.add(record(7, 1L, "Comment#1", 7));
		expectedOutput.add(record(2, 2L, "Hello", 2));
		expectedOutput.add(record(6, 2L, "Luke Skywalker", 6));
		expectedOutput.add(record(3, 3L, "Hello world", 3));
		expectedOutput.add(record(4, 3L, "Helloworld, how are you?", 4));
		expectedOutput.add(record(5, 3L, "I am fine.", 5));
		expectedOutput.add(record(8, 4L, "Comment#2", 8));
		expectedOutput.add(record(9, 4L, "Comment#3", 9));
		expectedOutput.add(record(10, 4L, "Comment#4", 10));
		expectedOutput.add(new Watermark(4L));

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();

		expectedOutput.clear();

		operator = createSortOperator(inputRowType, rowTimeIdx, gComparator);
		testHarness = createTestHarness(operator);
		testHarness.initializeState(snapshot);
		testHarness.open();
		// late data will be dropped
		testHarness.processElement(record(5, 3L, "I am fine.", 6));
		testHarness.processWatermark(new Watermark(5L));

		expectedOutput.add(record(4, 5L, "Hello, how are you?", 4));
		expectedOutput.add(new Watermark(5L));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		// those watermark has no effect
		testHarness.processWatermark(new Watermark(11L));
		testHarness.processWatermark(new Watermark(12L));
		expectedOutput.add(new Watermark(11L));
		expectedOutput.add(new Watermark(12L));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testOnlySortOnRowTime() throws Exception {
		BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
				new BigIntType(),
				new BigIntType(),
				new VarCharType(VarCharType.MAX_LENGTH),
				new IntType());
		int rowTimeIdx = 0;
		BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(inputRowType.getFieldTypes());
		RowTimeSortOperator operator = createSortOperator(inputRowType, rowTimeIdx, null);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(operator);
		testHarness.open();
		testHarness.processElement(record(3L, 2L, "Hello world", 3));
		testHarness.processElement(record(2L, 2L, "Hello", 2));
		testHarness.processElement(record(6L, 3L, "Luke Skywalker", 6));
		testHarness.processElement(record(5L, 3L, "I am fine.", 5));
		testHarness.processElement(record(7L, 4L, "Comment#1", 7));
		testHarness.processElement(record(9L, 4L, "Comment#3", 9));
		testHarness.processElement(record(10L, 4L, "Comment#4", 10));
		testHarness.processElement(record(8L, 4L, "Comment#2", 8));
		testHarness.processElement(record(1L, 1L, "Hi", 2));
		testHarness.processElement(record(1L, 1L, "Hi", 1));
		testHarness.processElement(record(4L, 3L, "Helloworld, how are you?", 4));
		testHarness.processWatermark(new Watermark(9L));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1L, 1L, "Hi", 2));
		expectedOutput.add(record(1L, 1L, "Hi", 1));
		expectedOutput.add(record(2L, 2L, "Hello", 2));
		expectedOutput.add(record(3L, 2L, "Hello world", 3));
		expectedOutput.add(record(4L, 3L, "Helloworld, how are you?", 4));
		expectedOutput.add(record(5L, 3L, "I am fine.", 5));
		expectedOutput.add(record(6L, 3L, "Luke Skywalker", 6));
		expectedOutput.add(record(7L, 4L, "Comment#1", 7));
		expectedOutput.add(record(8L, 4L, "Comment#2", 8));
		expectedOutput.add(record(9L, 4L, "Comment#3", 9));
		expectedOutput.add(new Watermark(9L));

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();

		expectedOutput.clear();

		operator = createSortOperator(inputRowType, rowTimeIdx, null);
		testHarness = createTestHarness(operator);
		testHarness.initializeState(snapshot);
		testHarness.open();
		// late data will be dropped
		testHarness.processElement(record(5L, 3L, "I am fine.", 6));
		testHarness.processWatermark(new Watermark(10L));

		expectedOutput.add(record(10L, 4L, "Comment#4", 10));
		expectedOutput.add(new Watermark(10L));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		// those watermark has no effect
		testHarness.processWatermark(new Watermark(11L));
		testHarness.processWatermark(new Watermark(12L));
		expectedOutput.add(new Watermark(11L));
		expectedOutput.add(new Watermark(12L));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	private RowTimeSortOperator createSortOperator(BaseRowTypeInfo inputRowType, int rowTimeIdx,
			GeneratedRecordComparator gComparator) {
		return new RowTimeSortOperator(inputRowType, rowTimeIdx, gComparator);
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(BaseTemporalSortOperator operator)
			throws Exception {
		OneInputStreamOperatorTestHarness testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
				operator, NullBinaryRowKeySelector.INSTANCE, NullBinaryRowKeySelector.INSTANCE.getProducedType());
		return testHarness;
	}

}
