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
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link ProcTimeSortOperator}.
 */
public class ProcTimeSortOperatorTest {

	private InternalTypeInfo<RowData> inputRowType = InternalTypeInfo.ofFields(
			new IntType(),
			new BigIntType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new IntType());

	private GeneratedRecordComparator gComparator = new GeneratedRecordComparator("", "", new Object[0]) {

		private static final long serialVersionUID = -6067266199060901331L;

		@Override
		public RecordComparator newInstance(ClassLoader classLoader) {

			return IntRecordComparator.INSTANCE;
		}
	};

	private RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(inputRowType.toRowFieldTypes());

	@Test
	public void test() throws Exception {
		ProcTimeSortOperator operator = createSortOperator();
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(operator);
		testHarness.open();
		testHarness.setProcessingTime(0L);
		testHarness.processElement(insertRecord(3, 3L, "Hello world", 3));
		testHarness.processElement(insertRecord(2, 2L, "Hello", 2));
		testHarness.processElement(insertRecord(6, 2L, "Luke Skywalker", 6));
		testHarness.processElement(insertRecord(5, 3L, "I am fine.", 5));
		testHarness.processElement(insertRecord(7, 1L, "Comment#1", 7));
		testHarness.processElement(insertRecord(9, 4L, "Comment#3", 9));
		testHarness.setProcessingTime(1L);

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(2, 2L, "Hello", 2));
		expectedOutput.add(insertRecord(3, 3L, "Hello world", 3));
		expectedOutput.add(insertRecord(5, 3L, "I am fine.", 5));
		expectedOutput.add(insertRecord(6, 2L, "Luke Skywalker", 6));
		expectedOutput.add(insertRecord(7, 1L, "Comment#1", 7));
		expectedOutput.add(insertRecord(9, 4L, "Comment#3", 9));
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		testHarness.processElement(insertRecord(10, 4L, "Comment#4", 10));
		testHarness.processElement(insertRecord(8, 4L, "Comment#2", 8));
		testHarness.processElement(insertRecord(1, 1L, "Hi", 2));
		testHarness.processElement(insertRecord(1, 1L, "Hi", 1));
		testHarness.processElement(insertRecord(4, 3L, "Helloworld, how are you?", 4));
		testHarness.processElement(insertRecord(4, 5L, "Hello, how are you?", 4));

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		testHarness.close();

		expectedOutput.clear();

		operator = createSortOperator();
		testHarness = createTestHarness(operator);
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(insertRecord(5, 3L, "I am fine.", 6));
		testHarness.setProcessingTime(1L);

		expectedOutput.add(insertRecord(1, 1L, "Hi", 2));
		expectedOutput.add(insertRecord(1, 1L, "Hi", 1));
		expectedOutput.add(insertRecord(4, 3L, "Helloworld, how are you?", 4));
		expectedOutput.add(insertRecord(4, 5L, "Hello, how are you?", 4));
		expectedOutput.add(insertRecord(5, 3L, "I am fine.", 6));
		expectedOutput.add(insertRecord(8, 4L, "Comment#2", 8));
		expectedOutput.add(insertRecord(10, 4L, "Comment#4", 10));
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	private ProcTimeSortOperator createSortOperator() {
		return new ProcTimeSortOperator(inputRowType, gComparator);
	}

	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(BaseTemporalSortOperator operator)
			throws Exception {
		OneInputStreamOperatorTestHarness testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
				operator, EmptyRowDataKeySelector.INSTANCE, EmptyRowDataKeySelector.INSTANCE.getProducedType());
		return testHarness;
	}

}
