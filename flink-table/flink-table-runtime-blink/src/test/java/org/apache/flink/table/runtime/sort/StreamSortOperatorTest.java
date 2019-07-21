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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Tests for {@link StreamSortOperator}.
 */
public class StreamSortOperatorTest {

	private BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(
			new VarCharType(VarCharType.MAX_LENGTH),
			new IntType());

	private GeneratedRecordComparator sortKeyComparator = new GeneratedRecordComparator("", "", new Object[0]) {

		private static final long serialVersionUID = -6067266199060901331L;

		@Override
		public RecordComparator newInstance(ClassLoader classLoader) {

			return new StringRecordComparator();
		}
	};

	private BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(inputRowType.getFieldTypes());

	@Test
	public void test() throws Exception {
		StreamSortOperator operator = createSortOperator();
		OneInputStreamOperatorTestHarness<BaseRow, BinaryRow> testHarness = createTestHarness(operator);
		testHarness.open();
		testHarness.processElement(record("hi", 1));
		testHarness.processElement(record("hello", 2));
		testHarness.processElement(record("world", 3));
		testHarness.processElement(record("word", 4));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("hello", 2));
		expectedOutput.add(record("hi", 1));
		expectedOutput.add(record("word", 4));
		expectedOutput.add(record("world", 3));

		// do a snapshot, data could be recovered from state
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
		operator.endInput();
		testHarness.close();
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

		expectedOutput.clear();

		operator = createSortOperator();
		testHarness = createTestHarness(operator);
		testHarness.initializeState(snapshot);
		testHarness.open();
		testHarness.processElement(record("abc", 1));
		testHarness.processElement(record("aa", 1));
		operator.endInput();
		testHarness.close();

		expectedOutput.add(record("aa", 1));
		expectedOutput.add(record("abc", 1));
		expectedOutput.add(record("hello", 2));
		expectedOutput.add(record("hi", 1));
		expectedOutput.add(record("word", 4));
		expectedOutput.add(record("world", 3));
		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	private StreamSortOperator createSortOperator() {
		return new StreamSortOperator(inputRowType, sortKeyComparator);
	}

	private OneInputStreamOperatorTestHarness createTestHarness(StreamSortOperator operator) throws Exception {
		OneInputStreamOperatorTestHarness testHarness = new OneInputStreamOperatorTestHarness(operator);
		return testHarness;
	}
}
