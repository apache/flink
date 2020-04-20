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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Tests for {@link DeduplicateKeepLastRowFunction}.
 */
public class DeduplicateKeepLastRowFunctionTest extends DeduplicateFunctionTestBase {

	private DeduplicateKeepLastRowFunction createFunction(boolean generateUpdateBefore) {
		return new DeduplicateKeepLastRowFunction(
			minTime.toMilliseconds(),
			inputRowType,
			generateUpdateBefore);
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			DeduplicateKeepLastRowFunction func)
			throws Exception {
		KeyedProcessOperator<BaseRow, BaseRow, BaseRow> operator = new KeyedProcessOperator<>(func);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, rowKeySelector, rowKeySelector.getProducedType());
	}

	@Test
	public void testWithoutGenerateUpdateBefore() throws Exception {
		DeduplicateKeepLastRowFunction func = createFunction(false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(record("book", 2L, 11));
		expectedOutput.add(record("book", 1L, 13));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testWithGenerateUpdateBefore() throws Exception {
		DeduplicateKeepLastRowFunction func = createFunction(true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		// Keep LastRow in deduplicate may send retraction
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(retractRecord("book", 1L, 12));
		expectedOutput.add(record("book", 1L, 13));
		expectedOutput.add(record("book", 2L, 11));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testWithGenerateUpdateBeforeAndStateTtl() throws Exception {
		DeduplicateKeepLastRowFunction func = createFunction(true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));

		testHarness.setStateTtlProcessingTime(30);
		testHarness.processElement(record("book", 1L, 17));
		testHarness.processElement(record("book", 2L, 18));
		testHarness.processElement(record("book", 1L, 19));

		// Keep LastRow in deduplicate may send retraction
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("book", 1L, 12));
		expectedOutput.add(retractRecord("book", 1L, 12));
		expectedOutput.add(record("book", 1L, 13));
		expectedOutput.add(record("book", 2L, 11));
		// because (2L,11), (1L,13) retired, so there is no retract message send to downstream
		expectedOutput.add(record("book", 1L, 17));
		expectedOutput.add(retractRecord("book", 1L, 17));
		expectedOutput.add(record("book", 1L, 19));
		expectedOutput.add(record("book", 2L, 18));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
