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

package org.apache.flink.table.runtime.deduplicate;

import org.apache.flink.api.common.time.Time;
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
 * Tests for {@link DeduplicateFunction}.
 */
public class DeduplicateFunctionTest extends BaseDeduplicateFunctionTest {

	private Time minTime = Time.milliseconds(10);
	private Time maxTime = Time.milliseconds(20);

	private DeduplicateFunction createFunction(boolean generateRetraction, boolean keepLastRow) {
		DeduplicateFunction func = new DeduplicateFunction(minTime.toMilliseconds(), maxTime.toMilliseconds(),
				inputRowType, generateRetraction, keepLastRow,
				generatedEqualiser);
		return func;
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			DeduplicateFunction func)
			throws Exception {
		KeyedProcessOperator operator = new KeyedProcessOperator(func);
		return new KeyedOneInputStreamOperatorTestHarness(operator, rowKeySelector, rowKeySelector.getProducedType());
	}

	@Test
	public void testKeepFirstRowWithoutGenerateRetraction() throws Exception {
		DeduplicateFunction func = createFunction(false, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 11));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testKeepFirstRowWithGenerateRetraction() throws Exception {
		DeduplicateFunction func = createFunction(true, false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		// Keep FirstRow in deduplicate will not send retraction
		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 11));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testKeepLastWithoutGenerateRetraction() throws Exception {
		DeduplicateFunction func = createFunction(false, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 1L, 13));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testKeepLastRowWithGenerateRetraction() throws Exception {
		DeduplicateFunction func = createFunction(true, true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 1L, 13));
		testHarness.close();

		// Keep LastRow in deduplicate may send retraction
		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(retractRecord("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 1L, 13));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}
}
