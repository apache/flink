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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.retractRecord;

/**
 * Tests for {@link MiniBatchDeduplicateKeepLastRowFunction}.
 */
public class MiniBatchDeduplicateKeepLastRowFunctionTest {

	private BaseRowTypeInfo inputRowType = new BaseRowTypeInfo(InternalTypes.STRING, InternalTypes.LONG,
			InternalTypes.INT);

	private int rowKeyIdx = 1;
	private BinaryRowKeySelector rowKeySelector = new BinaryRowKeySelector(new int[] { rowKeyIdx },
			inputRowType.getInternalTypes());


	private BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
			inputRowType.getFieldTypes(),
			new GenericRowRecordSortComparator(rowKeyIdx, inputRowType.getInternalTypes()[rowKeyIdx]));

	private TypeSerializer<BaseRow> typeSerializer = inputRowType.createSerializer(new ExecutionConfig());

	private MiniBatchDeduplicateKeepLastRowFunction createFunction(boolean generateRetraction) {
		return new MiniBatchDeduplicateKeepLastRowFunction(inputRowType, generateRetraction, typeSerializer);
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createTestHarness(
			MiniBatchDeduplicateKeepLastRowFunction func)
			throws Exception {
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeyedMapBundleOperator op = new KeyedMapBundleOperator(func, trigger);
		return new KeyedOneInputStreamOperatorTestHarness<>(op, rowKeySelector, rowKeySelector.getProducedType());
	}

	@Test
	public void testWithoutGenerateRetraction() throws Exception {
		MiniBatchDeduplicateKeepLastRowFunction func = createFunction(false);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 10));
		testHarness.processElement(record("book", 2L, 11));
		// output is empty because bundle not trigger yet.
		Assert.assertTrue(testHarness.getOutput().isEmpty());

		testHarness.processElement(record("book", 1L, 13));

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 1L, 13));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());

		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 11));

		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 3L, 11));
		testHarness.close();
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}

	@Test
	public void testWithGenerateRetraction() throws Exception {
		MiniBatchDeduplicateKeepLastRowFunction func = createFunction(true);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(record("book", 1L, 10));
		testHarness.processElement(record("book", 2L, 11));
		// output is empty because bundle not trigger yet.
		Assert.assertTrue(testHarness.getOutput().isEmpty());

		testHarness.processElement(record("book", 1L, 13));

		List<Object> expectedOutputOutput = new ArrayList<>();
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 1L, 13));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());

		testHarness.processElement(record("book", 1L, 12));
		testHarness.processElement(record("book", 2L, 11));
		testHarness.processElement(record("book", 3L, 11));

		// this will send retract message to downstream
		expectedOutputOutput.add(retractRecord("book", 1L, 13));
		expectedOutputOutput.add(record("book", 1L, 12));
		expectedOutputOutput.add(retractRecord("book", 2L, 11));
		expectedOutputOutput.add(record("book", 2L, 11));
		expectedOutputOutput.add(record("book", 3L, 11));
		testHarness.close();
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutputOutput, testHarness.getOutput());
	}
}
