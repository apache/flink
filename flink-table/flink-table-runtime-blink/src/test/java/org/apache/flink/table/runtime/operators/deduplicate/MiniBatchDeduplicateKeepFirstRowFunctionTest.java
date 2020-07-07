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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link MiniBatchDeduplicateKeepFirstRowFunction}.
 */
public class MiniBatchDeduplicateKeepFirstRowFunctionTest extends DeduplicateFunctionTestBase {

	private TypeSerializer<RowData> typeSerializer = inputRowType.createSerializer(new ExecutionConfig());

	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
			MiniBatchDeduplicateKeepFirstRowFunction func)
			throws Exception {
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeyedMapBundleOperator op = new KeyedMapBundleOperator(func, trigger);
		return new KeyedOneInputStreamOperatorTestHarness<>(op, rowKeySelector, rowKeySelector.getProducedType());
	}

	@Test
	public void testKeepFirstRowWithGenerateUpdateBefore() throws Exception {
		MiniBatchDeduplicateKeepFirstRowFunction func = new MiniBatchDeduplicateKeepFirstRowFunction(typeSerializer, minTime.toMilliseconds());
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 2L, 11));

		// output is empty because bundle not trigger yet.
		Assert.assertTrue(testHarness.getOutput().isEmpty());

		testHarness.processElement(insertRecord("book", 1L, 13));

		// Keep FirstRow in deduplicate will not send retraction
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("book", 1L, 12));
		expectedOutput.add(insertRecord("book", 2L, 11));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testKeepFirstRowWithStateTtl() throws Exception {
		MiniBatchDeduplicateKeepFirstRowFunction func = new MiniBatchDeduplicateKeepFirstRowFunction(typeSerializer, minTime.toMilliseconds());
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
		testHarness.setup();
		testHarness.open();
		testHarness.processElement(insertRecord("book", 1L, 12));
		testHarness.processElement(insertRecord("book", 2L, 11));
		// output is empty because bundle not trigger yet.
		Assert.assertTrue(testHarness.getOutput().isEmpty());
		testHarness.processElement(insertRecord("book", 1L, 13));

		testHarness.setStateTtlProcessingTime(30);
		testHarness.processElement(insertRecord("book", 1L, 17));
		testHarness.processElement(insertRecord("book", 2L, 18));
		testHarness.processElement(insertRecord("book", 1L, 19));

		// Keep FirstRow in deduplicate
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("book", 1L, 12));
		expectedOutput.add(insertRecord("book", 2L, 11));
		//(1L,12),(2L,11) has retired, so output (1L,17) and (2L,18)
		expectedOutput.add(insertRecord("book", 1L, 17));
		expectedOutput.add(insertRecord("book", 2L, 18));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
