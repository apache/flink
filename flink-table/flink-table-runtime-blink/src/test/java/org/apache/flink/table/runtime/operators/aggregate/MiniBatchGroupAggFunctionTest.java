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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;

/**
 * Tests for {@link MiniBatchGroupAggFunction}.
 */
public class MiniBatchGroupAggFunctionTest extends GroupAggFunctionTestBase {

	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
		MiniBatchGroupAggFunction aggFunction) throws Exception {
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeyedMapBundleOperator operator = new KeyedMapBundleOperator(aggFunction, trigger);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}

	private MiniBatchGroupAggFunction createFunction(boolean generateUpdateBefore) throws Exception {
		return new MiniBatchGroupAggFunction(
			function,
			equaliser,
			accTypes,
			RowType.of(inputFieldTypes),
			-1,
			false,
			minTime.toMilliseconds());
	}

	@Test
	public void testMiniBatchGroupAggWithStateTtl() throws Exception {

		MiniBatchGroupAggFunction function = createFunction(false);
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(function);
		testHarness.open();
		testHarness.setup();

		testHarness.processElement(insertRecord("key1", 1, 20L));
		testHarness.processElement(insertRecord("key2", 1, 3000L));
		testHarness.processElement(insertRecord("key1", 3, 999L));

		testHarness.processElement(insertRecord("key1", 2, 500L));
		testHarness.processElement(insertRecord("key2", 2, 3999L));
		testHarness.processElement(insertRecord("key2", 3, 1000L));

		//trigger expired state cleanup
		testHarness.setStateTtlProcessingTime(20);
		testHarness.processElement(insertRecord("key1", 4, 1020L));
		testHarness.processElement(insertRecord("key1", 5, 1290L));
		testHarness.processElement(insertRecord("key1", 6, 1290L));

		testHarness.processElement(insertRecord("key2", 4, 4999L));
		testHarness.processElement(insertRecord("key2", 5, 6000L));
		testHarness.processElement(insertRecord("key2", 6, 2000L));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("key1", 4L, 2L));
		expectedOutput.add(insertRecord("key2", 1L, 1L));
		expectedOutput.add(updateAfterRecord("key1", 6L, 3L));
		expectedOutput.add(updateAfterRecord("key2", 6L, 3L));
		//result doesn`t contain expired record with the same key
		expectedOutput.add(insertRecord("key1", 15L, 3L));
		expectedOutput.add(insertRecord("key2", 15L, 3L));

		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
