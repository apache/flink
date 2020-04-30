/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 * Tests to guard rescaling from checkpoint.
 */
public class RocksIncrementalCheckpointRescalingTest extends TestLogger {

	@Rule
	public TemporaryFolder rootFolder = new TemporaryFolder();

	private final int maxParallelism = 10;

	private KeySelector<String, String> keySelector = new TestKeySelector();

	private String[] records;

	@Before
	public void initRecords() throws Exception {
		records = new String[10];
		records[0] = "8";
		Assert.assertEquals(0, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[0]), maxParallelism)); // group 0

		records[1] = "5";
		Assert.assertEquals(1, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[1]), maxParallelism)); // group 1

		records[2] = "25";
		Assert.assertEquals(2, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[2]), maxParallelism)); // group 2

		records[3] = "13";
		Assert.assertEquals(3, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[3]), maxParallelism)); // group 3

		records[4] = "4";
		Assert.assertEquals(4, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[4]), maxParallelism)); // group 4

		records[5] = "7";
		Assert.assertEquals(5, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[5]), maxParallelism)); // group 5

		records[6] = "1";
		Assert.assertEquals(6, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[6]), maxParallelism)); // group 6

		records[7] = "6";
		Assert.assertEquals(7, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[7]), maxParallelism)); // group 7

		records[8] = "9";
		Assert.assertEquals(8, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[8]), maxParallelism)); // group 8

		records[9] = "3";
		Assert.assertEquals(9, KeyGroupRangeAssignment.assignToKeyGroup(keySelector.getKey(records[9]), maxParallelism)); // group 9
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testScalingUp() throws Exception {

		// -----------------------------------------> test with initial parallelism 1 <---------------------------------------

		OperatorSubtaskState snapshot;

		try (
			KeyedOneInputStreamOperatorTestHarness<String, String, Integer> harness =
				getHarnessTest(keySelector, maxParallelism, 1, 0)) {
			harness.setStateBackend(getStateBackend());
			harness.open();

			validHarnessResult(harness, 1, records);

			snapshot = harness.snapshot(0, 0);
		}

		// -----------------------------------------> test rescaling from 1 to 2 <---------------------------------------

		// init state for new subtask-0
		OperatorSubtaskState initState1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot, maxParallelism, 1, 2, 0);

		// init state for new subtask-1
		OperatorSubtaskState initState2 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot, maxParallelism, 1, 2, 1);

		KeyedOneInputStreamOperatorTestHarness<String, String, Integer>[] harness2 =
			new KeyedOneInputStreamOperatorTestHarness[3];

		OperatorSubtaskState snapshot2;

		try {
			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
				maxParallelism,
				2);

			// task's key-group [0, 4]
			KeyGroupRange localKeyGroupRange20 = keyGroupPartitions.get(0);
			Assert.assertEquals(new KeyGroupRange(0, 4), localKeyGroupRange20);
			harness2[0] = getHarnessTest(keySelector, maxParallelism, 2, 0);
			harness2[0].setStateBackend(getStateBackend());
			harness2[0].setup();
			harness2[0].initializeState(initState1);
			harness2[0].open();

			// task's key-group [5, 9]
			KeyGroupRange localKeyGroupRange21 = keyGroupPartitions.get(1);
			Assert.assertEquals(new KeyGroupRange(5, 9), localKeyGroupRange21);
			harness2[1] = getHarnessTest(keySelector, maxParallelism, 2, 1);
			harness2[1].setStateBackend(getStateBackend());
			harness2[1].setup();
			harness2[1].initializeState(initState2);
			harness2[1].open();

			validHarnessResult(harness2[0], 2, records[0], records[1], records[2], records[3], records[4]);

			validHarnessResult(harness2[1], 2, records[5], records[6], records[7], records[8], records[9]);

			snapshot2 = AbstractStreamOperatorTestHarness.repackageState(
				harness2[0].snapshot(0, 0),
				harness2[1].snapshot(0, 0)
			);

			validHarnessResult(harness2[0], 1, records[5], records[6], records[7], records[8], records[9]);

			validHarnessResult(harness2[1], 1, records[0], records[1], records[2], records[3], records[4]);
		} finally {
			closeHarness(harness2);
		}

		// -----------------------------------------> test rescaling from 2 to 3 <---------------------------------------

		// init state for new subtask-0
		initState1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot2, maxParallelism, 2, 3, 0);

		// init state for new subtask-1
		initState2 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot2, maxParallelism, 2, 3, 1);

		// init state for new subtask-2
		OperatorSubtaskState initState3 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot2, maxParallelism, 2, 3, 2);

		KeyedOneInputStreamOperatorTestHarness<String, String, Integer>[] harness3 =
			new KeyedOneInputStreamOperatorTestHarness[3];

		try {
			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
				maxParallelism,
				3);

			// task's key-group [0, 3]
			// this will choose the state handle to harness2[0] to init the target db with clipping.
			KeyGroupRange localKeyGroupRange30 = keyGroupPartitions.get(0);
			Assert.assertEquals(new KeyGroupRange(0, 3), localKeyGroupRange30);
			harness3[0] = getHarnessTest(keySelector, maxParallelism, 3, 0);
			harness3[0].setStateBackend(getStateBackend());
			harness3[0].setup();
			harness3[0].initializeState(initState1);
			harness3[0].open();

			// task's key-group [4, 6]
			KeyGroupRange localKeyGroupRange31 = keyGroupPartitions.get(1);
			Assert.assertEquals(new KeyGroupRange(4, 6), localKeyGroupRange31);
			harness3[1] = getHarnessTest(keySelector, maxParallelism, 3, 1);
			harness3[1].setStateBackend(getStateBackend());
			harness3[1].setup();
			harness3[1].initializeState(initState2);
			harness3[1].open();

			// task's key-group [7, 9]
			KeyGroupRange localKeyGroupRange32 = keyGroupPartitions.get(2);
			Assert.assertEquals(new KeyGroupRange(7, 9), localKeyGroupRange32);
			harness3[2] = getHarnessTest(keySelector, maxParallelism, 3, 2);
			harness3[2].setStateBackend(getStateBackend());
			harness3[2].setup();
			harness3[2].initializeState(initState3);
			harness3[2].open();

			validHarnessResult(harness3[0], 3, records[0], records[1], records[2], records[3]);
			validHarnessResult(harness3[1], 3, records[4], records[5], records[6]);
			validHarnessResult(harness3[2], 3, records[7], records[8], records[9]);

			validHarnessResult(harness3[0], 1, records[4], records[5], records[6], records[7], records[8], records[9]);
			validHarnessResult(harness3[1], 1, records[0], records[1], records[2], records[3], records[7], records[8], records[9]);
			validHarnessResult(harness3[2], 1, records[0], records[1], records[2], records[3], records[4], records[5], records[6]);
		} finally {
			closeHarness(harness3);
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testScalingDown() throws Exception {

		// -----------------------------------------> test with initial parallelism 3 <---------------------------------------

		KeyedOneInputStreamOperatorTestHarness<String, String, Integer>[] harness3 = new KeyedOneInputStreamOperatorTestHarness[3];
		OperatorSubtaskState snapshot3;

		try {
			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
				maxParallelism,
				3);

			// task's key-group [0, 3], this should trigger the condition to use clip
			KeyGroupRange localKeyGroupRange30 = keyGroupPartitions.get(0);
			Assert.assertEquals(new KeyGroupRange(0, 3), localKeyGroupRange30);
			harness3[0] = getHarnessTest(keySelector, maxParallelism, 3, 0);
			harness3[0].setStateBackend(getStateBackend());
			harness3[0].open();

			// task's key-group [4, 6]
			KeyGroupRange localKeyGroupRange31 = keyGroupPartitions.get(1);
			Assert.assertEquals(new KeyGroupRange(4, 6), localKeyGroupRange31);
			harness3[1] = getHarnessTest(keySelector, maxParallelism, 3, 1);
			harness3[1].setStateBackend(getStateBackend());
			harness3[1].open();

			// task's key-group [7, 9]
			KeyGroupRange localKeyGroupRange32 = keyGroupPartitions.get(2);
			Assert.assertEquals(new KeyGroupRange(7, 9), localKeyGroupRange32);
			harness3[2] = getHarnessTest(keySelector, maxParallelism, 3, 2);
			harness3[2].setStateBackend(getStateBackend());
			harness3[2].open();

			validHarnessResult(harness3[0], 1, records[0], records[1], records[2], records[3]);
			validHarnessResult(harness3[1], 1, records[4], records[5], records[6]);
			validHarnessResult(harness3[2], 1, records[7], records[8], records[9]);

			snapshot3 = AbstractStreamOperatorTestHarness.repackageState(
				harness3[0].snapshot(0, 0),
				harness3[1].snapshot(0, 0),
				harness3[2].snapshot(0, 0)
			);

		} finally {
			closeHarness(harness3);
		}

		// -----------------------------------------> test rescaling from 3 to 2 <---------------------------------------

		// init state for new subtask-0
		OperatorSubtaskState initState1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot3, maxParallelism, 3, 2, 0);

		// init state for new subtask-1
		OperatorSubtaskState initState2 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot3, maxParallelism, 3, 2, 1);

		KeyedOneInputStreamOperatorTestHarness<String, String, Integer>[] harness2 =
			new KeyedOneInputStreamOperatorTestHarness[3];

		OperatorSubtaskState snapshot2;

		try {
			List<KeyGroupRange> keyGroupPartitions = StateAssignmentOperation.createKeyGroupPartitions(
				maxParallelism,
				2);

			// task's key-group [0, 4]
			// this will choose the state handle generated by harness3[0] to init the target db without any clipping.
			KeyGroupRange localKeyGroupRange20 = keyGroupPartitions.get(0);
			Assert.assertEquals(new KeyGroupRange(0, 4), localKeyGroupRange20);
			harness2[0] = getHarnessTest(keySelector, maxParallelism, 2, 0);
			harness2[0].setStateBackend(getStateBackend());
			harness2[0].setup();
			harness2[0].initializeState(initState1);
			harness2[0].open();

			// task's key-group [5, 9], this will open a empty db, and insert records from two state handles.
			KeyGroupRange localKeyGroupRange21 = keyGroupPartitions.get(1);
			Assert.assertEquals(new KeyGroupRange(5, 9), localKeyGroupRange21);
			harness2[1] = getHarnessTest(keySelector, maxParallelism, 2, 1);
			harness2[1].setStateBackend(getStateBackend());
			harness2[1].setup();
			harness2[1].initializeState(initState2);
			harness2[1].open();

			validHarnessResult(harness2[0], 2, records[0], records[1], records[2], records[3], records[4]);

			validHarnessResult(harness2[1], 2, records[5], records[6], records[7], records[8], records[9]);

			snapshot2 = AbstractStreamOperatorTestHarness.repackageState(
				harness2[0].snapshot(0, 0),
				harness2[1].snapshot(0, 0)
			);

			validHarnessResult(harness2[0], 1, records[5], records[6], records[7], records[8], records[9]);

			validHarnessResult(harness2[1], 1, records[0], records[1], records[2], records[3], records[4]);
		} finally {
			closeHarness(harness2);
		}

		// -----------------------------------------> test rescaling from 2 to 1 <---------------------------------------

		// init state for new subtask-0
		initState1 = AbstractStreamOperatorTestHarness.repartitionOperatorState(
			snapshot2, maxParallelism, 2, 1, 0);

		try (
			KeyedOneInputStreamOperatorTestHarness<String, String, Integer> harness =
				getHarnessTest(keySelector, maxParallelism, 1, 0)) {

			// this will choose the state handle generated by harness2[0] to init the target db without any clipping.
			harness.setStateBackend(getStateBackend());
			harness.setup();
			harness.initializeState(initState1);
			harness.open();

			validHarnessResult(harness, 3, records);
		}
	}

	private void closeHarness(KeyedOneInputStreamOperatorTestHarness<?, ?, ?>[] harnessArr) throws Exception {
		for (KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness : harnessArr) {
			if (harness != null) {
				harness.close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void validHarnessResult(
		KeyedOneInputStreamOperatorTestHarness<?, String, ?> harness,
		Integer expectedValue,
		String... records) throws Exception {
		for (String record : records) {
			harness.processElement(new StreamRecord<>(record, 1));
			StreamRecord<Integer> outputRecord = (StreamRecord<Integer>) harness.getOutput().poll();
			Assert.assertNotNull(outputRecord);
			Assert.assertEquals(expectedValue, outputRecord.getValue());
		}
	}

	private KeyedOneInputStreamOperatorTestHarness<String, String, Integer> getHarnessTest(
		KeySelector<String, String> keySelector,
		int maxParallelism,
		int taskParallelism,
		int subtaskIdx) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(
			new KeyedProcessOperator<>(new TestKeyedFunction()),
			keySelector,
			BasicTypeInfo.STRING_TYPE_INFO,
			maxParallelism,
			taskParallelism,
			subtaskIdx);
	}

	private StateBackend getStateBackend() throws Exception {
		return new RocksDBStateBackend("file://" + rootFolder.newFolder().getAbsolutePath(), true);
	}

	/**
	 * A simple keyed function for tests.
	 */
	private class TestKeyedFunction extends KeyedProcessFunction<String, String, Integer> {

		private ValueState<Integer> counterState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			counterState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("counter", Integer.class));
		}

		@Override
		public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
			Integer oldCount = counterState.value();
			Integer newCount = oldCount != null ? oldCount + 1 : 1;
			counterState.update(newCount);
			out.collect(newCount);
		}
	}

	/**
	 * A simple key selector for tests.
	 */
	private class TestKeySelector implements KeySelector<String, String> {
		@Override
		public String getKey(String value) throws Exception {
			return value;
		}
	}
}
