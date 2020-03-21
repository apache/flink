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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoStreamMap;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestBoundedMultipleInputOperator;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MultipleInputStreamTask}. Theses tests implicitly also test the
 * {@link StreamMultipleInputProcessor}.
 */
public class MultipleInputStreamTaskTest {

	/**
	 * This test verifies that open() and close() are correctly called. This test also verifies
	 * that timestamps of emitted elements are correct. {@link CoStreamMap} assigns the input
	 * timestamp to emitted elements.
	 */
	@Test
	public void testOpenCloseAndTimestamps() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.INT_TYPE_INFO)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperator())
				.build()) {

			long initialTime = 0L;
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			testHarness.processElement(new StreamRecord<>("Hello", initialTime + 1), 0);
			expectedOutput.add(new StreamRecord<>("Hello", initialTime + 1));
			testHarness.processElement(new StreamRecord<>(1337, initialTime + 2), 1);
			expectedOutput.add(new StreamRecord<>("1337", initialTime + 2));
			testHarness.processElement(new StreamRecord<>(42.44d, initialTime + 3), 2);
			expectedOutput.add(new StreamRecord<>("42.44", initialTime + 3));

			testHarness.endInput();
			testHarness.waitForTaskCompletion();

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	/**
	 * This test verifies that checkpoint barriers are correctly forwarded.
	 */
	@Test
	public void testCheckpointBarriers() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperator())
				.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			long initialTime = 0L;

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);

			// This element should be buffered since we received a checkpoint barrier on this input
			testHarness.processElement(new StreamRecord<>("Hello-0-0", initialTime), 0, 0);
			// This one should go through
			testHarness.processElement(new StreamRecord<>("Ciao-0-0", initialTime), 0, 1);
			expectedOutput.add(new StreamRecord<>("Ciao-0-0", initialTime));

			// These elements should be forwarded, since we did not yet receive a checkpoint barrier
			// on that input, only add to same input, otherwise we would not know the ordering
			// of the output since the Task might read the inputs in any order
			testHarness.processElement(new StreamRecord<>(11, initialTime), 1, 1);
			testHarness.processElement(new StreamRecord<>(1.0d, initialTime), 2, 0);
			expectedOutput.add(new StreamRecord<>("11", initialTime));
			expectedOutput.add(new StreamRecord<>("1.0", initialTime));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			// now we should see the barrier and after that the buffered elements
			expectedOutput.add(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()));
			expectedOutput.add(new StreamRecord<>("Hello-0-0", initialTime));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	/**
	 * This test verifies that checkpoint barriers and barrier buffers work correctly with
	 * concurrent checkpoint barriers where one checkpoint is "overtaking" another checkpoint, i.e.
	 * some inputs receive barriers from an earlier checkpoint, thereby blocking,
	 * then all inputs receive barriers from a later checkpoint.
	 */
	@Test
	public void testOvertakingCheckpointBarriers() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.INT_TYPE_INFO, 2)
				.addInput(BasicTypeInfo.DOUBLE_TYPE_INFO, 2)
				.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperator())
				.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			long initialTime = 0L;

			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);

			// These elements should be buffered until we receive barriers from
			// all inputs
			testHarness.processElement(new StreamRecord<>("Hello-0-0", initialTime), 0, 0);
			testHarness.processElement(new StreamRecord<>("Ciao-0-0", initialTime), 0, 0);

			// These elements should be forwarded, since we did not yet receive a checkpoint barrier
			// on that input, only add to same input, otherwise we would not know the ordering
			// of the output since the Task might read the inputs in any order
			testHarness.processElement(new StreamRecord<>("Witam-0-1", initialTime), 0, 1);
			testHarness.processElement(new StreamRecord<>(42, initialTime), 1, 1);
			testHarness.processElement(new StreamRecord<>(1.0d, initialTime), 2, 1);
			expectedOutput.add(new StreamRecord<>("Witam-0-1", initialTime));
			expectedOutput.add(new StreamRecord<>("42", initialTime));
			expectedOutput.add(new StreamRecord<>("1.0", initialTime));

			// we should not yet see the barrier, only the two elements from non-blocked input

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// Now give a later barrier to all inputs, this should unblock the first channel,
			// thereby allowing the two blocked elements through
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			expectedOutput.add(new CancelCheckpointMarker(0));
			expectedOutput.add(new StreamRecord<>("Hello-0-0", initialTime));
			expectedOutput.add(new StreamRecord<>("Ciao-0-0", initialTime));
			expectedOutput.add(new CheckpointBarrier(1, 1, CheckpointOptions.forCheckpointWithDefaultLocation()));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// Then give the earlier barrier, these should be ignored
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 0, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 1, 1);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 0);
			testHarness.processEvent(new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation()), 2, 1);

			testHarness.waitForTaskCompletion();
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	@Test
	public void testOperatorMetricReuse() throws Exception {

		TaskMetricGroup taskMetricGroup = new UnregisteredMetricGroups.UnregisteredTaskMetricGroup() {
			@Override
			public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String name) {
				return new OperatorMetricGroup(NoOpMetricRegistry.INSTANCE, this, operatorID, name);
			}
		};

		try (StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.setupOperatorChain(new DuplicatingOperator())
				.chain(new OneInputStreamTaskTest.DuplicatingOperator(), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.chain(new OneInputStreamTaskTest.DuplicatingOperator(), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.finish()
				.setTaskMetricGroup(taskMetricGroup)
				.build()) {
			Counter numRecordsInCounter = taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
			Counter numRecordsOutCounter = taskMetricGroup.getIOMetricGroup().getNumRecordsOutCounter();

			int numRecords1 = 5;
			int numRecords2 = 3;
			int numRecords3 = 2;
			for (int x = 0; x < numRecords1; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 0, 0);
			}
			for (int x = 0; x < numRecords2; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 1, 0);
			}
			for (int x = 0; x < numRecords3; x++) {
				testHarness.processElement(new StreamRecord<>("hello"), 2, 0);
			}

			int totalRecords = numRecords1 + numRecords2 + numRecords3;
			assertEquals(totalRecords, numRecordsInCounter.getCount());
			assertEquals((totalRecords) * 2 * 2 * 2, numRecordsOutCounter.getCount());
			testHarness.waitForTaskCompletion();
		}
	}

	static class DuplicatingOperator extends AbstractStreamOperator<String>
		implements MultipleInputStreamOperator<String> {

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(new DuplicatingInput(), new DuplicatingInput(), new DuplicatingInput());
		}

		class DuplicatingInput implements Input<String> {
			@Override
			public void processElement(StreamRecord<String> element) throws Exception {
				output.collect(element);
				output.collect(element);
			}
		}
	}

	@Test
	public void testClosingAllOperatorsOnChainProperly() throws Exception {
		StreamTaskMailboxTestHarness<String> testHarness =
			new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.setupOperatorChain(new TestBoundedMultipleInputOperator("Operator0"))
				.chain(new TestBoundedOneInputStreamOperator("Operator1"), BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
				.finish()
				.build();

		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		try {
			testHarness.processElement(new StreamRecord<>("Hello-1"), 0);
			testHarness.endInput(0);
			testHarness.process();

			testHarness.processElement(new StreamRecord<>("Hello-2"), 1);
			testHarness.processElement(new StreamRecord<>("Hello-3"), 2);
			testHarness.endInput(1);
			testHarness.process();
			testHarness.endInput(2);
			testHarness.process();
			assertEquals(
				true,
				testHarness.getStreamTask().getInputOutputJointFuture(InputStatus.NOTHING_AVAILABLE).isDone());

			testHarness.waitForTaskCompletion();
		}
		finally {
			testHarness.close();
		}

		expectedOutput.add(new StreamRecord<>("[Operator0-1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[Operator0-1]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0-2]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[Operator0-3]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[Operator0-2]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0-3]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator0]: Bye"));
		expectedOutput.add(new StreamRecord<>("[Operator1]: End of input"));
		expectedOutput.add(new StreamRecord<>("[Operator1]: Bye"));

		assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
	}

	@Test
	public void testInputFairness() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new MultipleInputStreamTaskTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperator())
					.build()) {
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			testHarness.setAutoProcess(false);
			testHarness.processElement(new StreamRecord<>("0"), 0);
			testHarness.processElement(new StreamRecord<>("1"), 0);
			testHarness.processElement(new StreamRecord<>("2"), 0);
			testHarness.processElement(new StreamRecord<>("3"), 0);

			testHarness.processElement(new StreamRecord<>("0"), 2);
			testHarness.processElement(new StreamRecord<>("1"), 2);

			testHarness.process();

			// We do not know which of the input will be picked first, but we are expecting them
			// to alternate
			// NOTE: the behaviour of alternation once per record is not part of any contract.
			// Task is just expected to not starve any of the inputs, it just happens to be
			// currently implemented in truly "fair" fashion. That means this test might need
			// to be adjusted if logic changes.
			expectedOutput.add(new StreamRecord<>("0"));
			expectedOutput.add(new StreamRecord<>("0"));
			expectedOutput.add(new StreamRecord<>("1"));
			expectedOutput.add(new StreamRecord<>("1"));
			expectedOutput.add(new StreamRecord<>("2"));
			expectedOutput.add(new StreamRecord<>("3"));

			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
		}
	}

	// This must only be used in one test, otherwise the static fields will be changed
	// by several tests concurrently
	private static class MapToStringMultipleInputOperator
			extends AbstractStreamOperator<String> implements MultipleInputStreamOperator<String> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled;
		private boolean closeCalled;

		@Override
		public void open() throws Exception {
			super.open();
			if (closeCalled) {
				Assert.fail("Close called before open.");
			}
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new MapToStringInput<String>(),
				new MapToStringInput<Integer>(),
				new MapToStringInput<Double>());
		}

		public boolean wasCloseCalled() {
			return closeCalled;
		}

		public class MapToStringInput<T> implements Input<T> {
			@Override
			public void processElement(StreamRecord<T> element) throws Exception {
				if (!openCalled) {
					Assert.fail("Open was not called before run.");
				}
				if (element.hasTimestamp()) {
					output.collect(new StreamRecord<>(element.getValue().toString(), element.getTimestamp()));
				}
				else {
					output.collect(new StreamRecord<>(element.getValue().toString()));
				}
			}
		}
	}

	private static class IdentityMap implements CoMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) {
			return value;
		}

		@Override
		public String map2(Integer value) {

			return value.toString();
		}
	}
}

