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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestAnyModeMultipleInputStreamOperator;
import org.apache.flink.streaming.util.TestAnyModeMultipleInputStreamOperator.ToStringInput;
import org.apache.flink.streaming.util.TestSequentialMultipleInputStreamOperator;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test selective reading.
 */
public class StreamTaskMultipleInputSelectiveReadingTest {

	private static final StreamRecord<String>[] INPUT1 = new StreamRecord[] {
		new StreamRecord<>("Hello-1"),
		new StreamRecord<>("Hello-2"),
		new StreamRecord<>("Hello-3")
	};

	private static final StreamRecord<Integer>[] INPUT2 = new StreamRecord[] {
		new StreamRecord<>(1),
		new StreamRecord<>(2),
		new StreamRecord<>(3),
		new StreamRecord<>(4)
	};

	@Test
	public void testAnyOrderedReading() throws Exception {
		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		expectedOutput.add(new StreamRecord<>("[1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[2]: 1"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[2]: 2"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[2]: 3"));
		expectedOutput.add(new StreamRecord<>("[2]: 4"));

		testInputSelection(new TestAnyModeMultipleInputStreamOperator.Factory(), false, expectedOutput, true);
	}

	@Test
	public void testAnyUnorderedReading() throws Exception {
		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		expectedOutput.add(new StreamRecord<>("[1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[2]: 1"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[2]: 2"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[2]: 3"));
		expectedOutput.add(new StreamRecord<>("[2]: 4"));

		testInputSelection(new TestAnyModeMultipleInputStreamOperator.Factory(), true, expectedOutput, false);
	}

	@Test
	public void testSequentialReading() throws Exception {
		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		expectedOutput.add(new StreamRecord<>("[1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[2]: 1"));
		expectedOutput.add(new StreamRecord<>("[2]: 2"));
		expectedOutput.add(new StreamRecord<>("[2]: 3"));
		expectedOutput.add(new StreamRecord<>("[2]: 4"));

		testInputSelection(new TestSequentialMultipleInputStreamOperator.Factory(), true, expectedOutput, true);
	}

	@Test
	public void testSpecialRuleReading() throws Exception {
		ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
		expectedOutput.add(new StreamRecord<>("[1]: Hello-1"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-2"));
		expectedOutput.add(new StreamRecord<>("[2]: 1"));
		expectedOutput.add(new StreamRecord<>("[2]: 2"));
		expectedOutput.add(new StreamRecord<>("[1]: Hello-3"));
		expectedOutput.add(new StreamRecord<>("[2]: 3"));
		expectedOutput.add(new StreamRecord<>("[2]: 4"));

		testInputSelection(new SpecialRuleReadingStreamOperatorFactory(3, 4, 2), true, expectedOutput, true);
	}

	@Test
	public void testReadFinishedInput() throws Exception {
		try {
			testInputSelection(new TestReadFinishedInputStreamOperatorFactory(), true, new ArrayDeque<>(), true);
			fail("should throw an IOException");
		} catch (Exception t) {
			if (!ExceptionUtils.findThrowableWithMessage(t, "Can not make a progress: all selected inputs are already finished").isPresent()) {
				throw t;
			}
		}
	}

	private void testInputSelection(
			StreamOperatorFactory<String> streamOperatorFactory,
			boolean autoProcess,
			ArrayDeque<Object> expectedOutput,
			boolean orderedCheck) throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
			new StreamTaskMailboxTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.STRING_TYPE_INFO)
				.addInput(BasicTypeInfo.INT_TYPE_INFO)
				.setupOutputForSingletonOperatorChain(streamOperatorFactory)
				.build()) {

			testHarness.setAutoProcess(autoProcess);
			for (StreamRecord<String> record : INPUT1) {
				testHarness.processElement(record, 0);
			}
			for (StreamRecord<Integer> record : INPUT2) {
				testHarness.processElement(record, 1);
			}

			testHarness.endInput();

			if (!autoProcess) {
				testHarness.processAll();
			}
			testHarness.waitForTaskCompletion();

			if (orderedCheck) {
				assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));
			} else {
				assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
			}
		}
	}

	/**
	 * Setup three inputs only two selected and make sure that neither of the two inputs is starved,
	 * when one has some data all the time, but the other only rarely.
	 */
	@Test
	public void testInputStarvation() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new StreamTaskMailboxTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.addInput(BasicTypeInfo.STRING_TYPE_INFO)
					.setupOutputForSingletonOperatorChain(new TestInputStarvationMultipleInputOperatorFactory())
					.build()) {

			Queue<StreamRecord> expectedOutput = new ArrayDeque<>();

			testHarness.setAutoProcess(false);
			// StreamMultipleInputProcessor starts with all inputs available. Let it rotate and refresh properly.
			testHarness.processSingleStep();
			assertTrue(testHarness.getOutput().isEmpty());

			testHarness.processElement(new StreamRecord<>("NOT_SELECTED"), 0);

			testHarness.processElement(new StreamRecord<>("1"), 1);
			testHarness.processElement(new StreamRecord<>("2"), 1);
			testHarness.processElement(new StreamRecord<>("3"), 1);
			testHarness.processElement(new StreamRecord<>("4"), 1);

			testHarness.processSingleStep();
			expectedOutput.add(new StreamRecord<>("[2]: 1"));
			testHarness.processSingleStep();
			expectedOutput.add(new StreamRecord<>("[2]: 2"));
			assertThat(testHarness.getOutput(), contains(expectedOutput.toArray()));

			// InputGate 2 was not available in previous steps, so let's check if we are not starving it
			testHarness.processElement(new StreamRecord<>("1"), 2);
			testHarness.processSingleStep();
			testHarness.processSingleStep();

			// One of those processing single step should pick up InputGate 2, however it's not
			// important which one. We just must avoid starvation.
			expectedOutput.add(new StreamRecord<>("[3]: 1"));
			expectedOutput.add(new StreamRecord<>("[2]: 3"));

			assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
		}
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private static class SpecialRuleReadingStreamOperator extends AbstractStreamOperatorV2<String>
		implements MultipleInputStreamOperator<String>, InputSelectable, BoundedMultiInput {

		private final int input1Records;
		private final int input2Records;
		private final int maxContinuousReadingRecords;

		private int input1ReadingRecords;
		private int input2ReadingRecords;

		private int continuousReadingRecords;
		private InputSelection inputSelection = InputSelection.FIRST;

		SpecialRuleReadingStreamOperator(StreamOperatorParameters<String> parameters, int input1Records, int input2Records, int maxContinuousReadingRecords) {
			super(parameters, 2);

			this.input1Records = input1Records;
			this.input2Records = input2Records;
			this.maxContinuousReadingRecords = maxContinuousReadingRecords;
		}

		@Override
		public InputSelection nextSelection() {
			return inputSelection;
		}

		@Override
		public void endInput(int inputId) {
			inputSelection = (inputId == 1) ? InputSelection.SECOND : InputSelection.FIRST;
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new ToStringInput(this, 1) {
					@Override
					public void processElement(StreamRecord element) {
						super.processElement(element);
						input1ReadingRecords++;
						continuousReadingRecords++;
						if (continuousReadingRecords == maxContinuousReadingRecords) {
							continuousReadingRecords = 0;
							if (input2ReadingRecords < input2Records) {
								inputSelection = InputSelection.SECOND;
								return;
							}
						}

						inputSelection = InputSelection.FIRST;
					}
				},
				new ToStringInput(this, 2) {
					@Override
					public void processElement(StreamRecord element) {
						super.processElement(element);
						input2ReadingRecords++;
						continuousReadingRecords++;
						if (continuousReadingRecords == maxContinuousReadingRecords) {
							continuousReadingRecords = 0;
							if (input1ReadingRecords < input1Records) {
								inputSelection = InputSelection.FIRST;
								return;
							}
						}

						inputSelection = InputSelection.SECOND;
					}
				}
			);
		}
	}

	private static class SpecialRuleReadingStreamOperatorFactory extends AbstractStreamOperatorFactory<String> {
		private final int input1Records;
		private final int input2Records;
		private final int maxContinuousReadingRecords;

		public SpecialRuleReadingStreamOperatorFactory(
				int input1Records,
				int input2Records,
				int maxContinuousReadingRecords) {
			this.input1Records = input1Records;
			this.input2Records = input2Records;
			this.maxContinuousReadingRecords = maxContinuousReadingRecords;
		}

		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new SpecialRuleReadingStreamOperator(parameters, input1Records, input2Records, maxContinuousReadingRecords);
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return SpecialRuleReadingStreamOperator.class;
		}
	}

	private static class TestReadFinishedInputStreamOperator extends TestAnyModeMultipleInputStreamOperator {
		TestReadFinishedInputStreamOperator(StreamOperatorParameters<String> parameters) {
			super(parameters);
		}

		@Override
		public InputSelection nextSelection() {
			return InputSelection.FIRST;
		}
	}

	private static class TestReadFinishedInputStreamOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new TestReadFinishedInputStreamOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return TestReadFinishedInputStreamOperator.class;
		}
	}

	private static class TestInputStarvationMultipleInputOperator extends AbstractStreamOperatorV2<String>
		implements MultipleInputStreamOperator<String>, InputSelectable {

		public TestInputStarvationMultipleInputOperator(StreamOperatorParameters<String> parameters) {
			super(parameters, 3);
		}

		@Override
		public InputSelection nextSelection() {
			return new InputSelection.Builder().select(2).select(3).build();
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new ToStringInput(this, 1),
				new ToStringInput(this, 2),
				new ToStringInput(this, 3));
		}
	}

	private static class TestInputStarvationMultipleInputOperatorFactory extends AbstractStreamOperatorFactory<String> {
		@Override
		public <T extends StreamOperator<String>> T createStreamOperator(StreamOperatorParameters<String> parameters) {
			return (T) new TestInputStarvationMultipleInputOperator(parameters);
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return TestInputStarvationMultipleInputOperator.class;
		}
	}
}
