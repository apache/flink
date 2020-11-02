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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.TestHarnessUtil.buildSubtaskState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link StreamingGlobalCommitterOperator}.
 */
public class StreamingGlobalCommitterOperatorTest extends TestLogger {

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutSerializer() throws Exception {
		final OneInputStreamOperatorTestHarness<String, String> testHarness =
				createTestHarness(new TestSink.DefaultGlobalCommitter(), null);
		testHarness.initializeEmptyState();
		testHarness.open();
	}

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutCommitter() throws Exception {
		final OneInputStreamOperatorTestHarness<String, String> testHarness =
				createTestHarness(null, TestSink.StringCommittableSerializer.INSTANCE);
		testHarness.initializeEmptyState();
		testHarness.open();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void doNotSupportRetry() throws Exception {
		final List<String> input = Arrays.asList("lazy", "leaf");

		final OneInputStreamOperatorTestHarness<String, String> testHarness =
				createTestHarness(new TestSink.AlwaysRetryGlobalCommitter());

		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.processElements(input
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(1L, 1L);
		testHarness.notifyOfCompletedCheckpoint(1L);

		testHarness.close();
	}

	@Test
	public void closeCommitter() throws Exception {
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter();
		final OneInputStreamOperatorTestHarness<String, String> testHarness = createTestHarness(
				globalCommitter);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.close();
		assertThat(globalCommitter.isClosed(), is(true));
	}

	@Test
	public void restoredFromMergedState() throws Exception {

		final List<String> input1 = Arrays.asList("host", "drop");
		final OperatorSubtaskState operatorSubtaskState1 = buildSubtaskState(
				createTestHarness(),
				input1);

		final List<String> input2 = Arrays.asList("future", "evil", "how");
		final OperatorSubtaskState operatorSubtaskState2 = buildSubtaskState(
				createTestHarness(),
				input2);

		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter();
		final OneInputStreamOperatorTestHarness<String, String> testHarness = createTestHarness(
				globalCommitter);

		final OperatorSubtaskState mergedOperatorSubtaskState =
				OneInputStreamOperatorTestHarness.repackageState(
						operatorSubtaskState1,
						operatorSubtaskState2);

		testHarness.initializeState(
				OneInputStreamOperatorTestHarness.repartitionOperatorState(
						mergedOperatorSubtaskState, 2, 2, 1, 0));
		testHarness.open();

		final List<String> expectedOutput = new ArrayList<>();
		expectedOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(input1));
		expectedOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(input2));

		testHarness.snapshot(1L, 1L);
		testHarness.notifyOfCompletedCheckpoint(1L);
		testHarness.close();

		assertThat(
				globalCommitter.getCommittedData(),
				containsInAnyOrder(expectedOutput.toArray()));
	}

	@Test
	public void commitMultipleStagesTogether() throws Exception {

		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter();

		final List<String> input1 = Arrays.asList("cautious", "nature");
		final List<String> input2 = Arrays.asList("count", "over");
		final List<String> input3 = Arrays.asList("lawyer", "grammar");

		final List<String> expectedOutput = new ArrayList<>();

		expectedOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(input1));
		expectedOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(input2));
		expectedOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(input3));

		final OneInputStreamOperatorTestHarness<String, String> testHarness = createTestHarness(
				globalCommitter);
		testHarness.initializeEmptyState();
		testHarness.open();

		testHarness.processElements(input1
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(1L, 1L);
		testHarness.processElements(input2
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(2L, 2L);
		testHarness.processElements(input3
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(3L, 3L);

		testHarness.notifyOfCompletedCheckpoint(3L);

		testHarness.close();

		assertThat(
				testHarness.getOutput(),
				containsInAnyOrder(expectedOutput.stream().map(StreamRecord::new).toArray()));

		assertThat(
				globalCommitter.getCommittedData(),
				containsInAnyOrder(expectedOutput.toArray()));
	}

	@Test
	public void filterRecoveredCommittables() throws Exception {
		final List<String> input = Arrays.asList("silent", "elder", "patience");
		final String successCommittedCommittable = TestSink.DefaultGlobalCommitter.COMBINER.apply(
				input);

		final OperatorSubtaskState operatorSubtaskState = buildSubtaskState(
				createTestHarness(),
				input);
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				successCommittedCommittable);

		final OneInputStreamOperatorTestHarness<String, String> testHarness = createTestHarness(
				globalCommitter);

		// all data from previous checkpoint are expected to be committed,
		// so we expect no data to be re-committed.
		testHarness.initializeState(operatorSubtaskState);
		testHarness.open();
		testHarness.snapshot(1L, 1L);
		testHarness.notifyOfCompletedCheckpoint(1L);
		assertTrue(globalCommitter.getCommittedData().isEmpty());
		testHarness.close();
	}

	@Test
	public void endOfInput() throws Exception {
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter();

		final OneInputStreamOperatorTestHarness<String, String> testHarness = createTestHarness(
				globalCommitter);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.snapshot(1L, 1L);
		testHarness.endInput();
		testHarness.notifyOfCompletedCheckpoint(1L);
		testHarness.close();
		assertThat(
				globalCommitter.getCommittedData(),
				contains("end of input"));
	}

	private OneInputStreamOperatorTestHarness<String, String> createTestHarness() throws Exception {
		return createTestHarness(
				new TestSink.DefaultGlobalCommitter(),
				TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<String, String> createTestHarness(
			GlobalCommitter<String, String> globalCommitter) throws Exception {
		return createTestHarness(globalCommitter, TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<String, String> createTestHarness(
			GlobalCommitter<String, String> globalCommitter,
			SimpleVersionedSerializer<String> serializer) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
				new StreamingGlobalCommitterOperatorFactory<>(TestSink
						.newBuilder()
						.setGlobalCommitter(globalCommitter)
						.setGlobalCommittableSerializer(serializer)
						.build()),
				StringSerializer.INSTANCE);
	}
}
