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

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
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

import static org.apache.flink.streaming.util.SinkTestUtil.convertStringListToByteArrayList;
import static org.apache.flink.streaming.util.TestHarnessUtil.buildSubtaskState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link GlobalStreamingCommitterOperator}.
 */
public class GlobalStreamingCommitterOperatorTest extends TestLogger {

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutSerializer() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(new TestSink.DefaultGlobalCommitter(""), null);
		testHarness.initializeEmptyState();
		testHarness.open();
	}

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutCommitter() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(null, TestSink.StringCommittableSerializer.INSTANCE);
		testHarness.initializeEmptyState();
		testHarness.open();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void doNotSupportRetry() throws Exception {
		final List<byte[]> input = convertStringListToByteArrayList(Arrays.asList("lazy", "leaf"));

		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
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
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				globalCommitter);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.close();
		assertThat(globalCommitter.isClosed(), is(true));
	}

	@Test
	public void restoredFromMergedState() throws Exception {

		final List<String> stringInput1 = Arrays.asList("host", "drop");
		final List<String> stringInput2 = Arrays.asList("future", "evil", "how");

		final List<byte[]> byteInput1 = convertStringListToByteArrayList(stringInput1);
		final List<byte[]> byteInput2 = convertStringListToByteArrayList(stringInput2);

		final OperatorSubtaskState operatorSubtaskState1 = buildSubtaskState(
				createTestHarness(),
				byteInput1);

		final OperatorSubtaskState operatorSubtaskState2 = buildSubtaskState(
				createTestHarness(),
				byteInput2);

		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				globalCommitter);

		final OperatorSubtaskState mergedOperatorSubtaskState =
				OneInputStreamOperatorTestHarness.repackageState(
						operatorSubtaskState1,
						operatorSubtaskState2);

		testHarness.initializeState(
				OneInputStreamOperatorTestHarness.repartitionOperatorState(
						mergedOperatorSubtaskState, 2, 2, 1, 0));
		testHarness.open();

		final List<String> expectedStringOutput = new ArrayList<>();
		expectedStringOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(stringInput1));
		expectedStringOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(stringInput2));


		testHarness.snapshot(1L, 1L);
		testHarness.notifyOfCompletedCheckpoint(1L);
		testHarness.close();

		assertThat(testHarness.getOutput().size(), equalTo(0));

		assertThat(
				globalCommitter.getCommittedData(),
				containsInAnyOrder(expectedStringOutput.toArray()));
	}

	@Test
	public void commitMultipleStagesTogether() throws Exception {

		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");

		final List<String> stringInput1 = Arrays.asList("cautious", "nature");
		final List<String> stringInput2 = Arrays.asList("count", "over");
		final List<String> stringInput3 = Arrays.asList("lawyer", "grammar");

		final List<byte[]> input1 = convertStringListToByteArrayList(stringInput1);
		final List<byte[]> input2 = convertStringListToByteArrayList(stringInput2);
		final List<byte[]> input3 = convertStringListToByteArrayList(stringInput3);

		final List<String> expectedStringOutput = new ArrayList<>();

		expectedStringOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(stringInput1));
		expectedStringOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(stringInput2));
		expectedStringOutput.add(TestSink.DefaultGlobalCommitter.COMBINER.apply(stringInput3));

		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
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

		assertThat(testHarness.getOutput().size(), equalTo(0));

		assertThat(
				globalCommitter.getCommittedData(),
				containsInAnyOrder(expectedStringOutput.toArray()));
	}

	@Test
	public void filterRecoveredCommittables() throws Exception {
		final List<String> stringInput = Arrays.asList("silent", "elder", "patience");
		final List<byte[]> byteInput = convertStringListToByteArrayList(stringInput);
		final String successCommittedCommittable = TestSink.DefaultGlobalCommitter.COMBINER.apply(
				stringInput);

		final OperatorSubtaskState operatorSubtaskState = buildSubtaskState(
				createTestHarness(),
				byteInput);
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				successCommittedCommittable);

		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
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
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");

		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
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

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness() throws Exception {
		return createTestHarness(
				new TestSink.DefaultGlobalCommitter(""),
				TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
			GlobalCommitter<String, String> globalCommitter) throws Exception {
		return createTestHarness(globalCommitter, TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
			GlobalCommitter<String, String> globalCommitter,
			SimpleVersionedSerializer<String> serializer) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
				new GlobalStreamingCommitterOperatorFactory<>(TestSink
						.newBuilder()
						.addWriter()
						.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addGlobalCommitter(globalCommitter)
						.setGlobalCommittableSerializer(serializer)
						.build()),
				BytePrimitiveArraySerializer.INSTANCE);
	}
}
