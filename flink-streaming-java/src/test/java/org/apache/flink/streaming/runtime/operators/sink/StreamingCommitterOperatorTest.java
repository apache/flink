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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.SinkTestUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.SinkTestUtil.containsStreamElementsInAnyOrder;
import static org.apache.flink.streaming.util.TestHarnessUtil.buildSubtaskState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;


/**
 * Test the {@link StreamingCommitterOperator}.
 *
 * <p>This has no tests of its own because {@link StreamingCommitterOperator} has no functionality
 * beyond what {@link AbstractStreamingCommitterOperator} provides.
 */
public class StreamingCommitterOperatorTest extends TestLogger {

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutSerializer() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(new TestSink.DefaultCommitter(), null);
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
		final List<byte[]> input = SinkTestUtil.convertStringListToByteArrayList(Arrays.asList("lazy", "leaf"));
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(new TestSink.AlwaysRetryCommitter());

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
		final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				committer);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.close();
		assertThat(committer.isClosed(), is(true));
	}

	@Test
	public void restoredFromMergedState() throws Exception {

		final List<String> stringInput1 = Arrays.asList("today", "whom");
		final List<String> stringInput2 = Arrays.asList("future", "evil", "how");
		final List<byte[]> byteInput1 = SinkTestUtil.convertStringListToByteArrayList(stringInput1);
		final List<byte[]> byteInput2 = SinkTestUtil.convertStringListToByteArrayList(stringInput2);

		final OperatorSubtaskState operatorSubtaskState1 = buildSubtaskState(
				createTestHarness(),
				byteInput1);

		final OperatorSubtaskState operatorSubtaskState2 = buildSubtaskState(
				createTestHarness(),
				byteInput2);

		final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				committer);

		final OperatorSubtaskState mergedOperatorSubtaskState =
				OneInputStreamOperatorTestHarness.repackageState(
						operatorSubtaskState1,
						operatorSubtaskState2);

		testHarness.initializeState(
				OneInputStreamOperatorTestHarness.repartitionOperatorState(
						mergedOperatorSubtaskState, 2, 2, 1, 0));
		testHarness.open();

		final List<byte[]> expectedByteOutput = new ArrayList<>();
		expectedByteOutput.addAll(byteInput1);
		expectedByteOutput.addAll(byteInput2);

		final List<String> expectedStringOutput = new ArrayList<>();
		expectedStringOutput.addAll(stringInput1);
		expectedStringOutput.addAll(stringInput2);

		testHarness.snapshot(1L, 1L);
		testHarness.notifyOfCompletedCheckpoint(1);

		testHarness.close();

		assertThat(
				testHarness.getOutput(),
				containsStreamElementsInAnyOrder(expectedByteOutput
						.stream()
						.map(StreamRecord::new)
						.toArray()));

		assertThat(
				committer.getCommittedData(),
				containsInAnyOrder(expectedStringOutput.toArray()));
	}

	@Test
	public void commitMultipleStagesTogether() throws Exception {

		final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();

		final List<String> stringInput1 = Arrays.asList("cautious", "nature");
		final List<String> stringInput2 = Arrays.asList("count", "over");
		final List<String> stringInput3 = Arrays.asList("lawyer", "grammar");
		final List<String> expectedStringOutput = new ArrayList<>();
		final List<byte[]> byteInput1 = SinkTestUtil.convertStringListToByteArrayList(stringInput1);
		final List<byte[]> byteInput2 = SinkTestUtil.convertStringListToByteArrayList(stringInput2);
		final List<byte[]> byteInput3 = SinkTestUtil.convertStringListToByteArrayList(stringInput3);
		final List<byte[]> expectedByteOutput = new ArrayList<>();

		expectedStringOutput.addAll(stringInput1);
		expectedStringOutput.addAll(stringInput2);
		expectedStringOutput.addAll(stringInput3);

		expectedByteOutput.addAll(byteInput1);
		expectedByteOutput.addAll(byteInput2);
		expectedByteOutput.addAll(byteInput3);

		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness = createTestHarness(
				committer);
		testHarness.initializeEmptyState();
		testHarness.open();

		testHarness.processElements(byteInput1
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(1L, 1L);
		testHarness.processElements(byteInput2
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(2L, 2L);
		testHarness.processElements(byteInput3
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));
		testHarness.snapshot(3L, 3L);

		testHarness.notifyOfCompletedCheckpoint(1);
		testHarness.notifyOfCompletedCheckpoint(3);

		testHarness.close();

		assertThat(
				testHarness.getOutput(),
				containsStreamElementsInAnyOrder(expectedByteOutput
						.stream()
						.map(StreamRecord::new)
						.toArray()));

		assertThat(
				committer.getCommittedData(),
				containsInAnyOrder(expectedStringOutput.toArray()));
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness() throws Exception {
		return createTestHarness(
				new TestSink.DefaultCommitter(),
				TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(Committer<String> committer) throws Exception {
		return createTestHarness(committer, TestSink.StringCommittableSerializer.INSTANCE);
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
			Committer<String> committer,
			SimpleVersionedSerializer<String> serializer) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(
				new StreamingCommitterOperatorFactory<>(TestSink
						.newBuilder()
						.addWriter()
						.addCommitter(committer)
						.setCommittableSerializer(serializer)
						.addGlobalCommitter()
						.build()),
				BytePrimitiveArraySerializer.INSTANCE);
	}

}
