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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.util.SinkTestUtil.convertStringListToByteArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link BatchGlobalCommitterOperator}.
 */
public class BatchGlobalCommitterOperatorTest extends TestLogger {

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutCommitter() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(null);

		testHarness.initializeEmptyState();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void doNotSupportRetry() throws Exception {
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(new TestSink.AlwaysRetryGlobalCommitter());

		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.processElement(new StreamRecord<>(TestSink.StringCommittableSerializer.INSTANCE.serialize(
				"hotel")));
		testHarness.endInput();
		testHarness.close();
	}

	@Test
	public void endOfInput() throws Exception {
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(globalCommitter);
		final List<String> stringInput = Arrays.asList("compete", "swear", "shallow");
		final List<byte[]> byteInput = convertStringListToByteArrayList(stringInput);

		testHarness.initializeEmptyState();
		testHarness.open();

		testHarness.processElements(byteInput
				.stream()
				.map(StreamRecord::new)
				.collect(Collectors.toList()));

		testHarness.endInput();

		final List<String> expectedCommittedData = Arrays.asList(
				globalCommitter.combine(stringInput),
				"end of input");

		assertThat(testHarness.getOutput().size(), equalTo(0));

		assertThat(
				globalCommitter.getCommittedData(),
				containsInAnyOrder(expectedCommittedData.toArray()));

		testHarness.close();

	}

	@Test
	public void close() throws Exception {
		final TestSink.DefaultGlobalCommitter globalCommitter = new TestSink.DefaultGlobalCommitter(
				"");
		final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
				createTestHarness(globalCommitter);
		testHarness.initializeEmptyState();
		testHarness.open();
		testHarness.close();

		assertThat(globalCommitter.isClosed(), is(true));
	}

	private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
			GlobalCommitter<String, String> globalCommitter) throws Exception {

		return new OneInputStreamOperatorTestHarness<>(
				new BatchGlobalCommitterOperatorFactory<>(TestSink
						.newBuilder()
						.addWriter()
						.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.addGlobalCommitter(globalCommitter)
						.setGlobalCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
						.build()),
				BytePrimitiveArraySerializer.INSTANCE);
	}
}
