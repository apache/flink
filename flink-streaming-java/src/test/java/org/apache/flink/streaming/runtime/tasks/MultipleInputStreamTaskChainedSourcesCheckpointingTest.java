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

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.MapToStringMultipleInputOperatorFactory;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.addSourceRecords;
import static org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTaskTest.buildTestHarness;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MultipleInputStreamTask} combined with {@link org.apache.flink.streaming.api.operators.SourceOperator} chaining.
 */
public class MultipleInputStreamTaskChainedSourcesCheckpointingTest {
	private static final int MAX_STEPS = 100;

	private final CheckpointMetaData metaData = new CheckpointMetaData(1L, System.currentTimeMillis());
	private final CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
	private final CheckpointBarrier checkpointBarrier = new CheckpointBarrier(metaData.getCheckpointId(), metaData.getTimestamp(), options);

	/**
	 * In this scenario:
	 * 1. checkpoint is triggered via RPC and source is blocked
	 * 2. network inputs are processed until CheckpointBarriers are processed
	 * 3. aligned checkpoint is performed
	 */
	@Test
	public void testSourceCheckpointFirst() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness()) {
			testHarness.setAutoProcess(false);
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			addRecordsAndBarriers(testHarness);

			Future<Boolean> checkpointFuture = testHarness.getStreamTask().triggerCheckpointAsync(metaData, options, false);
			processSingleStepUntil(testHarness, checkpointFuture::isDone);

			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

			ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());

			assertThat(actualOutput.subList(0, expectedOutput.size()), containsInAnyOrder(expectedOutput.toArray()));
			assertThat(actualOutput.get(expectedOutput.size()), equalTo(checkpointBarrier));
		}
	}

	/**
	 * In this scenario:
	 * 1. checkpoint is triggered via RPC and source is blocked
	 * 2. unaligned checkpoint is performed
	 * 3. all data from network inputs are processed
	 */
	@Test
	public void testSourceCheckpointFirstUnaligned() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness(true)) {
			testHarness.setAutoProcess(false);
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			addRecords(testHarness);

			Future<Boolean> checkpointFuture = testHarness.getStreamTask().triggerCheckpointAsync(metaData, options, false);
			processSingleStepUntil(testHarness, checkpointFuture::isDone);

			assertThat(testHarness.getOutput(), contains(checkpointBarrier));

			testHarness.processAll();

			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

			ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());
			assertThat(actualOutput.subList(1, expectedOutput.size() + 1), containsInAnyOrder(expectedOutput.toArray()));
		}
	}

	/**
	 * In this scenario:
	 * 1a. network inputs are processed until CheckpointBarriers are processed
	 * 1b. source records are processed at the same time
	 * 2. checkpoint is triggered via RPC
	 * 3. aligned checkpoint is performed
	 */
	@Test
	public void testSourceCheckpointLast() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness()) {
			testHarness.setAutoProcess(false);
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();
			addRecordsAndBarriers(testHarness);

			testHarness.processAll();

			Future<Boolean> checkpointFuture = testHarness.getStreamTask().triggerCheckpointAsync(metaData, options, false);
			processSingleStepUntil(testHarness, checkpointFuture::isDone);

			expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));

			ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());

			assertThat(actualOutput.subList(0, expectedOutput.size()), containsInAnyOrder(expectedOutput.toArray()));
			assertThat(actualOutput.get(expectedOutput.size()), equalTo(checkpointBarrier));
		}
	}

	/**
	 * In this scenario:
	 * 1. network inputs are processed until CheckpointBarriers are processed
	 * 2. there are no source records to be processed
	 * 3. checkpoint is triggered on first received CheckpointBarrier
	 * 4. unaligned checkpoint is performed at some point of time blocking the source
	 * 5. more source records are added, that shouldn't be processed
	 */
	@Test
	public void testSourceCheckpointLastUnaligned() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness = buildTestHarness(true)) {
			testHarness.setAutoProcess(false);
			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			addNetworkRecords(testHarness);
			addBarriers(testHarness);

			testHarness.processAll();
			addSourceRecords(testHarness, 1, 1337, 1337, 1337);
			testHarness.processAll();

			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(new StreamRecord<>("47.0", TimestampAssigner.NO_TIMESTAMP));
			expectedOutput.add(checkpointBarrier);

			assertThat(testHarness.getOutput(), containsInAnyOrder(expectedOutput.toArray()));
		}
	}

	@Test
	public void testOnlyOneSource() throws Exception {
		try (StreamTaskMailboxTestHarness<String> testHarness =
				new StreamTaskMailboxTestHarnessBuilder<>(MultipleInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
					.modifyExecutionConfig(config -> config.enableObjectReuse())
					.addSourceInput(
						new SourceOperatorFactory<>(
							new MockSource(Boundedness.BOUNDED, 1),
							WatermarkStrategy.noWatermarks()))
					.setupOutputForSingletonOperatorChain(new MapToStringMultipleInputOperatorFactory(1))
					.build()) {
			testHarness.setAutoProcess(false);

			ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

			addSourceRecords(testHarness, 0, 42, 43, 44);
			processSingleStepUntil(testHarness, () -> !testHarness.getOutput().isEmpty());
			expectedOutput.add(new StreamRecord<>("42", TimestampAssigner.NO_TIMESTAMP));

			Future<Boolean> checkpointFuture = testHarness.getStreamTask().triggerCheckpointAsync(metaData, options, false);
			processSingleStepUntil(testHarness, checkpointFuture::isDone);

			ArrayList<Object> actualOutput = new ArrayList<>(testHarness.getOutput());
			assertThat(actualOutput.subList(0, expectedOutput.size()), containsInAnyOrder(expectedOutput.toArray()));
			assertThat(actualOutput.get(expectedOutput.size()), equalTo(checkpointBarrier));
		}
	}

	private void addRecordsAndBarriers(StreamTaskMailboxTestHarness<String> testHarness) throws Exception {
		addRecords(testHarness);
		addBarriers(testHarness);
	}

	private void addBarriers(StreamTaskMailboxTestHarness<String> testHarness) throws Exception {
		testHarness.processEvent(checkpointBarrier, 0);
		testHarness.processEvent(checkpointBarrier, 1);
	}

	private void addRecords(StreamTaskMailboxTestHarness<String> testHarness) throws Exception {
		addSourceRecords(testHarness, 1, 42, 42, 42);
		addNetworkRecords(testHarness);
	}

	private void addNetworkRecords(StreamTaskMailboxTestHarness<String> testHarness) throws Exception {
		testHarness.processElement(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP), 0);
		testHarness.processElement(new StreamRecord<>("44", TimestampAssigner.NO_TIMESTAMP), 0);
		testHarness.processElement(new StreamRecord<>(47d, TimestampAssigner.NO_TIMESTAMP), 1);
		testHarness.processElement(new StreamRecord<>(47d, TimestampAssigner.NO_TIMESTAMP), 1);
	}

	private void processSingleStepUntil(StreamTaskMailboxTestHarness<String> testHarness, Supplier<Boolean> condition) throws Exception {
		assertFalse(condition.get());
		for (int i = 0; i < MAX_STEPS && !condition.get(); i++) {
			testHarness.processSingleStep();
		}
		assertTrue(condition.get());
	}
}

