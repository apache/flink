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
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.util.TestHarnessUtil.assertOutputEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for verifying that the {@link SourceOperator} as a task input can be integrated
 * well with {@link org.apache.flink.streaming.runtime.io.StreamOneInputProcessor}.
 */
public class SourceOperatorStreamTaskTest {
	private static final OperatorID OPERATOR_ID = new OperatorID();
	private static final int NUM_RECORDS = 10;

	/**
	 * Tests that the stream operator can snapshot and restore the operator state of chained
	 * operators.
	 */
	@Test
	public void testSnapshotAndRestore() throws Exception {
		// process NUM_RECORDS records and take a snapshot.
		TaskStateSnapshot taskStateSnapshot = executeAndWaitForCheckpoint(
				1,
				null,
				IntStream.range(0, NUM_RECORDS));

		// Resume from the snapshot and continue to process another NUM_RECORDS records.
		executeAndWaitForCheckpoint(
				2,
				taskStateSnapshot,
				IntStream.range(NUM_RECORDS, NUM_RECORDS * 2));
	}

	private TaskStateSnapshot executeAndWaitForCheckpoint(
			long checkpointId,
			TaskStateSnapshot initialSnapshot,
			IntStream expectedRecords) throws Exception {

		try (StreamTaskMailboxTestHarness<Integer> testHarness = createTestHarness(checkpointId, initialSnapshot)) {
			// Add records to the splits.
			MockSourceSplit split = getAndMaybeAssignSplit(testHarness);
			// Add records to the split and update expected output.
			addRecords(split, NUM_RECORDS);
			// Process all the records.
			processUntil(testHarness, () -> !testHarness.getStreamTask().inputProcessor.getAvailableFuture().isDone());

			// Trigger a checkpoint.
			CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
			OneShotLatch waitForAcknowledgeLatch = new OneShotLatch();
			testHarness.taskStateManager.setWaitForReportLatch(waitForAcknowledgeLatch);
			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointId);
			Future<Boolean> checkpointFuture =
					testHarness
							.getStreamTask()
							.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, false);

			// Wait until the checkpoint finishes.
			// We have to mark the source reader as available here, otherwise the runMailboxStep() call after
			// checkpiont is completed will block.
			getSourceReaderFromTask(testHarness).markAvailable();
			processUntil(testHarness, checkpointFuture::isDone);
			waitForAcknowledgeLatch.await();

			// Build expected output to verify the results
			Queue<Object> expectedOutput = new LinkedList<>();
			expectedRecords.forEach(r -> expectedOutput.offer(new StreamRecord<>(r, TimestampAssigner.NO_TIMESTAMP)));
			// Add barrier to the expected output.
			expectedOutput.add(new CheckpointBarrier(checkpointId, checkpointId, checkpointOptions));

			assertEquals(checkpointId, testHarness.taskStateManager.getReportedCheckpointId());
			assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

			return testHarness.taskStateManager.getLastJobManagerTaskStateSnapshot();
		}
	}

	private void processUntil(StreamTaskMailboxTestHarness testHarness, Supplier<Boolean> condition) throws Exception {
		do {
			testHarness.getStreamTask().runMailboxStep();
		} while (!condition.get());
	}

	private StreamTaskMailboxTestHarness<Integer> createTestHarness(
			long checkpointId,
			TaskStateSnapshot snapshot) throws Exception {
		// get a source operator.
		SourceOperatorFactory<Integer> sourceOperatorFactory = new SourceOperatorFactory<>(
				new MockSource(Boundedness.BOUNDED, 1),
				WatermarkStrategy.noWatermarks());

		// build a test harness.
		MultipleInputStreamTaskTestHarnessBuilder<Integer> builder =
				new MultipleInputStreamTaskTestHarnessBuilder<>(SourceOperatorStreamTask::new, BasicTypeInfo.INT_TYPE_INFO);
		if (snapshot != null) {
			// Set initial snapshot if needed.
			builder.setTaskStateSnapshot(checkpointId, snapshot);
		}
		return builder
				.setupOutputForSingletonOperatorChain(sourceOperatorFactory, OPERATOR_ID)
				.build();
	}

	private MockSourceSplit getAndMaybeAssignSplit(StreamTaskMailboxTestHarness<Integer> testHarness) throws Exception {
		List<MockSourceSplit> assignedSplits = getSourceReaderFromTask(testHarness).getAssignedSplits();
		if (assignedSplits.isEmpty()) {
			// Prepare the source split and assign it to the source reader.
			MockSourceSplit split = new MockSourceSplit(0, 0);
			// Assign the split to the source reader.
			AddSplitEvent<MockSourceSplit> addSplitEvent =
					new AddSplitEvent<>(Collections.singletonList(split), new MockSourceSplitSerializer());
			testHarness
					.getStreamTask()
					.dispatchOperatorEvent(OPERATOR_ID, new SerializedValue<>(addSplitEvent));
			// Run the task until the split assignment is done.
			while (assignedSplits.isEmpty()) {
				testHarness.getStreamTask().runMailboxStep();
			}
			// Need to mark the source reader as available for further processing.
			getSourceReaderFromTask(testHarness).markAvailable();
		}
		// The source reader already has an assigned split, just return it
		return assignedSplits.get(0);
	}

	private void addRecords(MockSourceSplit split, int numRecords) {
		int startingIndex = split.index();
		for (int i = startingIndex; i < startingIndex + numRecords; i++) {
			split.addRecord(i);
		}
	}

	private MockSourceReader getSourceReaderFromTask(StreamTaskMailboxTestHarness<Integer> testHarness) {
		return (MockSourceReader) ((SourceOperator) testHarness.getStreamTask().headOperator).getSourceReader();
	}
}
