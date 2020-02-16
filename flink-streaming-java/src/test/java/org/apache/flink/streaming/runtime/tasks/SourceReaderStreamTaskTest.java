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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SourceReaderOperator;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.util.TestHarnessUtil.assertOutputEquals;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Tests for verifying that the {@link SourceReaderOperator} as a task input can be integrated
 * well with {@link org.apache.flink.streaming.runtime.io.StreamOneInputProcessor}.
 */
public class SourceReaderStreamTaskTest {
	private static final int TIMEOUT = 30_000;

	/**
	 * Tests that the stream operator can snapshot and restore the operator state of chained
	 * operators.
	 */
	@Test
	public void testSnapshotAndRestore() throws Exception {
		final int numRecords = 10;

		TaskStateSnapshot taskStateSnapshot = executeAndWaitForCheckpoint(
			numRecords,
			1,
			IntStream.range(0, numRecords),
			Optional.empty());

		executeAndWaitForCheckpoint(
			numRecords,
			2,
			IntStream.range(numRecords, 2 * numRecords),
			Optional.of(taskStateSnapshot));
	}

	private TaskStateSnapshot executeAndWaitForCheckpoint(
			int numRecords,
			long checkpointId,
			IntStream expectedOutputStream,
			Optional<TaskStateSnapshot> initialSnapshot) throws Exception {

		final LinkedBlockingQueue<Object> expectedOutput = new LinkedBlockingQueue<>();
		expectedOutputStream.forEach(record -> expectedOutput.add(new StreamRecord<>(record)));
		CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
		expectedOutput.add(new CheckpointBarrier(checkpointId, checkpointId, checkpointOptions));

		final StreamTaskTestHarness<Integer> testHarness = createTestHarness(numRecords);
		if (initialSnapshot.isPresent()) {
			testHarness.setTaskStateSnapshot(checkpointId, initialSnapshot.get());
		}

		TestTaskStateManager taskStateManager = testHarness.taskStateManager;
		OneShotLatch waitForAcknowledgeLatch = new OneShotLatch();

		taskStateManager.setWaitForReportLatch(waitForAcknowledgeLatch);

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		StreamTask<Integer, ?> streamTask = testHarness.getTask();
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointId);

		// wait with triggering the checkpoint until we emit all of the data
		while (testHarness.getTask().inputProcessor.getAvailableFuture().isDone()) {
			Thread.sleep(1);
		}

		streamTask.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, false).get();

		waitForAcknowledgeLatch.await();

		assertEquals(checkpointId, taskStateManager.getReportedCheckpointId());

		testHarness.getTask().cancel();
		try {
			testHarness.waitForTaskCompletion(TIMEOUT);
		}
		catch (Exception ex) {
			if (!ExceptionUtils.findThrowable(ex, CancelTaskException.class).isPresent()) {
				throw ex;
			}
		}

		assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
		return taskStateManager.getLastJobManagerTaskStateSnapshot();
	}

	private static StreamTaskTestHarness<Integer> createTestHarness(int numRecords) {
		final StreamTaskTestHarness<Integer> testHarness = new StreamTaskTestHarness<>(
			SourceReaderStreamTask::new,
			BasicTypeInfo.INT_TYPE_INFO);
		final StreamConfig streamConfig = testHarness.getStreamConfig();

		testHarness.setupOutputForSingletonOperatorChain();
		streamConfig.setStreamOperator(new TestingIntegerSourceReaderOperator(numRecords));
		streamConfig.setOperatorID(new OperatorID(42, 44));

		return testHarness;
	}

	/**
	 * A simple {@link SourceReaderOperator} implementation for emitting limited int type records.
	 */
	private static class TestingIntegerSourceReaderOperator extends SourceReaderOperator<Integer> {
		private static final long serialVersionUID = 1L;

		private final int numRecords;

		private int lastRecord;
		private int counter;

		private ListState<Integer> counterState;

		TestingIntegerSourceReaderOperator(int numRecords) {
			this.numRecords = numRecords;
		}

		@Override
		public InputStatus emitNext(DataOutput<Integer> output) throws Exception {
			output.emitRecord(new StreamRecord<>(counter++));

			return hasEmittedEverything() ? InputStatus.NOTHING_AVAILABLE : InputStatus.MORE_AVAILABLE;
		}

		private boolean hasEmittedEverything() {
			return counter >= lastRecord;
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			super.initializeState(context);

			counterState = context.getOperatorStateStore().getListState(
				new ListStateDescriptor<>("counter", IntSerializer.INSTANCE));

			Iterator<Integer> counters = counterState.get().iterator();
			if (counters.hasNext()) {
				counter = counters.next();
			}
			lastRecord = counter + numRecords;
			checkState(!counters.hasNext());
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			super.snapshotState(context);

			counterState.clear();
			counterState.add(counter);
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			if (hasEmittedEverything()) {
				return new CompletableFuture<>();
			}
			return AVAILABLE;
		}

		@Override
		public void close() {
		}
	}
}
