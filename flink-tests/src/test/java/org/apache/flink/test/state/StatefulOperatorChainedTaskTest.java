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

package org.apache.flink.test.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CheckpointingOptions.STATE_BACKEND;
import static org.junit.Assert.assertEquals;

/**
 * Test for StatefulOperatorChainedTaskTest.
 */
public class StatefulOperatorChainedTaskTest {

	private static final Set<OperatorID> RESTORED_OPERATORS = ConcurrentHashMap.newKeySet();
	private TemporaryFolder temporaryFolder;

	@Before
	public void setup() throws IOException {
		RESTORED_OPERATORS.clear();
		temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
	}

	@Test
	public void testMultipleStatefulOperatorChainedSnapshotAndRestore() throws Exception {

		OperatorID headOperatorID = new OperatorID(42L, 42L);
		OperatorID tailOperatorID = new OperatorID(44L, 44L);

		JobManagerTaskRestore restore = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator("head"),
			tailOperatorID,
			new CounterOperator("tail"),
			Optional.empty());

		TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();

		assertEquals(2, stateHandles.getSubtaskStateMappings().size());

		createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator("head"),
			tailOperatorID,
			new CounterOperator("tail"),
			Optional.of(restore));

		assertEquals(new HashSet<>(Arrays.asList(headOperatorID, tailOperatorID)), RESTORED_OPERATORS);
	}

	private JobManagerTaskRestore createRunAndCheckpointOperatorChain(
		OperatorID headId,
		OneInputStreamOperator<String, String> headOperator,
		OperatorID tailId,
		OneInputStreamOperator<String, String> tailOperator,
		Optional<JobManagerTaskRestore> restore) throws Exception {

		File localRootDir = temporaryFolder.newFolder();
		final OneInputStreamTaskTestHarness<String, String> testHarness =
			new OneInputStreamTaskTestHarness<>(
				OneInputStreamTask::new,
				1, 1,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				localRootDir);

		testHarness.setupOperatorChain(headId, headOperator)
			.chain(tailId, tailOperator, StringSerializer.INSTANCE, true)
			.finish();

		if (restore.isPresent()) {
			JobManagerTaskRestore taskRestore = restore.get();
			testHarness.setTaskStateSnapshot(
				taskRestore.getRestoreCheckpointId(),
				taskRestore.getTaskStateSnapshot());
		}

		StreamMockEnvironment environment = new StreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.getExecutionConfig(),
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize,
			testHarness.getTaskStateManager());

		Configuration configuration = new Configuration();
		configuration.setString(STATE_BACKEND.key(), "rocksdb");
		File file = temporaryFolder.newFolder();
		configuration.setString(CHECKPOINTS_DIRECTORY.key(), file.toURI().toString());
		configuration.setString(INCREMENTAL_CHECKPOINTS.key(), "true");
		environment.setTaskManagerInfo(
			new TestingTaskManagerRuntimeInfo(
				configuration,
				System.getProperty("java.io.tmpdir").split(",|" + File.pathSeparator)));
		testHarness.invoke(environment);
		testHarness.waitForTaskRunning();

		OneInputStreamTask<String, String> streamTask = testHarness.getTask();

		processRecords(testHarness);
		triggerCheckpoint(testHarness, streamTask);

		TestTaskStateManager taskStateManager = testHarness.getTaskStateManager();

		JobManagerTaskRestore jobManagerTaskRestore = new JobManagerTaskRestore(
			taskStateManager.getReportedCheckpointId(),
			taskStateManager.getLastJobManagerTaskStateSnapshot());

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
		return jobManagerTaskRestore;
	}

	private void triggerCheckpoint(
		OneInputStreamTaskTestHarness<String, String> testHarness,
		OneInputStreamTask<String, String> streamTask) throws Exception {

		long checkpointId = 1L;
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 1L);

		testHarness.getTaskStateManager().setWaitForReportLatch(new OneShotLatch());

		while (!streamTask.triggerCheckpointAsync(checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation(), false).get()) {}

		testHarness.getTaskStateManager().getWaitForReportLatch().await();
		long reportedCheckpointId = testHarness.getTaskStateManager().getReportedCheckpointId();

		assertEquals(checkpointId, reportedCheckpointId);
	}

	private void processRecords(OneInputStreamTaskTestHarness<String, String> testHarness) throws Exception {
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>("10"), 0, 0);
		testHarness.processElement(new StreamRecord<>("20"), 0, 0);
		testHarness.processElement(new StreamRecord<>("30"), 0, 0);

		testHarness.waitForInputProcessing();

		expectedOutput.add(new StreamRecord<>("10"));
		expectedOutput.add(new StreamRecord<>("20"));
		expectedOutput.add(new StreamRecord<>("30"));
		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
	}

	private abstract static class RestoreWatchOperator<IN, OUT>
		extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT> {

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			if (context.isRestored()) {
				RESTORED_OPERATORS.add(getOperatorID());
			}
		}
	}

	/**
	 * Operator that counts processed messages and keeps result on state.
	 */
	private static class CounterOperator extends RestoreWatchOperator<String, String> {
		private static final long serialVersionUID = 2048954179291813243L;

		private static long snapshotOutData = 0L;
		private ValueState<Long> counterState;
		private long counter = 0;
		private String prefix;

		CounterOperator(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			counter++;
			output.collect(element);
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			super.initializeState(context);

			counterState = context
				.getKeyedStateStore()
				.getState(new ValueStateDescriptor<>(prefix + "counter-state", LongSerializer.INSTANCE));

			// set key manually to make RocksDBListState get the serialized key.
			setCurrentKey("10");

			if (context.isRestored()) {
				counter =  counterState.value();
				assertEquals(snapshotOutData, counter);
				counterState.clear();
			}
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			counterState.update(counter);
			snapshotOutData = counter;
		}
	}
}

