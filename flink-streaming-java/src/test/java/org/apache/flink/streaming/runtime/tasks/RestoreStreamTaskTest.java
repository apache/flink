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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests ensuring correct behaviour of {@link org.apache.flink.runtime.state.ManagedInitializationContext#isRestored}
 * method.
 */
public class RestoreStreamTaskTest extends TestLogger {

	private static final Set<OperatorID> RESTORED_OPERATORS = ConcurrentHashMap.newKeySet();

	@Before
	public void setup() {
		RESTORED_OPERATORS.clear();
	}

	@Test
	public void testRestore() throws Exception {
		OperatorID headOperatorID = new OperatorID(42L, 42L);
		OperatorID tailOperatorID = new OperatorID(44L, 44L);
		AcknowledgeStreamMockEnvironment environment1 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.empty());

		assertEquals(2, environment1.getCheckpointStateHandles().getSubtaskStateMappings().size());

		TaskStateSnapshot stateHandles = environment1.getCheckpointStateHandles();

		AcknowledgeStreamMockEnvironment environment2 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.of(stateHandles));

		assertEquals(
			new HashSet<OperatorID>() {{
				add(headOperatorID);
				add(tailOperatorID);
			}},
			RESTORED_OPERATORS);
	}

	@Test
	public void testRestoreHeadWithNewId() throws Exception {
		OperatorID tailOperatorID = new OperatorID(44L, 44L);
		AcknowledgeStreamMockEnvironment environment1 = createRunAndCheckpointOperatorChain(
			new OperatorID(42L, 42L),
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.empty());

		assertEquals(2, environment1.getCheckpointStateHandles().getSubtaskStateMappings().size());

		TaskStateSnapshot stateHandles = environment1.getCheckpointStateHandles();

		AcknowledgeStreamMockEnvironment environment2 = createRunAndCheckpointOperatorChain(
			new OperatorID(4242L, 4242L),
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.of(stateHandles));

		assertEquals(
			new HashSet<OperatorID>() {{
				add(tailOperatorID);
			}},
			RESTORED_OPERATORS);
	}

	@Test
	public void testRestoreTailWithNewId() throws Exception {
		OperatorID headOperatorID = new OperatorID(42L, 42L);

		AcknowledgeStreamMockEnvironment environment1 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			new OperatorID(44L, 44L),
			new CounterOperator(),
			Optional.empty());

		assertEquals(2, environment1.getCheckpointStateHandles().getSubtaskStateMappings().size());

		TaskStateSnapshot stateHandles = environment1.getCheckpointStateHandles();

		AcknowledgeStreamMockEnvironment environment2 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			new OperatorID(4444L, 4444L),
			new CounterOperator(),
			Optional.of(stateHandles));

		assertEquals(
			new HashSet<OperatorID>() {{
				add(headOperatorID);
			}},
			RESTORED_OPERATORS);
	}

	@Test
	public void testRestoreAfterScaleUp() throws Exception {
		OperatorID headOperatorID = new OperatorID(42L, 42L);
		OperatorID tailOperatorID = new OperatorID(44L, 44L);

		AcknowledgeStreamMockEnvironment environment1 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.empty());

		assertEquals(2, environment1.getCheckpointStateHandles().getSubtaskStateMappings().size());

		// test empty state in case of scale up
		OperatorSubtaskState emptyHeadOperatorState = StateAssignmentOperation.operatorSubtaskStateFrom(
			new OperatorInstanceID(0, headOperatorID),
			Collections.emptyMap(),
			Collections.emptyMap(),
			Collections.emptyMap(),
			Collections.emptyMap());

		TaskStateSnapshot stateHandles = environment1.getCheckpointStateHandles();
		stateHandles.putSubtaskStateByOperatorID(headOperatorID, emptyHeadOperatorState);

		AcknowledgeStreamMockEnvironment environment2 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new CounterOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.of(stateHandles));

		assertEquals(
			new HashSet<OperatorID>() {{
				add(headOperatorID);
				add(tailOperatorID);
			}},
			RESTORED_OPERATORS);
	}

	@Test
	public void testRestoreWithoutState() throws Exception {
		OperatorID headOperatorID = new OperatorID(42L, 42L);
		OperatorID tailOperatorID = new OperatorID(44L, 44L);

		AcknowledgeStreamMockEnvironment environment1 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new StatelessOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.empty());

		assertEquals(2, environment1.getCheckpointStateHandles().getSubtaskStateMappings().size());

		TaskStateSnapshot stateHandles = environment1.getCheckpointStateHandles();

		AcknowledgeStreamMockEnvironment environment2 = createRunAndCheckpointOperatorChain(
			headOperatorID,
			new StatelessOperator(),
			tailOperatorID,
			new CounterOperator(),
			Optional.of(stateHandles));

		assertEquals(
			new HashSet<OperatorID>() {{
				add(headOperatorID);
				add(tailOperatorID);
			}},
			RESTORED_OPERATORS);
	}

	private AcknowledgeStreamMockEnvironment createRunAndCheckpointOperatorChain(
			OperatorID headId,
			OneInputStreamOperator<String, String> headOperator,
			OperatorID tailId,
			OneInputStreamOperator<String, String> tailOperator,
			Optional<TaskStateSnapshot> stateHandles) throws Exception {

		final OneInputStreamTask<String, String> streamTask = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<String, String> testHarness =
			new OneInputStreamTaskTestHarness<String, String>(
				streamTask, 1, 1,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOperatorChain(headId, headOperator)
			.chain(tailId, tailOperator, StringSerializer.INSTANCE)
			.finish();

		AcknowledgeStreamMockEnvironment environment = new AcknowledgeStreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.executionConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize);

		if (stateHandles.isPresent()) {
			streamTask.setInitialState(stateHandles.get());
		}
		testHarness.invoke(environment);
		testHarness.waitForTaskRunning();

		processRecords(testHarness);
		triggerCheckpoint(testHarness, environment, streamTask);

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		return environment;
	}

	private void triggerCheckpoint(
			OneInputStreamTaskTestHarness<String, String> testHarness,
			AcknowledgeStreamMockEnvironment environment,
			OneInputStreamTask<String, String> streamTask) throws Exception {
		long checkpointId = 1L;
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 1L);

		while (!streamTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forCheckpoint())) {}

		environment.getCheckpointLatch().await();
		assertEquals(checkpointId, environment.getCheckpointId());
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

		private ListState<Long> counterState;
		private long counter = 0;

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			counter++;
			output.collect(element);
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {
			super.initializeState(context);

			counterState = context
				.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("counter-state", LongSerializer.INSTANCE));

			if (context.isRestored()) {
				for (Long value : counterState.get()) {
					counter += value;
				}
				counterState.clear();
			}
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
			counterState.add(counter);
		}
	}

	/**
	 * Operator that does nothing except counting state restorations.
	 */
	private static class StatelessOperator extends RestoreWatchOperator<String, String> {

		private static final long serialVersionUID = 2048954179291813244L;

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			output.collect(element);
		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {
		}
	}
}
