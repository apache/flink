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
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests ensuring correct behaviour of {@link
 * org.apache.flink.runtime.state.ManagedInitializationContext#isRestored} method.
 */
class RestoreStreamTaskTest {

    private static final Map<OperatorID, Long> RESTORED_OPERATORS = new ConcurrentHashMap();

    @BeforeEach
    void setup() {
        RESTORED_OPERATORS.clear();
    }

    @Test
    void testRestore() throws Exception {

        OperatorID headOperatorID = new OperatorID(42L, 42L);
        OperatorID tailOperatorID = new OperatorID(44L, 44L);

        JobManagerTaskRestore restore =
                createRunAndCheckpointOperatorChain(
                        headOperatorID,
                        new CounterOperator(),
                        tailOperatorID,
                        new CounterOperator(),
                        Optional.empty());

        TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();

        assertThat(stateHandles.getSubtaskStateMappings()).hasSize(2);

        createRunAndCheckpointOperatorChain(
                headOperatorID,
                new CounterOperator(),
                tailOperatorID,
                new CounterOperator(),
                Optional.of(restore));

        assertThat(RESTORED_OPERATORS)
                .containsOnlyKeys(headOperatorID, tailOperatorID)
                .containsValue(restore.getRestoreCheckpointId());
    }

    @Test
    void testRestoreHeadWithNewId() throws Exception {

        OperatorID tailOperatorID = new OperatorID(44L, 44L);

        JobManagerTaskRestore restore =
                createRunAndCheckpointOperatorChain(
                        new OperatorID(42L, 42L),
                        new CounterOperator(),
                        tailOperatorID,
                        new CounterOperator(),
                        Optional.empty());

        TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();

        assertThat(stateHandles.getSubtaskStateMappings()).hasSize(2);

        createRunAndCheckpointOperatorChain(
                new OperatorID(4242L, 4242L),
                new CounterOperator(),
                tailOperatorID,
                new CounterOperator(),
                Optional.of(restore));

        assertThat(RESTORED_OPERATORS)
                .containsOnlyKeys(tailOperatorID)
                .containsValue(restore.getRestoreCheckpointId());
    }

    @Test
    void testRestoreTailWithNewId() throws Exception {
        OperatorID headOperatorID = new OperatorID(42L, 42L);

        JobManagerTaskRestore restore =
                createRunAndCheckpointOperatorChain(
                        headOperatorID,
                        new CounterOperator(),
                        new OperatorID(44L, 44L),
                        new CounterOperator(),
                        Optional.empty());

        TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();
        assertThat(stateHandles.getSubtaskStateMappings()).hasSize(2);

        createRunAndCheckpointOperatorChain(
                headOperatorID,
                new CounterOperator(),
                new OperatorID(4444L, 4444L),
                new CounterOperator(),
                Optional.of(restore));

        assertThat(RESTORED_OPERATORS)
                .containsOnlyKeys(headOperatorID)
                .containsValue(restore.getRestoreCheckpointId());
    }

    @Test
    void testRestoreAfterScaleUp() throws Exception {
        OperatorID headOperatorID = new OperatorID(42L, 42L);
        OperatorID tailOperatorID = new OperatorID(44L, 44L);

        JobManagerTaskRestore restore =
                createRunAndCheckpointOperatorChain(
                        headOperatorID,
                        new CounterOperator(),
                        tailOperatorID,
                        new CounterOperator(),
                        Optional.empty());

        TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();

        assertThat(stateHandles.getSubtaskStateMappings()).hasSize(2);

        // test empty state in case of scale up

        OperatorSubtaskState emptyHeadOperatorState = OperatorSubtaskState.builder().build();

        stateHandles.putSubtaskStateByOperatorID(headOperatorID, emptyHeadOperatorState);

        createRunAndCheckpointOperatorChain(
                headOperatorID,
                new CounterOperator(),
                tailOperatorID,
                new CounterOperator(),
                Optional.of(restore));

        assertThat(RESTORED_OPERATORS)
                .containsOnlyKeys(headOperatorID, tailOperatorID)
                .containsValue(restore.getRestoreCheckpointId());
    }

    @Test
    void testRestoreWithoutState() throws Exception {
        OperatorID headOperatorID = new OperatorID(42L, 42L);
        OperatorID tailOperatorID = new OperatorID(44L, 44L);

        JobManagerTaskRestore restore =
                createRunAndCheckpointOperatorChain(
                        headOperatorID,
                        new StatelessOperator(),
                        tailOperatorID,
                        new CounterOperator(),
                        Optional.empty());

        TaskStateSnapshot stateHandles = restore.getTaskStateSnapshot();
        assertThat(stateHandles.getSubtaskStateMappings()).hasSize(2);

        createRunAndCheckpointOperatorChain(
                headOperatorID,
                new StatelessOperator(),
                tailOperatorID,
                new CounterOperator(),
                Optional.of(restore));

        assertThat(RESTORED_OPERATORS)
                .containsOnlyKeys(headOperatorID, tailOperatorID)
                .containsValue(restore.getRestoreCheckpointId());
    }

    private JobManagerTaskRestore createRunAndCheckpointOperatorChain(
            OperatorID headId,
            OneInputStreamOperator<String, String> headOperator,
            OperatorID tailId,
            OneInputStreamOperator<String, String> tailOperator,
            Optional<JobManagerTaskRestore> restore)
            throws Exception {

        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        1,
                        1,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness
                .setupOperatorChain(headId, headOperator)
                .chain(tailId, tailOperator, StringSerializer.INSTANCE)
                .finish();

        if (restore.isPresent()) {
            JobManagerTaskRestore taskRestore = restore.get();
            testHarness.setTaskStateSnapshot(
                    taskRestore.getRestoreCheckpointId(), taskRestore.getTaskStateSnapshot());
        }

        StreamMockEnvironment environment =
                new StreamMockEnvironment(
                        testHarness.jobConfig,
                        testHarness.taskConfig,
                        testHarness.executionConfig,
                        testHarness.memorySize,
                        new MockInputSplitProvider(),
                        testHarness.bufferSize,
                        testHarness.taskStateManager);

        testHarness.invoke(environment);
        testHarness.waitForTaskRunning();

        OneInputStreamTask<String, String> streamTask = testHarness.getTask();

        processRecords(testHarness);
        triggerCheckpoint(testHarness, streamTask);

        TestTaskStateManager taskStateManager = testHarness.taskStateManager;

        JobManagerTaskRestore jobManagerTaskRestore =
                new JobManagerTaskRestore(
                        taskStateManager.getReportedCheckpointId(),
                        taskStateManager.getLastJobManagerTaskStateSnapshot());

        testHarness.endInput();
        testHarness.waitForTaskCompletion();
        return jobManagerTaskRestore;
    }

    private void triggerCheckpoint(
            OneInputStreamTaskTestHarness<String, String> testHarness,
            OneInputStreamTask<String, String> streamTask)
            throws Exception {

        long checkpointId = 1L;
        CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 1L);

        streamTask.triggerCheckpointAsync(
                checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation());

        testHarness.taskStateManager.getWaitForReportLatch().await();
        long reportedCheckpointId = testHarness.taskStateManager.getReportedCheckpointId();

        assertThat(reportedCheckpointId).isEqualTo(checkpointId);
    }

    private void processRecords(OneInputStreamTaskTestHarness<String, String> testHarness)
            throws Exception {
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>("10"), 0, 0);
        testHarness.processElement(new StreamRecord<>("20"), 0, 0);
        testHarness.processElement(new StreamRecord<>("30"), 0, 0);

        testHarness.waitForInputProcessing();

        expectedOutput.add(new StreamRecord<>("10"));
        expectedOutput.add(new StreamRecord<>("20"));
        expectedOutput.add(new StreamRecord<>("30"));
        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    private abstract static class RestoreWatchOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT> {

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            assertThat(context.getRestoredCheckpointId().isPresent())
                    .as("Restored context id should be set iff is restored")
                    .isEqualTo(context.isRestored());
            if (context.isRestored()) {
                RESTORED_OPERATORS.put(
                        getOperatorID(), context.getRestoredCheckpointId().getAsLong());
            }
        }
    }

    /** Operator that counts processed messages and keeps result on state. */
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

            counterState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "counter-state", LongSerializer.INSTANCE));

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

    /** Operator that does nothing except counting state restorations. */
    private static class StatelessOperator extends RestoreWatchOperator<String, String> {

        private static final long serialVersionUID = 2048954179291813244L;

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {}
    }
}
