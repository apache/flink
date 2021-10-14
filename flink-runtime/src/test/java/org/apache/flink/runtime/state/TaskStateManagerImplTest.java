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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateHandleDummyUtil;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executor;

public class TaskStateManagerImplTest extends TestLogger {

    /** Test reporting and retrieving prioritized local and remote state. */
    @Test
    public void testStateReportingAndRetrieving() {

        JobID jobID = new JobID();
        ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

        TestCheckpointResponder testCheckpointResponder = new TestCheckpointResponder();
        TestTaskLocalStateStore testTaskLocalStateStore = new TestTaskLocalStateStore();
        InMemoryStateChangelogStorage changelogStorage = new InMemoryStateChangelogStorage();

        TaskStateManager taskStateManager =
                taskStateManager(
                        jobID,
                        executionAttemptID,
                        testCheckpointResponder,
                        null,
                        testTaskLocalStateStore,
                        changelogStorage);

        // ---------------------------------------- test reporting
        // -----------------------------------------

        CheckpointMetaData checkpointMetaData = new CheckpointMetaData(74L, 11L);
        CheckpointMetrics checkpointMetrics = new CheckpointMetrics();
        TaskStateSnapshot jmTaskStateSnapshot = new TaskStateSnapshot();

        OperatorID operatorID_1 = new OperatorID(1L, 1L);
        OperatorID operatorID_2 = new OperatorID(2L, 2L);
        OperatorID operatorID_3 = new OperatorID(3L, 3L);

        Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_1).isRestored());
        Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_2).isRestored());
        Assert.assertFalse(taskStateManager.prioritizedOperatorState(operatorID_3).isRestored());

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        // Remote state of operator 1 has only managed keyed state.
        OperatorSubtaskState jmOperatorSubtaskState_1 =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateHandleDummyUtil.createNewKeyedStateHandle(keyGroupRange))
                        .build();
        // Remote state of operator 1 has only raw keyed state.
        OperatorSubtaskState jmOperatorSubtaskState_2 =
                OperatorSubtaskState.builder()
                        .setRawKeyedState(
                                StateHandleDummyUtil.createNewKeyedStateHandle(keyGroupRange))
                        .build();

        jmTaskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, jmOperatorSubtaskState_1);
        jmTaskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, jmOperatorSubtaskState_2);

        TaskStateSnapshot tmTaskStateSnapshot = new TaskStateSnapshot();

        // Only operator 1 has a local alternative for the managed keyed state.
        OperatorSubtaskState tmOperatorSubtaskState_1 =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                StateHandleDummyUtil.createNewKeyedStateHandle(keyGroupRange))
                        .build();

        tmTaskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, tmOperatorSubtaskState_1);

        taskStateManager.reportTaskStateSnapshots(
                checkpointMetaData, checkpointMetrics, jmTaskStateSnapshot, tmTaskStateSnapshot);

        TestCheckpointResponder.AcknowledgeReport acknowledgeReport =
                testCheckpointResponder.getAcknowledgeReports().get(0);

        // checks that the checkpoint responder and the local state store received state as
        // expected.
        Assert.assertEquals(
                checkpointMetaData.getCheckpointId(), acknowledgeReport.getCheckpointId());
        Assert.assertEquals(checkpointMetrics, acknowledgeReport.getCheckpointMetrics());
        Assert.assertEquals(executionAttemptID, acknowledgeReport.getExecutionAttemptID());
        Assert.assertEquals(jobID, acknowledgeReport.getJobID());
        Assert.assertEquals(jmTaskStateSnapshot, acknowledgeReport.getSubtaskState());
        Assert.assertEquals(
                tmTaskStateSnapshot,
                testTaskLocalStateStore.retrieveLocalState(checkpointMetaData.getCheckpointId()));

        // -------------------------------------- test prio retrieving
        // ---------------------------------------

        JobManagerTaskRestore taskRestore =
                new JobManagerTaskRestore(
                        checkpointMetaData.getCheckpointId(), acknowledgeReport.getSubtaskState());

        taskStateManager =
                taskStateManager(
                        jobID,
                        executionAttemptID,
                        testCheckpointResponder,
                        taskRestore,
                        testTaskLocalStateStore,
                        changelogStorage);

        // this has remote AND local managed keyed state.
        PrioritizedOperatorSubtaskState prioritized_1 =
                taskStateManager.prioritizedOperatorState(operatorID_1);
        // this has only remote raw keyed state.
        PrioritizedOperatorSubtaskState prioritized_2 =
                taskStateManager.prioritizedOperatorState(operatorID_2);
        // not restored.
        PrioritizedOperatorSubtaskState prioritized_3 =
                taskStateManager.prioritizedOperatorState(operatorID_3);

        Assert.assertTrue(prioritized_1.isRestored());
        Assert.assertTrue(prioritized_2.isRestored());
        Assert.assertTrue(prioritized_3.isRestored());
        Assert.assertTrue(taskStateManager.prioritizedOperatorState(new OperatorID()).isRestored());

        // checks for operator 1.
        Iterator<StateObjectCollection<KeyedStateHandle>> prioritizedManagedKeyedState_1 =
                prioritized_1.getPrioritizedManagedKeyedState().iterator();

        Assert.assertTrue(prioritizedManagedKeyedState_1.hasNext());
        StateObjectCollection<KeyedStateHandle> current = prioritizedManagedKeyedState_1.next();
        KeyedStateHandle keyedStateHandleExp =
                tmOperatorSubtaskState_1.getManagedKeyedState().iterator().next();
        KeyedStateHandle keyedStateHandleAct = current.iterator().next();
        Assert.assertTrue(keyedStateHandleExp == keyedStateHandleAct);
        Assert.assertTrue(prioritizedManagedKeyedState_1.hasNext());
        current = prioritizedManagedKeyedState_1.next();
        keyedStateHandleExp = jmOperatorSubtaskState_1.getManagedKeyedState().iterator().next();
        keyedStateHandleAct = current.iterator().next();
        Assert.assertTrue(keyedStateHandleExp == keyedStateHandleAct);
        Assert.assertFalse(prioritizedManagedKeyedState_1.hasNext());

        // checks for operator 2.
        Iterator<StateObjectCollection<KeyedStateHandle>> prioritizedRawKeyedState_2 =
                prioritized_2.getPrioritizedRawKeyedState().iterator();

        Assert.assertTrue(prioritizedRawKeyedState_2.hasNext());
        current = prioritizedRawKeyedState_2.next();
        keyedStateHandleExp = jmOperatorSubtaskState_2.getRawKeyedState().iterator().next();
        keyedStateHandleAct = current.iterator().next();
        Assert.assertTrue(keyedStateHandleExp == keyedStateHandleAct);
        Assert.assertFalse(prioritizedRawKeyedState_2.hasNext());
    }

    /**
     * This tests if the {@link TaskStateManager} properly returns the the subtask local state dir
     * from the corresponding {@link TaskLocalStateStoreImpl}.
     */
    @Test
    public void testForwardingSubtaskLocalStateBaseDirFromLocalStateStore() throws IOException {
        JobID jobID = new JobID(42L, 43L);
        AllocationID allocationID = new AllocationID(4711L, 23L);
        JobVertexID jobVertexID = new JobVertexID(12L, 34L);
        ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
        TestCheckpointResponder checkpointResponderMock = new TestCheckpointResponder();

        Executor directExecutor = Executors.directExecutor();

        TemporaryFolder tmpFolder = new TemporaryFolder();

        try {
            tmpFolder.create();

            File[] allocBaseDirs =
                    new File[] {
                        tmpFolder.newFolder(), tmpFolder.newFolder(), tmpFolder.newFolder()
                    };

            LocalRecoveryDirectoryProviderImpl directoryProvider =
                    new LocalRecoveryDirectoryProviderImpl(allocBaseDirs, jobID, jobVertexID, 0);

            LocalRecoveryConfig localRecoveryConfig =
                    new LocalRecoveryConfig(true, directoryProvider);

            TaskLocalStateStore taskLocalStateStore =
                    new TaskLocalStateStoreImpl(
                            jobID,
                            allocationID,
                            jobVertexID,
                            13,
                            localRecoveryConfig,
                            directExecutor);

            InMemoryStateChangelogStorage changelogStorage = new InMemoryStateChangelogStorage();

            TaskStateManager taskStateManager =
                    taskStateManager(
                            jobID,
                            executionAttemptID,
                            checkpointResponderMock,
                            null,
                            taskLocalStateStore,
                            changelogStorage);

            LocalRecoveryConfig localRecoveryConfFromTaskLocalStateStore =
                    taskLocalStateStore.getLocalRecoveryConfig();

            LocalRecoveryConfig localRecoveryConfFromTaskStateManager =
                    taskStateManager.createLocalRecoveryConfig();

            for (int i = 0; i < 10; ++i) {
                Assert.assertEquals(
                        allocBaseDirs[i % allocBaseDirs.length],
                        localRecoveryConfFromTaskLocalStateStore
                                .getLocalStateDirectoryProvider()
                                .allocationBaseDirectory(i));
                Assert.assertEquals(
                        allocBaseDirs[i % allocBaseDirs.length],
                        localRecoveryConfFromTaskStateManager
                                .getLocalStateDirectoryProvider()
                                .allocationBaseDirectory(i));
            }

            Assert.assertEquals(
                    localRecoveryConfFromTaskLocalStateStore.isLocalRecoveryEnabled(),
                    localRecoveryConfFromTaskStateManager.isLocalRecoveryEnabled());
        } finally {
            tmpFolder.delete();
        }
    }

    @Test
    public void testStateRetrievingWithFinishedOperator() {
        TaskStateSnapshot taskStateSnapshot = TaskStateSnapshot.FINISHED_ON_RESTORE;

        JobManagerTaskRestore jobManagerTaskRestore =
                new JobManagerTaskRestore(2, taskStateSnapshot);
        TaskStateManagerImpl stateManager =
                new TaskStateManagerImpl(
                        new JobID(),
                        new ExecutionAttemptID(),
                        new TestTaskLocalStateStore(),
                        null,
                        jobManagerTaskRestore,
                        new TestCheckpointResponder());
        Assert.assertTrue(stateManager.isTaskDeployedAsFinished());
    }

    public void testAcquringRestoreCheckpointId() {
        TaskStateManagerImpl emptyStateManager =
                new TaskStateManagerImpl(
                        new JobID(),
                        new ExecutionAttemptID(),
                        new TestTaskLocalStateStore(),
                        null,
                        null,
                        new TestCheckpointResponder());
        Assert.assertFalse(emptyStateManager.getRestoreCheckpointId().isPresent());

        TaskStateManagerImpl nonEmptyStateManager =
                new TaskStateManagerImpl(
                        new JobID(),
                        new ExecutionAttemptID(),
                        new TestTaskLocalStateStore(),
                        null,
                        new JobManagerTaskRestore(2, new TaskStateSnapshot()),
                        new TestCheckpointResponder());
        Assert.assertEquals(2L, (long) nonEmptyStateManager.getRestoreCheckpointId().get());
    }

    public static TaskStateManager taskStateManager(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            CheckpointResponder checkpointResponderMock,
            JobManagerTaskRestore jobManagerTaskRestore,
            TaskLocalStateStore localStateStore,
            StateChangelogStorage<?> stateChangelogStorage) {

        return new TaskStateManagerImpl(
                jobID,
                executionAttemptID,
                localStateStore,
                stateChangelogStorage,
                jobManagerTaskRestore,
                checkpointResponderMock);
    }
}
