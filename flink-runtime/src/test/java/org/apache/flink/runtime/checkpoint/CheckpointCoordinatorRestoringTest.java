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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.SerializableObject;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.compareKeyedState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.comparePartitionableState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateChainedPartitionableStateHandle;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateKeyGroupState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generatePartitionableStateHandle;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecution;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionJobVertex;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockSubtaskState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.verifyStateRestore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for restoring checkpoint. */
@SuppressWarnings("checkstyle:EmptyLineSeparator")
public class CheckpointCoordinatorRestoringTest extends TestLogger {
    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private enum TestScaleType {
        INCREASE_PARALLELISM,
        DECREASE_PARALLELISM,
        SAME_PARALLELISM;
    }

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * Tests that the checkpointed partitioned and non-partitioned state is assigned properly to the
     * {@link Execution} upon recovery.
     *
     * @throws Exception
     */
    @Test
    public void testRestoreLatestCheckpointedState() throws Exception {
        final JobID jid = new JobID();

        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        int parallelism1 = 3;
        int parallelism2 = 2;
        int maxParallelism1 = 42;
        int maxParallelism2 = 13;

        final ExecutionJobVertex jobVertex1 =
                mockExecutionJobVertex(jobVertexID1, parallelism1, maxParallelism1);
        final ExecutionJobVertex jobVertex2 =
                mockExecutionJobVertex(jobVertexID2, parallelism2, maxParallelism2);

        List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

        allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
        allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

        ExecutionVertex[] arrayExecutionVertices =
                allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

        CompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setJobId(jid)
                        .setTasks(arrayExecutionVertices)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, coord.getPendingCheckpoints().size());
        long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

        List<KeyGroupRange> keyGroupPartitions1 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
        List<KeyGroupRange> keyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

        for (int index = 0; index < jobVertex1.getParallelism(); index++) {
            TaskStateSnapshot subtaskState =
                    mockSubtaskState(jobVertexID1, index, keyGroupPartitions1.get(index));

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex1
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskState);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        for (int index = 0; index < jobVertex2.getParallelism(); index++) {
            TaskStateSnapshot subtaskState =
                    mockSubtaskState(jobVertexID2, index, keyGroupPartitions2.get(index));

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex2
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskState);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

        assertEquals(1, completedCheckpoints.size());

        // shutdown the store
        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner(), () -> {});

        // restore the store
        Set<ExecutionJobVertex> tasks = new HashSet<>();

        tasks.add(jobVertex1);
        tasks.add(jobVertex2);

        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

        // validate that all shared states are registered again after the recovery.
        for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
            for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                    verify(subtaskState, times(2))
                            .registerSharedStates(any(SharedStateRegistry.class));
                }
            }
        }

        // verify the restored state
        verifyStateRestore(jobVertexID1, jobVertex1, keyGroupPartitions1);
        verifyStateRestore(jobVertexID2, jobVertex2, keyGroupPartitions2);
    }

    @Test
    public void testRestoreLatestCheckpointedStateScaleIn() throws Exception {
        testRestoreLatestCheckpointedStateWithChangingParallelism(false);
    }

    @Test
    public void testRestoreLatestCheckpointedStateScaleOut() throws Exception {
        testRestoreLatestCheckpointedStateWithChangingParallelism(true);
    }

    @Test
    public void testRestoreLatestCheckpointWhenPreferCheckpoint() throws Exception {
        testRestoreLatestCheckpointIsPreferSavepoint(true);
    }

    @Test
    public void testRestoreLatestCheckpointWhenPreferSavepoint() throws Exception {
        testRestoreLatestCheckpointIsPreferSavepoint(false);
    }

    private void testRestoreLatestCheckpointIsPreferSavepoint(boolean isPreferCheckpoint) {
        try {
            final JobID jid = new JobID();
            StandaloneCheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

            final JobVertexID statefulId = new JobVertexID();
            final JobVertexID statelessId = new JobVertexID();

            Execution statefulExec1 = mockExecution();
            Execution statelessExec1 = mockExecution();

            ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0, 1);
            ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0, 1);

            ExecutionJobVertex stateful =
                    mockExecutionJobVertex(statefulId, new ExecutionVertex[] {stateful1});
            ExecutionJobVertex stateless =
                    mockExecutionJobVertex(statelessId, new ExecutionVertex[] {stateless1});

            Set<ExecutionJobVertex> tasks = new HashSet<>();
            tasks.add(stateful);
            tasks.add(stateless);

            CompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore(2);

            CheckpointCoordinatorConfiguration chkConfig =
                    new CheckpointCoordinatorConfigurationBuilder()
                            .setPreferCheckpointForRecovery(isPreferCheckpoint)
                            .build();
            CheckpointCoordinator coord =
                    new CheckpointCoordinatorBuilder()
                            .setJobId(jid)
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setCheckpointIDCounter(checkpointIDCounter)
                            .setCompletedCheckpointStore(store)
                            .setTasks(new ExecutionVertex[] {stateful1, stateless1})
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            // trigger a checkpoint and wait to become a completed checkpoint
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    coord.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertFalse(checkpointFuture.isCompletedExceptionally());

            long checkpointId = checkpointIDCounter.getLast();

            KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 0);
            List<SerializableObject> testStates =
                    Collections.singletonList(new SerializableObject());
            KeyedStateHandle serializedKeyGroupStates =
                    generateKeyGroupState(keyGroupRange, testStates);

            TaskStateSnapshot subtaskStatesForCheckpoint = new TaskStateSnapshot();

            subtaskStatesForCheckpoint.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(statefulId),
                    OperatorSubtaskState.builder()
                            .setManagedKeyedState(serializedKeyGroupStates)
                            .build());

            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            statefulExec1.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStatesForCheckpoint),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            CompletedCheckpoint success = coord.getSuccessfulCheckpoints().get(0);
            assertEquals(jid, success.getJobId());

            // trigger a savepoint and wait it to be finished
            String savepointDir = tmpFolder.newFolder().getAbsolutePath();
            CompletableFuture<CompletedCheckpoint> savepointFuture =
                    coord.triggerSavepoint(savepointDir);

            KeyGroupRange keyGroupRangeForSavepoint = KeyGroupRange.of(1, 1);
            List<SerializableObject> testStatesForSavepoint =
                    Collections.singletonList(new SerializableObject());
            KeyedStateHandle serializedKeyGroupStatesForSavepoint =
                    generateKeyGroupState(keyGroupRangeForSavepoint, testStatesForSavepoint);

            TaskStateSnapshot subtaskStatesForSavepoint = new TaskStateSnapshot();

            subtaskStatesForSavepoint.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(statefulId),
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(StateObjectCollection.empty())
                            .setRawOperatorState(StateObjectCollection.empty())
                            .setManagedKeyedState(
                                    StateObjectCollection.singleton(
                                            serializedKeyGroupStatesForSavepoint))
                            .setRawKeyedState(StateObjectCollection.empty())
                            .build());

            manuallyTriggeredScheduledExecutor.triggerAll();
            checkpointId = checkpointIDCounter.getLast();
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            statefulExec1.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStatesForSavepoint),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            assertNotNull(savepointFuture.get());

            // restore and jump the latest savepoint
            assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

            // compare and see if it used the checkpoint's subtaskStates
            BaseMatcher<JobManagerTaskRestore> matcher =
                    new BaseMatcher<JobManagerTaskRestore>() {
                        @Override
                        public boolean matches(Object o) {
                            if (o instanceof JobManagerTaskRestore) {
                                JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
                                if (isPreferCheckpoint) {
                                    return Objects.equals(
                                            taskRestore.getTaskStateSnapshot(),
                                            subtaskStatesForCheckpoint);
                                } else {
                                    return Objects.equals(
                                            taskRestore.getTaskStateSnapshot(),
                                            subtaskStatesForSavepoint);
                                }
                            }
                            return false;
                        }

                        @Override
                        public void describeTo(Description description) {
                            if (isPreferCheckpoint) {
                                description.appendValue(subtaskStatesForCheckpoint);
                            } else {
                                description.appendValue(subtaskStatesForSavepoint);
                            }
                        }
                    };

            verify(statefulExec1, times(1)).setInitialState(MockitoHamcrest.argThat(matcher));
            verify(statelessExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());

            coord.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests the checkpoint restoration with changing parallelism of job vertex with partitioned
     * state.
     *
     * @throws Exception
     */
    private void testRestoreLatestCheckpointedStateWithChangingParallelism(boolean scaleOut)
            throws Exception {
        final JobID jid = new JobID();

        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        int parallelism1 = 3;
        int parallelism2 = scaleOut ? 2 : 13;

        int maxParallelism1 = 42;
        int maxParallelism2 = 13;

        int newParallelism2 = scaleOut ? 13 : 2;

        final ExecutionJobVertex jobVertex1 =
                mockExecutionJobVertex(jobVertexID1, parallelism1, maxParallelism1);
        final ExecutionJobVertex jobVertex2 =
                mockExecutionJobVertex(jobVertexID2, parallelism2, maxParallelism2);

        List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

        allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
        allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

        ExecutionVertex[] arrayExecutionVertices =
                allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setJobId(jid)
                        .setTasks(arrayExecutionVertices)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, coord.getPendingCheckpoints().size());
        long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

        List<KeyGroupRange> keyGroupPartitions1 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
        List<KeyGroupRange> keyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

        // vertex 1
        for (int index = 0; index < jobVertex1.getParallelism(); index++) {
            OperatorStateHandle opStateBackend =
                    generatePartitionableStateHandle(jobVertexID1, index, 2, 8, false);
            KeyGroupsStateHandle keyedStateBackend =
                    generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
            KeyGroupsStateHandle keyedStateRaw =
                    generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), true);
            OperatorSubtaskState operatorSubtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(opStateBackend)
                            .setManagedKeyedState(keyedStateBackend)
                            .setRawKeyedState(keyedStateRaw)
                            .build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex1
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        // vertex 2
        final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesBackend =
                new ArrayList<>(jobVertex2.getParallelism());
        final List<ChainedStateHandle<OperatorStateHandle>> expectedOpStatesRaw =
                new ArrayList<>(jobVertex2.getParallelism());
        for (int index = 0; index < jobVertex2.getParallelism(); index++) {
            KeyGroupsStateHandle keyedStateBackend =
                    generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
            KeyGroupsStateHandle keyedStateRaw =
                    generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), true);
            OperatorStateHandle opStateBackend =
                    generatePartitionableStateHandle(jobVertexID2, index, 2, 8, false);
            OperatorStateHandle opStateRaw =
                    generatePartitionableStateHandle(jobVertexID2, index, 2, 8, true);
            expectedOpStatesBackend.add(
                    new ChainedStateHandle<>(Collections.singletonList(opStateBackend)));
            expectedOpStatesRaw.add(
                    new ChainedStateHandle<>(Collections.singletonList(opStateRaw)));

            OperatorSubtaskState operatorSubtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(opStateBackend)
                            .setRawOperatorState(opStateRaw)
                            .setManagedKeyedState(keyedStateBackend)
                            .setRawKeyedState(keyedStateRaw)
                            .build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex2
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

        assertEquals(1, completedCheckpoints.size());

        Set<ExecutionJobVertex> tasks = new HashSet<>();

        List<KeyGroupRange> newKeyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

        final ExecutionJobVertex newJobVertex1 =
                mockExecutionJobVertex(jobVertexID1, parallelism1, maxParallelism1);

        // rescale vertex 2
        final ExecutionJobVertex newJobVertex2 =
                mockExecutionJobVertex(jobVertexID2, newParallelism2, maxParallelism2);

        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);
        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

        // verify the restored state
        verifyStateRestore(jobVertexID1, newJobVertex1, keyGroupPartitions1);
        List<List<Collection<OperatorStateHandle>>> actualOpStatesBackend =
                new ArrayList<>(newJobVertex2.getParallelism());
        List<List<Collection<OperatorStateHandle>>> actualOpStatesRaw =
                new ArrayList<>(newJobVertex2.getParallelism());
        for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

            List<OperatorIDPair> operatorIDs = newJobVertex2.getOperatorIDs();

            KeyGroupsStateHandle originalKeyedStateBackend =
                    generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), false);
            KeyGroupsStateHandle originalKeyedStateRaw =
                    generateKeyGroupState(jobVertexID2, newKeyGroupPartitions2.get(i), true);

            JobManagerTaskRestore taskRestore =
                    newJobVertex2
                            .getTaskVertices()[i]
                            .getCurrentExecutionAttempt()
                            .getTaskRestore();
            Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
            TaskStateSnapshot taskStateHandles = taskRestore.getTaskStateSnapshot();

            final int headOpIndex = operatorIDs.size() - 1;
            List<Collection<OperatorStateHandle>> allParallelManagedOpStates =
                    new ArrayList<>(operatorIDs.size());
            List<Collection<OperatorStateHandle>> allParallelRawOpStates =
                    new ArrayList<>(operatorIDs.size());

            for (int idx = 0; idx < operatorIDs.size(); ++idx) {
                OperatorID operatorID = operatorIDs.get(idx).getGeneratedOperatorID();
                OperatorSubtaskState opState =
                        taskStateHandles.getSubtaskStateByOperatorID(operatorID);
                Collection<OperatorStateHandle> opStateBackend = opState.getManagedOperatorState();
                Collection<OperatorStateHandle> opStateRaw = opState.getRawOperatorState();
                allParallelManagedOpStates.add(opStateBackend);
                allParallelRawOpStates.add(opStateRaw);
                if (idx == headOpIndex) {
                    Collection<KeyedStateHandle> keyedStateBackend = opState.getManagedKeyedState();
                    Collection<KeyedStateHandle> keyGroupStateRaw = opState.getRawKeyedState();
                    compareKeyedState(
                            Collections.singletonList(originalKeyedStateBackend),
                            keyedStateBackend);
                    compareKeyedState(
                            Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
                }
            }
            actualOpStatesBackend.add(allParallelManagedOpStates);
            actualOpStatesRaw.add(allParallelRawOpStates);
        }

        comparePartitionableState(expectedOpStatesBackend, actualOpStatesBackend);
        comparePartitionableState(expectedOpStatesRaw, actualOpStatesRaw);
    }

    /**
     * Tests that the checkpoint restoration fails if the max parallelism of the job vertices has
     * changed.
     *
     * @throws Exception
     */
    @Test(expected = IllegalStateException.class)
    public void testRestoreLatestCheckpointFailureWhenMaxParallelismChanges() throws Exception {
        final JobID jid = new JobID();

        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        int parallelism1 = 3;
        int parallelism2 = 2;
        int maxParallelism1 = 42;
        int maxParallelism2 = 13;

        final ExecutionJobVertex jobVertex1 =
                mockExecutionJobVertex(jobVertexID1, parallelism1, maxParallelism1);
        final ExecutionJobVertex jobVertex2 =
                mockExecutionJobVertex(jobVertexID2, parallelism2, maxParallelism2);

        List<ExecutionVertex> allExecutionVertices = new ArrayList<>(parallelism1 + parallelism2);

        allExecutionVertices.addAll(Arrays.asList(jobVertex1.getTaskVertices()));
        allExecutionVertices.addAll(Arrays.asList(jobVertex2.getTaskVertices()));

        ExecutionVertex[] arrayExecutionVertices =
                allExecutionVertices.toArray(new ExecutionVertex[allExecutionVertices.size()]);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setJobId(jid)
                        .setTasks(arrayExecutionVertices)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, coord.getPendingCheckpoints().size());
        long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

        List<KeyGroupRange> keyGroupPartitions1 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);
        List<KeyGroupRange> keyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

        for (int index = 0; index < jobVertex1.getParallelism(); index++) {
            KeyGroupsStateHandle keyGroupState =
                    generateKeyGroupState(jobVertexID1, keyGroupPartitions1.get(index), false);
            OperatorSubtaskState operatorSubtaskState =
                    OperatorSubtaskState.builder().setManagedKeyedState(keyGroupState).build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);
            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex1
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        for (int index = 0; index < jobVertex2.getParallelism(); index++) {
            KeyGroupsStateHandle keyGroupState =
                    generateKeyGroupState(jobVertexID2, keyGroupPartitions2.get(index), false);
            OperatorSubtaskState operatorSubtaskState =
                    OperatorSubtaskState.builder().setManagedKeyedState(keyGroupState).build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID2), operatorSubtaskState);
            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jid,
                            jobVertex2
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        List<CompletedCheckpoint> completedCheckpoints = coord.getSuccessfulCheckpoints();

        assertEquals(1, completedCheckpoints.size());

        Set<ExecutionJobVertex> tasks = new HashSet<>();

        int newMaxParallelism1 = 20;
        int newMaxParallelism2 = 42;

        final ExecutionJobVertex newJobVertex1 =
                mockExecutionJobVertex(jobVertexID1, parallelism1, newMaxParallelism1);

        final ExecutionJobVertex newJobVertex2 =
                mockExecutionJobVertex(jobVertexID2, parallelism2, newMaxParallelism2);

        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);

        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

        fail("The restoration should have failed because the max parallelism changed.");
    }

    @Test
    public void testStateRecoveryWhenTopologyChangeOut() throws Exception {
        testStateRecoveryWithTopologyChange(TestScaleType.INCREASE_PARALLELISM);
    }

    @Test
    public void testStateRecoveryWhenTopologyChangeIn() throws Exception {
        testStateRecoveryWithTopologyChange(TestScaleType.DECREASE_PARALLELISM);
    }

    @Test
    public void testStateRecoveryWhenTopologyChange() throws Exception {
        testStateRecoveryWithTopologyChange(TestScaleType.SAME_PARALLELISM);
    }

    private static Tuple2<JobVertexID, OperatorID> generateIDPair() {
        JobVertexID jobVertexID = new JobVertexID();
        OperatorID operatorID = OperatorID.fromJobVertexID(jobVertexID);
        return new Tuple2<>(jobVertexID, operatorID);
    }

    /**
     * old topology. [operator1,operator2] * parallelism1 -> [operator3,operator4] * parallelism2
     *
     * <p>new topology
     *
     * <p>[operator5,operator1,operator3] * newParallelism1 -> [operator3, operator6] *
     * newParallelism2
     */
    public void testStateRecoveryWithTopologyChange(TestScaleType scaleType) throws Exception {

        /*
         * Old topology
         * CHAIN(op1 -> op2) * parallelism1 -> CHAIN(op3 -> op4) * parallelism2
         */
        Tuple2<JobVertexID, OperatorID> id1 = generateIDPair();
        Tuple2<JobVertexID, OperatorID> id2 = generateIDPair();
        int parallelism1 = 10;
        int maxParallelism1 = 64;

        Tuple2<JobVertexID, OperatorID> id3 = generateIDPair();
        Tuple2<JobVertexID, OperatorID> id4 = generateIDPair();
        int parallelism2 = 10;
        int maxParallelism2 = 64;

        List<KeyGroupRange> keyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, parallelism2);

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();

        // prepare vertex1 state
        for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id1, id2)) {
            OperatorState taskState = new OperatorState(id.f1, parallelism1, maxParallelism1);
            operatorStates.put(id.f1, taskState);
            for (int index = 0; index < taskState.getParallelism(); index++) {
                OperatorSubtaskState subtaskState =
                        OperatorSubtaskState.builder()
                                .setManagedOperatorState(
                                        generatePartitionableStateHandle(id.f0, index, 2, 8, false))
                                .setRawOperatorState(
                                        generatePartitionableStateHandle(id.f0, index, 2, 8, true))
                                .build();
                taskState.putState(index, subtaskState);
            }
        }

        List<List<ChainedStateHandle<OperatorStateHandle>>> expectedManagedOperatorStates =
                new ArrayList<>();
        List<List<ChainedStateHandle<OperatorStateHandle>>> expectedRawOperatorStates =
                new ArrayList<>();
        // prepare vertex2 state
        for (Tuple2<JobVertexID, OperatorID> id : Arrays.asList(id3, id4)) {
            OperatorState operatorState = new OperatorState(id.f1, parallelism2, maxParallelism2);
            operatorStates.put(id.f1, operatorState);
            List<ChainedStateHandle<OperatorStateHandle>> expectedManagedOperatorState =
                    new ArrayList<>();
            List<ChainedStateHandle<OperatorStateHandle>> expectedRawOperatorState =
                    new ArrayList<>();
            expectedManagedOperatorStates.add(expectedManagedOperatorState);
            expectedRawOperatorStates.add(expectedRawOperatorState);

            for (int index = 0; index < operatorState.getParallelism(); index++) {
                final OperatorSubtaskState.Builder stateBuilder = OperatorSubtaskState.builder();
                OperatorStateHandle subManagedOperatorState =
                        generateChainedPartitionableStateHandle(id.f0, index, 2, 8, false).get(0);
                OperatorStateHandle subRawOperatorState =
                        generateChainedPartitionableStateHandle(id.f0, index, 2, 8, true).get(0);
                if (id.f0.equals(id3.f0)) {
                    stateBuilder.setManagedKeyedState(
                            generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), false));
                }
                if (id.f0.equals(id3.f0)) {
                    stateBuilder.setRawKeyedState(
                            generateKeyGroupState(id.f0, keyGroupPartitions2.get(index), true));
                }

                expectedManagedOperatorState.add(
                        ChainedStateHandle.wrapSingleHandle(subManagedOperatorState));
                expectedRawOperatorState.add(
                        ChainedStateHandle.wrapSingleHandle(subRawOperatorState));

                OperatorSubtaskState subtaskState =
                        stateBuilder
                                .setManagedOperatorState(subManagedOperatorState)
                                .setRawOperatorState(subRawOperatorState)
                                .build();
                operatorState.putState(index, subtaskState);
            }
        }

        /*
         * New topology
         * CHAIN(op5 -> op1 -> op2) * newParallelism1 -> CHAIN(op3 -> op6) * newParallelism2
         */
        Tuple2<JobVertexID, OperatorID> id5 = generateIDPair();
        int newParallelism1 = 10;

        Tuple2<JobVertexID, OperatorID> id6 = generateIDPair();
        int newParallelism2 = parallelism2;

        if (scaleType == TestScaleType.INCREASE_PARALLELISM) {
            newParallelism2 = 20;
        } else if (scaleType == TestScaleType.DECREASE_PARALLELISM) {
            newParallelism2 = 8;
        }

        List<KeyGroupRange> newKeyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

        final ExecutionJobVertex newJobVertex1 =
                mockExecutionJobVertex(
                        id5.f0,
                        Arrays.asList(id2.f1, id1.f1, id5.f1),
                        newParallelism1,
                        maxParallelism1);

        final ExecutionJobVertex newJobVertex2 =
                mockExecutionJobVertex(
                        id3.f0, Arrays.asList(id6.f1, id3.f1), newParallelism2, maxParallelism2);

        Set<ExecutionJobVertex> tasks = new HashSet<>();

        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);

        JobID jobID = new JobID();

        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobID,
                        2,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3000,
                        operatorStates,
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation());

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setTasks(newJobVertex1.getTaskVertices())
                        .setCompletedCheckpointStore(
                                CompletedCheckpointStore.storeFor(() -> {}, completedCheckpoint))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        coord.restoreLatestCheckpointedStateToAll(tasks, true);

        for (int i = 0; i < newJobVertex1.getParallelism(); i++) {

            final List<OperatorIDPair> operatorIDs = newJobVertex1.getOperatorIDs();

            JobManagerTaskRestore taskRestore =
                    newJobVertex1
                            .getTaskVertices()[i]
                            .getCurrentExecutionAttempt()
                            .getTaskRestore();
            Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            OperatorSubtaskState headOpState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID());
            assertTrue(headOpState.getManagedKeyedState().isEmpty());
            assertTrue(headOpState.getRawKeyedState().isEmpty());

            // operator5
            {
                int operatorIndexInChain = 2;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());

                assertTrue(opState.getManagedOperatorState().isEmpty());
                assertTrue(opState.getRawOperatorState().isEmpty());
            }
            // operator1
            {
                int operatorIndexInChain = 1;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());

                OperatorStateHandle expectedManagedOpState =
                        generatePartitionableStateHandle(id1.f0, i, 2, 8, false);
                OperatorStateHandle expectedRawOpState =
                        generatePartitionableStateHandle(id1.f0, i, 2, 8, true);

                Collection<OperatorStateHandle> managedOperatorState =
                        opState.getManagedOperatorState();
                assertEquals(1, managedOperatorState.size());
                assertTrue(
                        CommonTestUtils.isStreamContentEqual(
                                expectedManagedOpState.openInputStream(),
                                managedOperatorState.iterator().next().openInputStream()));

                Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
                assertEquals(1, rawOperatorState.size());
                assertTrue(
                        CommonTestUtils.isStreamContentEqual(
                                expectedRawOpState.openInputStream(),
                                rawOperatorState.iterator().next().openInputStream()));
            }
            // operator2
            {
                int operatorIndexInChain = 0;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());

                OperatorStateHandle expectedManagedOpState =
                        generatePartitionableStateHandle(id2.f0, i, 2, 8, false);
                OperatorStateHandle expectedRawOpState =
                        generatePartitionableStateHandle(id2.f0, i, 2, 8, true);

                Collection<OperatorStateHandle> managedOperatorState =
                        opState.getManagedOperatorState();
                assertEquals(1, managedOperatorState.size());
                assertTrue(
                        CommonTestUtils.isStreamContentEqual(
                                expectedManagedOpState.openInputStream(),
                                managedOperatorState.iterator().next().openInputStream()));

                Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
                assertEquals(1, rawOperatorState.size());
                assertTrue(
                        CommonTestUtils.isStreamContentEqual(
                                expectedRawOpState.openInputStream(),
                                rawOperatorState.iterator().next().openInputStream()));
            }
        }

        List<List<Collection<OperatorStateHandle>>> actualManagedOperatorStates =
                new ArrayList<>(newJobVertex2.getParallelism());
        List<List<Collection<OperatorStateHandle>>> actualRawOperatorStates =
                new ArrayList<>(newJobVertex2.getParallelism());

        for (int i = 0; i < newJobVertex2.getParallelism(); i++) {

            final List<OperatorIDPair> operatorIDs = newJobVertex2.getOperatorIDs();

            JobManagerTaskRestore taskRestore =
                    newJobVertex2
                            .getTaskVertices()[i]
                            .getCurrentExecutionAttempt()
                            .getTaskRestore();
            Assert.assertEquals(2L, taskRestore.getRestoreCheckpointId());
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            // operator 3
            {
                int operatorIndexInChain = 1;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());

                List<Collection<OperatorStateHandle>> actualSubManagedOperatorState =
                        new ArrayList<>(1);
                actualSubManagedOperatorState.add(opState.getManagedOperatorState());

                List<Collection<OperatorStateHandle>> actualSubRawOperatorState =
                        new ArrayList<>(1);
                actualSubRawOperatorState.add(opState.getRawOperatorState());

                actualManagedOperatorStates.add(actualSubManagedOperatorState);
                actualRawOperatorStates.add(actualSubRawOperatorState);
            }

            // operator 6
            {
                int operatorIndexInChain = 0;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());
                assertTrue(opState.getManagedOperatorState().isEmpty());
                assertTrue(opState.getRawOperatorState().isEmpty());
            }

            KeyGroupsStateHandle originalKeyedStateBackend =
                    generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), false);
            KeyGroupsStateHandle originalKeyedStateRaw =
                    generateKeyGroupState(id3.f0, newKeyGroupPartitions2.get(i), true);

            OperatorSubtaskState headOpState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID());

            Collection<KeyedStateHandle> keyedStateBackend = headOpState.getManagedKeyedState();
            Collection<KeyedStateHandle> keyGroupStateRaw = headOpState.getRawKeyedState();

            compareKeyedState(
                    Collections.singletonList(originalKeyedStateBackend), keyedStateBackend);
            compareKeyedState(Collections.singletonList(originalKeyedStateRaw), keyGroupStateRaw);
        }

        comparePartitionableState(
                expectedManagedOperatorStates.get(0), actualManagedOperatorStates);
        comparePartitionableState(expectedRawOperatorStates.get(0), actualRawOperatorStates);
    }
}
