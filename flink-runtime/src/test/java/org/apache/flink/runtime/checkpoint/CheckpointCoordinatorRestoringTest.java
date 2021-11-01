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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.VertexFinishedStateChecker.VertexFinishedState;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.compareKeyedState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.comparePartitionableState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateChainedPartitionableStateHandle;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateKeyGroupState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generatePartitionableStateHandle;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockSubtaskState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.verifyStateRestore;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    private static void acknowledgeCheckpoint(
            CheckpointCoordinator coordinator,
            ExecutionGraph executionGraph,
            ExecutionJobVertex jobVertex,
            long checkpointId)
            throws Exception {
        final List<KeyGroupRange> partitions =
                StateAssignmentOperation.createKeyGroupPartitions(
                        jobVertex.getMaxParallelism(), jobVertex.getParallelism());
        for (int partitionIdx = 0; partitionIdx < partitions.size(); partitionIdx++) {
            TaskStateSnapshot subtaskState =
                    mockSubtaskState(
                            jobVertex.getJobVertexId(), partitionIdx, partitions.get(partitionIdx));
            final AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            executionGraph.getJobID(),
                            jobVertex
                                    .getTaskVertices()[partitionIdx]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskState);
            coordinator.receiveAcknowledgeMessage(
                    acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }
    }

    private static ExecutionGraph createExecutionGraph(List<TestingVertex> vertices)
            throws Exception {
        final CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder builder =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder();
        for (TestingVertex vertex : vertices) {
            builder.addJobVertex(
                    vertex.getId(), vertex.getParallelism(), vertex.getMaxParallelism());
        }
        return builder.build();
    }

    private static class TestingVertex {

        private final JobVertexID id;
        private final int parallelism;
        private final int maxParallelism;

        private TestingVertex(JobVertexID id, int parallelism, int maxParallelism) {
            this.id = id;
            this.parallelism = parallelism;
            this.maxParallelism = maxParallelism;
        }

        public JobVertexID getId() {
            return id;
        }

        public int getParallelism() {
            return parallelism;
        }

        public int getMaxParallelism() {
            return maxParallelism;
        }
    }

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * Tests that the checkpointed partitioned and non-partitioned state is assigned properly to the
     * {@link Execution} upon recovery.
     */
    @Test
    public void testRestoreLatestCheckpointedState() throws Exception {
        final List<TestingVertex> vertices =
                Arrays.asList(
                        new TestingVertex(new JobVertexID(), 3, 42),
                        new TestingVertex(new JobVertexID(), 2, 13));
        testRestoreLatestCheckpointedState(
                vertices,
                testSuccessfulCheckpointsArePersistedToCompletedCheckpointStore(vertices));
    }

    private Collection<CompletedCheckpoint>
            testSuccessfulCheckpointsArePersistedToCompletedCheckpointStore(
                    List<TestingVertex> vertices) throws Exception {
        final ExecutionGraph executionGraph = createExecutionGraph(vertices);
        final EmbeddedCompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore();

        // set up the coordinator and validate the initial state
        final CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(executionGraph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCompletedCheckpointStore(store)
                        .build();

        // trigger the checkpoint
        coordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        // we should have a single pending checkpoint
        assertEquals(1, coordinator.getPendingCheckpoints().size());
        final long checkpointId =
                Iterables.getOnlyElement(coordinator.getPendingCheckpoints().keySet());

        // acknowledge checkpoints from all vertex partitions
        for (TestingVertex vertex : vertices) {
            final ExecutionJobVertex executionVertex =
                    Objects.requireNonNull(executionGraph.getJobVertex(vertex.getId()));
            acknowledgeCheckpoint(coordinator, executionGraph, executionVertex, checkpointId);
        }

        final List<CompletedCheckpoint> completedCheckpoints =
                coordinator.getSuccessfulCheckpoints();
        assertEquals(1, completedCheckpoints.size());

        // shutdown the store
        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner());

        return store.getAllCheckpoints();
    }

    private void testRestoreLatestCheckpointedState(
            List<TestingVertex> vertices, Collection<CompletedCheckpoint> completedCheckpoints)
            throws Exception {
        final ExecutionGraph executionGraph = createExecutionGraph(vertices);
        final EmbeddedCompletedCheckpointStore store =
                new EmbeddedCompletedCheckpointStore(
                        completedCheckpoints.size(), completedCheckpoints);

        // set up the coordinator and validate the initial state
        final CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(executionGraph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCompletedCheckpointStore(store)
                        .build();

        final Set<ExecutionJobVertex> executionVertices =
                vertices.stream()
                        .map(TestingVertex::getId)
                        .map(executionGraph::getJobVertex)
                        .collect(Collectors.toSet());
        assertTrue(coordinator.restoreLatestCheckpointedStateToAll(executionVertices, false));

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
        for (ExecutionJobVertex executionVertex : executionVertices) {
            verifyStateRestore(executionVertex);
        }
    }

    @Test
    public void testRestoreLatestCheckpointedStateScaleIn() throws Exception {
        testRestoreLatestCheckpointedStateWithChangingParallelism(false);
    }

    @Test
    public void testRestoreLatestCheckpointedStateScaleOut() throws Exception {
        testRestoreLatestCheckpointedStateWithChangingParallelism(true);
    }

    /**
     * Tests the checkpoint restoration with changing parallelism of job vertex with partitioned
     * state.
     */
    private void testRestoreLatestCheckpointedStateWithChangingParallelism(boolean scaleOut)
            throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        int parallelism1 = 3;
        int parallelism2 = scaleOut ? 2 : 13;

        int maxParallelism1 = 42;
        int maxParallelism2 = 13;

        int newParallelism2 = scaleOut ? 13 : 2;

        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                        .addJobVertex(jobVertexID2, parallelism2, maxParallelism2)
                        .build();

        final ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        final ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(completedCheckpointStore)
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
                            .setInputChannelState(
                                    StateObjectCollection.singleton(
                                            createNewInputChannelStateHandle(3, new Random())))
                            .build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID1), operatorSubtaskState);

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
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
            expectedOpStatesBackend.add(new ChainedStateHandle<>(singletonList(opStateBackend)));
            expectedOpStatesRaw.add(new ChainedStateHandle<>(singletonList(opStateRaw)));

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
                            graph.getJobID(),
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

        List<KeyGroupRange> newKeyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

        // rescale vertex 2
        final ExecutionGraph newGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                        .addJobVertex(jobVertexID2, newParallelism2, maxParallelism2)
                        .build();

        final ExecutionJobVertex newJobVertex1 = newGraph.getJobVertex(jobVertexID1);
        final ExecutionJobVertex newJobVertex2 = newGraph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator newCoord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(newGraph)
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);
        assertTrue(newCoord.restoreLatestCheckpointedStateToAll(tasks, false));

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
                    compareKeyedState(singletonList(originalKeyedStateBackend), keyedStateBackend);
                    compareKeyedState(singletonList(originalKeyedStateRaw), keyGroupStateRaw);
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
        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        int parallelism1 = 3;
        int parallelism2 = 2;
        int maxParallelism1 = 42;
        int maxParallelism2 = 13;

        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                        .addJobVertex(jobVertexID2, parallelism2, maxParallelism2)
                        .build();
        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(completedCheckpointStore)
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
                            graph.getJobID(),
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
                            graph.getJobID(),
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

        int newMaxParallelism1 = 20;
        int newMaxParallelism2 = 42;

        ExecutionGraph newGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, newMaxParallelism1)
                        .addJobVertex(jobVertexID2, parallelism2, newMaxParallelism2)
                        .build();

        ExecutionJobVertex newJobVertex1 = newGraph.getJobVertex(jobVertexID1);
        ExecutionJobVertex newJobVertex2 = newGraph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator newCoord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(newGraph)
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);
        assertTrue(newCoord.restoreLatestCheckpointedStateToAll(tasks, false));

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

        ExecutionGraph newGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(
                                id5.f0,
                                newParallelism1,
                                maxParallelism1,
                                Stream.of(id2.f1, id1.f1, id5.f1)
                                        .map(OperatorIDPair::generatedIDOnly)
                                        .collect(Collectors.toList()),
                                true)
                        .addJobVertex(
                                id3.f0,
                                newParallelism2,
                                maxParallelism2,
                                Stream.of(id6.f1, id3.f1)
                                        .map(OperatorIDPair::generatedIDOnly)
                                        .collect(Collectors.toList()),
                                true)
                        .build();

        ExecutionJobVertex newJobVertex1 = newGraph.getJobVertex(id5.f0);
        ExecutionJobVertex newJobVertex2 = newGraph.getJobVertex(id3.f0);

        Set<ExecutionJobVertex> tasks = new HashSet<>();

        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);

        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        newGraph.getJobID(),
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
                        .setExecutionGraph(newGraph)
                        .setCompletedCheckpointStore(storeFor(() -> {}, completedCheckpoint))
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

            compareKeyedState(singletonList(originalKeyedStateBackend), keyedStateBackend);
            compareKeyedState(singletonList(originalKeyedStateRaw), keyGroupStateRaw);
        }

        comparePartitionableState(
                expectedManagedOperatorStates.get(0), actualManagedOperatorStates);
        comparePartitionableState(expectedRawOperatorStates.get(0), actualRawOperatorStates);
    }

    static CompletedCheckpointStore storeFor(
            Runnable postCleanupAction, CompletedCheckpoint... checkpoints) throws Exception {
        StandaloneCompletedCheckpointStore store =
                new StandaloneCompletedCheckpointStore(checkpoints.length);
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        for (final CompletedCheckpoint checkpoint : checkpoints) {
            store.addCheckpoint(checkpoint, checkpointsCleaner, postCleanupAction);
        }
        return store;
    }

    @Test
    public void testRestoreLatestCheckpointedStateWithoutInFlightData() throws Exception {
        // given: Operator with not empty states.
        final JobVertexID jobVertexID = new JobVertexID();
        int parallelism1 = 3;
        int maxParallelism1 = 42;

        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, parallelism1, maxParallelism1)
                        .build();

        final ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointCoordinatorConfiguration(
                                new CheckpointCoordinatorConfigurationBuilder()
                                        .setCheckpointIdOfIgnoredInFlightData(1)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertEquals(1, coord.getPendingCheckpoints().size());
        long checkpointId = Iterables.getOnlyElement(coord.getPendingCheckpoints().keySet());

        List<KeyGroupRange> keyGroupPartitions1 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);

        Random random = new Random();
        // fill the states and complete the checkpoint.
        for (int index = 0; index < jobVertex.getParallelism(); index++) {
            OperatorSubtaskState operatorSubtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(
                                    generatePartitionableStateHandle(
                                            jobVertexID, index, 2, 8, false))
                            .setRawOperatorState(
                                    generatePartitionableStateHandle(
                                            jobVertexID, index, 2, 8, true))
                            .setManagedKeyedState(
                                    generateKeyGroupState(
                                            jobVertexID, keyGroupPartitions1.get(index), false))
                            .setRawKeyedState(
                                    generateKeyGroupState(
                                            jobVertexID, keyGroupPartitions1.get(index), true))
                            .setInputChannelState(
                                    StateObjectCollection.singleton(
                                            createNewInputChannelStateHandle(3, random)))
                            .setResultSubpartitionState(
                                    StateObjectCollection.singleton(
                                            createNewResultSubpartitionStateHandle(3, random)))
                            .build();
            TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
            taskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(jobVertexID), operatorSubtaskState);

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            jobVertex
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates);

            coord.receiveAcknowledgeMessage(acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }

        assertEquals(1, coord.getSuccessfulCheckpoints().size());

        // when: Restore latest checkpoint without in-flight data.
        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(jobVertex);
        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

        // then: All states should be restored successfully except InputChannel and
        // ResultSubpartition which should be ignored.
        verifyStateRestore(jobVertexID, jobVertex, keyGroupPartitions1);
        for (int i = 0; i < jobVertex.getParallelism(); i++) {
            JobManagerTaskRestore taskRestore =
                    jobVertex.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
            Assert.assertEquals(1L, taskRestore.getRestoreCheckpointId());
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            OperatorSubtaskState operatorState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            OperatorID.fromJobVertexID(jobVertexID));

            assertTrue(operatorState.getInputChannelState().isEmpty());
            assertTrue(operatorState.getResultSubpartitionState().isEmpty());

            assertFalse(operatorState.getRawOperatorState().isEmpty());
            assertFalse(operatorState.getManagedOperatorState().isEmpty());
            assertFalse(operatorState.getRawKeyedState().isEmpty());
            assertFalse(operatorState.getManagedOperatorState().isEmpty());
        }
    }

    @Test
    public void testRestoreFinishedStateWithoutInFlightData() throws Exception {
        // given: Operator with not empty states.
        OperatorIDPair op1 = OperatorIDPair.generatedIDOnly(new OperatorID());
        final JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 1, 1, singletonList(op1), true)
                        .build();

        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();
        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(
                op1.getGeneratedOperatorID(),
                new FullyFinishedOperatorState(op1.getGeneratedOperatorID(), 1, 1));
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        graph.getJobID(),
                        2,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3000,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation());
        completedCheckpointStore.addCheckpoint(
                completedCheckpoint, new CheckpointsCleaner(), () -> {});

        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(
                                new CheckpointCoordinatorConfigurationBuilder()
                                        .setCheckpointIdOfIgnoredInFlightData(2)
                                        .build())
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .build();

        ExecutionJobVertex vertex = graph.getJobVertex(jobVertexID);
        coord.restoreInitialCheckpointIfPresent(Collections.singleton(vertex));
        TaskStateSnapshot restoredState =
                vertex.getTaskVertices()[0]
                        .getCurrentExecutionAttempt()
                        .getTaskRestore()
                        .getTaskStateSnapshot();
        assertTrue(restoredState.isTaskDeployedAsFinished());
    }

    @Test
    public void testRestoringPartiallyFinishedChainsFailsWithoutUidHash() throws Exception {
        // If useUidHash is set to false, the operator states would still be keyed with the
        // generated ID, which simulates the case of restoring a checkpoint taken after jobs
        // started. The checker should still be able to access the stored state correctly, otherwise
        // it would mark op1 as running and pass the check wrongly.
        testRestoringPartiallyFinishedChainsFails(false);
    }

    @Test
    public void testRestoringPartiallyFinishedChainsFailsWithUidHash() throws Exception {
        testRestoringPartiallyFinishedChainsFails(true);
    }

    private void testRestoringPartiallyFinishedChainsFails(boolean useUidHash) throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();
        final JobVertexID jobVertexID2 = new JobVertexID();
        // The op1 has uidHash set.
        OperatorIDPair op1 = OperatorIDPair.of(new OperatorID(), new OperatorID());
        OperatorIDPair op2 = OperatorIDPair.generatedIDOnly(new OperatorID());
        OperatorIDPair op3 = OperatorIDPair.generatedIDOnly(new OperatorID());

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID2, 1, 1, singletonList(op3), true)
                        .addJobVertex(jobVertexID1, 1, 1, Arrays.asList(op1, op2), true)
                        .build();

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(
                useUidHash ? op1.getUserDefinedOperatorID().get() : op1.getGeneratedOperatorID(),
                new FullyFinishedOperatorState(op1.getGeneratedOperatorID(), 1, 1));
        operatorStates.put(
                op2.getGeneratedOperatorID(),
                new OperatorState(op2.getGeneratedOperatorID(), 1, 1));
        CompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore();
        store.addCheckpoint(
                new CompletedCheckpoint(
                        graph.getJobID(),
                        2,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3000,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation()),
                new CheckpointsCleaner(),
                () -> {});

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        Set<ExecutionJobVertex> vertices = new HashSet<>();
        vertices.add(graph.getJobVertex(jobVertexID1));

        thrown.expect(FlinkRuntimeException.class);
        thrown.expectMessage(
                "Can not restore vertex "
                        + "anon("
                        + jobVertexID1
                        + ")"
                        + " which contain mixed operator finished state: [ALL_RUNNING, FULLY_FINISHED]");
        coord.restoreInitialCheckpointIfPresent(vertices);
    }

    @Test
    public void testAddingRunningOperatorBeforeFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedState.FULLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with fully finished vertices"
                        + " predeceased with the ones not fully finished. Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a predecessor not fully finished");
    }

    @Test
    public void testAddingPartiallyFinishedOperatorBeforeFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedState.FULLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with fully finished vertices"
                        + " predeceased with the ones not fully finished. Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a predecessor not fully finished");
    }

    @Test
    public void testAddingAllRunningOperatorBeforePartiallyFinishedOneWithAllToAllFails()
            throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a all running predecessor");
    }

    @Test
    public void testAddingPartiallyFinishedOperatorBeforePartiallyFinishedOneWithAllToAllFails()
            throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.ALL_TO_ALL},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a partially finished predecessor");
    }

    @Test
    public void
            testAddingPartiallyFinishedOperatorBeforePartiallyFinishedOneWithPointwiseAndAllToAllFails()
                    throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.PARTIALLY_FINISHED,
                jobVertexID2,
                "vert2",
                VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {
                    DistributionPattern.POINTWISE, DistributionPattern.ALL_TO_ALL
                },
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with running or partially finished ones and connected via the ALL_TO_ALL edges. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a partially finished predecessor");
    }

    @Test
    public void testAddingAllRunningOperatorBeforePartiallyFinishedOneFails() throws Exception {
        JobVertexID jobVertexID2 = new JobVertexID();

        testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
                new JobVertexID(),
                "vert1",
                VertexFinishedState.ALL_RUNNING,
                jobVertexID2,
                "vert2",
                VertexFinishedState.PARTIALLY_FINISHED,
                new DistributionPattern[] {DistributionPattern.POINTWISE},
                FlinkRuntimeException.class,
                "Illegal JobGraph modification. Cannot run a program with partially finished vertices"
                        + " predeceased with all running ones. "
                        + "Task vertex vert2"
                        + "("
                        + jobVertexID2
                        + ")"
                        + " has a all running predecessor");
    }

    private void testAddingOperatorsBeforePartiallyOrFullyFinishedOne(
            JobVertexID firstVertexId,
            String firstVertexName,
            VertexFinishedState firstOperatorFinishedState,
            JobVertexID secondVertexId,
            String secondVertexName,
            VertexFinishedState secondOperatorFinishedState,
            DistributionPattern[] distributionPatterns,
            Class<? extends Throwable> expectedExceptionalClass,
            String expectedMessage)
            throws Exception {
        OperatorIDPair op1 = OperatorIDPair.generatedIDOnly(new OperatorID());
        OperatorIDPair op2 = OperatorIDPair.generatedIDOnly(new OperatorID());
        JobVertex vertex1 = new JobVertex(firstVertexName, firstVertexId, singletonList(op1));
        JobVertex vertex2 = new JobVertex(secondVertexName, secondVertexId, singletonList(op2));
        vertex1.setInvokableClass(NoOpInvokable.class);
        vertex2.setInvokableClass(NoOpInvokable.class);

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(vertex1, true)
                        .addJobVertex(vertex2, false)
                        .setDistributionPattern(distributionPatterns[0])
                        .build();

        // Adds the additional edges
        for (int i = 1; i < distributionPatterns.length; ++i) {
            vertex2.connectNewDataSetAsInput(
                    vertex1, distributionPatterns[i], ResultPartitionType.PIPELINED);
        }

        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        operatorStates.put(
                op1.getGeneratedOperatorID(),
                createOperatorState(op1.getGeneratedOperatorID(), firstOperatorFinishedState));
        operatorStates.put(
                op2.getGeneratedOperatorID(),
                createOperatorState(op2.getGeneratedOperatorID(), secondOperatorFinishedState));

        CompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore();
        store.addCheckpoint(
                new CompletedCheckpoint(
                        graph.getJobID(),
                        2,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3000,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation()),
                new CheckpointsCleaner(),
                () -> {});

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        Set<ExecutionJobVertex> vertices = new HashSet<>();
        vertices.add(graph.getJobVertex(vertex1.getID()));
        vertices.add(graph.getJobVertex(vertex2.getID()));

        thrown.expect(expectedExceptionalClass);
        thrown.expectMessage(expectedMessage);

        coord.restoreInitialCheckpointIfPresent(vertices);
    }

    private OperatorState createOperatorState(
            OperatorID operatorId, VertexFinishedState finishedState) {
        switch (finishedState) {
            case ALL_RUNNING:
                return new OperatorState(operatorId, 2, 2);
            case PARTIALLY_FINISHED:
                OperatorState operatorState = new OperatorState(operatorId, 2, 2);
                operatorState.putState(0, FinishedOperatorSubtaskState.INSTANCE);
                return operatorState;
            case FULLY_FINISHED:
                return new FullyFinishedOperatorState(operatorId, 2, 2);
            default:
                throw new UnsupportedOperationException(
                        "Not supported finished state: " + finishedState);
        }
    }
}
