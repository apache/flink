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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
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
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for restoring checkpoint. */
@SuppressWarnings("checkstyle:EmptyLineSeparator")
class CheckpointCoordinatorRestoringTest extends TestLogger {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

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
        return builder.build(EXECUTOR_RESOURCE.getExecutor());
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

    @TempDir private java.nio.file.Path tmpFolder;

    @BeforeEach
    void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * Tests that the checkpointed partitioned and non-partitioned state is assigned properly to the
     * {@link Execution} upon recovery.
     */
    @Test
    void testRestoreLatestCheckpointedState() throws Exception {
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
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .setCompletedCheckpointStore(store)
                        .build(executionGraph);

        // trigger the checkpoint
        coordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        // we should have a single pending checkpoint
        assertThat(coordinator.getPendingCheckpoints().size()).isOne();
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
        assertThat(completedCheckpoints.size()).isOne();

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
                        completedCheckpoints.size(), completedCheckpoints, RestoreMode.DEFAULT);

        // set up the coordinator and validate the initial state
        final CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .setCompletedCheckpointStore(store)
                        .build(executionGraph);

        final Set<ExecutionJobVertex> executionVertices =
                vertices.stream()
                        .map(TestingVertex::getId)
                        .map(executionGraph::getJobVertex)
                        .collect(Collectors.toSet());
        assertThat(coordinator.restoreLatestCheckpointedStateToAll(executionVertices, false))
                .isTrue();

        // validate that all shared states are registered again after the recovery.
        for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
            for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                    verify(subtaskState, times(2))
                            .registerSharedStates(
                                    any(SharedStateRegistry.class),
                                    eq(completedCheckpoint.getCheckpointID()));
                }
            }
        }

        // verify the restored state
        for (ExecutionJobVertex executionVertex : executionVertices) {
            verifyStateRestore(executionVertex);
        }
    }

    @Test
    void testRestoreLatestCheckpointedStateScaleIn() throws Exception {
        testRestoreLatestCheckpointedStateWithChangingParallelism(false);
    }

    @Test
    void testRestoreLatestCheckpointedStateScaleOut() throws Exception {
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
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        final ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(coord.getPendingCheckpoints().size()).isOne();
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

        assertThat(completedCheckpoints.size()).isOne();

        List<KeyGroupRange> newKeyGroupPartitions2 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism2, newParallelism2);

        // rescale vertex 2
        final ExecutionGraph newGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                        .addJobVertex(jobVertexID2, newParallelism2, maxParallelism2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final ExecutionJobVertex newJobVertex1 = newGraph.getJobVertex(jobVertexID1);
        final ExecutionJobVertex newJobVertex2 = newGraph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator newCoord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(newGraph);

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);
        assertThat(newCoord.restoreLatestCheckpointedStateToAll(tasks, false)).isTrue();

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
            assertThat(taskRestore.getRestoreCheckpointId()).isOne();
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
    //    @Test(expected = IllegalStateException.class)
    @Test
    void testRestoreLatestCheckpointFailureWhenMaxParallelismChanges() throws Exception {
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
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(coord.getPendingCheckpoints().size()).isOne();
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

        assertThat(completedCheckpoints.size()).isOne();

        int newMaxParallelism1 = 20;
        int newMaxParallelism2 = 42;

        ExecutionGraph newGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, newMaxParallelism1)
                        .addJobVertex(jobVertexID2, parallelism2, newMaxParallelism2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionJobVertex newJobVertex1 = newGraph.getJobVertex(jobVertexID1);
        ExecutionJobVertex newJobVertex2 = newGraph.getJobVertex(jobVertexID2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator newCoord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(newGraph);

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(newJobVertex1);
        tasks.add(newJobVertex2);
        assertThatThrownBy(() -> newCoord.restoreLatestCheckpointedStateToAll(tasks, false))
                .as("The restoration should have failed because the max parallelism changed.")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testStateRecoveryWhenTopologyChangeOut() throws Exception {
        testStateRecoveryWithTopologyChange(TestScaleType.INCREASE_PARALLELISM);
    }

    @Test
    void testStateRecoveryWhenTopologyChangeIn() throws Exception {
        testStateRecoveryWithTopologyChange(TestScaleType.DECREASE_PARALLELISM);
    }

    @Test
    void testStateRecoveryWhenTopologyChange() throws Exception {
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
                        .build(EXECUTOR_RESOURCE.getExecutor());

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
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        // set up the coordinator and validate the initial state
        SharedStateRegistry sharedStateRegistry =
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        Executors.directExecutor(), emptyList(), RestoreMode.DEFAULT);
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(
                                storeFor(sharedStateRegistry, () -> {}, completedCheckpoint))
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(newGraph);

        coord.restoreLatestCheckpointedStateToAll(tasks, true);

        for (int i = 0; i < newJobVertex1.getParallelism(); i++) {

            final List<OperatorIDPair> operatorIDs = newJobVertex1.getOperatorIDs();

            JobManagerTaskRestore taskRestore =
                    newJobVertex1
                            .getTaskVertices()[i]
                            .getCurrentExecutionAttempt()
                            .getTaskRestore();
            assertThat(taskRestore.getRestoreCheckpointId()).isEqualTo(2L);
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            OperatorSubtaskState headOpState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID());
            assertThat(headOpState.getManagedKeyedState()).isEmpty();
            assertThat(headOpState.getRawKeyedState()).isEmpty();

            // operator5
            {
                int operatorIndexInChain = 2;
                OperatorSubtaskState opState =
                        stateSnapshot.getSubtaskStateByOperatorID(
                                operatorIDs.get(operatorIndexInChain).getGeneratedOperatorID());

                assertThat(opState.getManagedOperatorState()).isEmpty();
                assertThat(opState.getRawOperatorState()).isEmpty();
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
                assertThat(managedOperatorState.size()).isOne();
                assertThat(
                                CommonTestUtils.isStreamContentEqual(
                                        expectedManagedOpState.openInputStream(),
                                        managedOperatorState.iterator().next().openInputStream()))
                        .isTrue();

                Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
                assertThat(rawOperatorState.size()).isOne();
                assertThat(
                                CommonTestUtils.isStreamContentEqual(
                                        expectedRawOpState.openInputStream(),
                                        rawOperatorState.iterator().next().openInputStream()))
                        .isTrue();
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
                assertThat(managedOperatorState.size()).isOne();
                assertThat(
                                CommonTestUtils.isStreamContentEqual(
                                        expectedManagedOpState.openInputStream(),
                                        managedOperatorState.iterator().next().openInputStream()))
                        .isTrue();

                Collection<OperatorStateHandle> rawOperatorState = opState.getRawOperatorState();
                assertThat(rawOperatorState.size()).isOne();
                assertThat(
                                CommonTestUtils.isStreamContentEqual(
                                        expectedRawOpState.openInputStream(),
                                        rawOperatorState.iterator().next().openInputStream()))
                        .isTrue();
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
            assertThat(taskRestore.getRestoreCheckpointId()).isEqualTo(2L);
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
                assertThat(opState.getManagedOperatorState()).isEmpty();
                assertThat(opState.getRawOperatorState()).isEmpty();
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
            SharedStateRegistry sharedStateRegistry,
            Runnable postCleanupAction,
            CompletedCheckpoint... checkpoints)
            throws Exception {
        StandaloneCompletedCheckpointStore store =
                new StandaloneCompletedCheckpointStore(checkpoints.length);
        CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
        for (final CompletedCheckpoint checkpoint : checkpoints) {
            store.addCheckpointAndSubsumeOldestOne(
                    checkpoint, checkpointsCleaner, postCleanupAction);
        }
        return store;
    }

    @Test
    void testRestoreLatestCheckpointedStateWithoutInFlightData() throws Exception {
        // given: Operator with not empty states.
        final JobVertexID jobVertexID = new JobVertexID();
        int parallelism1 = 3;
        int maxParallelism1 = 42;

        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, parallelism1, maxParallelism1)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setCheckpointCoordinatorConfiguration(
                                new CheckpointCoordinatorConfigurationBuilder()
                                        .setCheckpointIdOfIgnoredInFlightData(1)
                                        .build())
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // trigger the checkpoint
        coord.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(coord.getPendingCheckpoints().size()).isOne();
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

        assertThat(coord.getSuccessfulCheckpoints().size()).isOne();

        // when: Restore latest checkpoint without in-flight data.
        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(jobVertex);
        assertThat(coord.restoreLatestCheckpointedStateToAll(tasks, false)).isTrue();

        // then: All states should be restored successfully except InputChannel and
        // ResultSubpartition which should be ignored.
        verifyStateRestore(jobVertexID, jobVertex, keyGroupPartitions1);
        for (int i = 0; i < jobVertex.getParallelism(); i++) {
            JobManagerTaskRestore taskRestore =
                    jobVertex.getTaskVertices()[i].getCurrentExecutionAttempt().getTaskRestore();
            assertThat(taskRestore.getRestoreCheckpointId()).isOne();
            TaskStateSnapshot stateSnapshot = taskRestore.getTaskStateSnapshot();

            OperatorSubtaskState operatorState =
                    stateSnapshot.getSubtaskStateByOperatorID(
                            OperatorID.fromJobVertexID(jobVertexID));

            assertThat(operatorState.getInputChannelState()).isEmpty();
            assertThat(operatorState.getResultSubpartitionState()).isEmpty();

            assertThat(operatorState.getRawOperatorState()).isNotEmpty();
            assertThat(operatorState.getManagedOperatorState()).isNotEmpty();
            assertThat(operatorState.getRawKeyedState()).isNotEmpty();
            assertThat(operatorState.getManagedOperatorState()).isNotEmpty();
        }
    }

    @Test
    void testRestoreFinishedStateWithoutInFlightData() throws Exception {
        // given: Operator with not empty states.
        OperatorIDPair op1 = OperatorIDPair.generatedIDOnly(new OperatorID());
        final JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 1, 1, singletonList(op1), true)
                        .build(EXECUTOR_RESOURCE.getExecutor());

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
                        new TestCompletedCheckpointStorageLocation(),
                        null);
        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint, new CheckpointsCleaner(), () -> {});

        CheckpointCoordinator coord =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                new CheckpointCoordinatorConfigurationBuilder()
                                        .setCheckpointIdOfIgnoredInFlightData(2)
                                        .build())
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .build(graph);

        ExecutionJobVertex vertex = graph.getJobVertex(jobVertexID);
        coord.restoreInitialCheckpointIfPresent(Collections.singleton(vertex));
        TaskStateSnapshot restoredState =
                vertex.getTaskVertices()[0]
                        .getCurrentExecutionAttempt()
                        .getTaskRestore()
                        .getTaskStateSnapshot();
        assertThat(restoredState.isTaskDeployedAsFinished()).isTrue();
    }

    @Test
    void testJobGraphModificationsAreCheckedForInitialCheckpoint() throws Exception {
        final JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 1, 1)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CompletedCheckpointStore completedCheckpointStore = new EmbeddedCompletedCheckpointStore();
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        graph.getJobID(),
                        2,
                        System.currentTimeMillis(),
                        System.currentTimeMillis() + 3000,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);
        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                completedCheckpoint, new CheckpointsCleaner(), () -> {});

        BooleanValue checked = new BooleanValue(false);
        CheckpointCoordinator restoreCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setVertexFinishedStateCheckerFactory(
                                (vertices, states) ->
                                        new VertexFinishedStateChecker(vertices, states) {
                                            @Override
                                            public void validateOperatorsFinishedState() {
                                                checked.set(true);
                                            }
                                        })
                        .build(graph);
        restoreCoordinator.restoreInitialCheckpointIfPresent(
                new HashSet<>(graph.getAllVertices().values()));
        assertThat(checked.get())
                .as("The finished states should be checked when job is restored on startup")
                .isTrue();
    }

    @Test
    void testJobGraphModificationsAreCheckedForSavepoint() throws Exception {
        final JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 1, 1)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);
        File savepointPath = TempDirUtils.newFolder(tmpFolder);
        CompletableFuture<CompletedCheckpoint> savepointFuture =
                coordinator.triggerSavepoint(
                        "file://" + savepointPath.getAbsolutePath(), SavepointFormatType.CANONICAL);
        manuallyTriggeredScheduledExecutor.triggerAll();
        long pendingSavepointId =
                coordinator.getPendingCheckpoints().keySet().stream().findFirst().get();
        coordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        graph.getJobVertex(jobVertexID)
                                .getTaskVertices()[0]
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        pendingSavepointId),
                "localhost");
        assertThat(savepointFuture).isDone();

        BooleanValue checked = new BooleanValue(false);
        CheckpointCoordinator restoreCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setVertexFinishedStateCheckerFactory(
                                (vertices, states) ->
                                        new VertexFinishedStateChecker(vertices, states) {
                                            @Override
                                            public void validateOperatorsFinishedState() {
                                                checked.set(true);
                                            }
                                        })
                        .build(graph);
        restoreCoordinator.restoreSavepoint(
                SavepointRestoreSettings.forPath(savepointFuture.get().getExternalPointer()),
                graph.getAllVertices(),
                getClass().getClassLoader());
        assertThat(checked.get())
                .as("The finished states should be checked when job is restored on startup")
                .isTrue();
    }
}
