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
 *
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAllocator;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateKeyGroupState;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.acknowledgePendingCheckpoint;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.setAllExecutionsToRunning;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.waitForCheckpointInProgress;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.waitForCompletedCheckpoint;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.waitForJobStatusRunning;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlotAllocator.getArgumentCapturingDelegatingSlotAllocator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalRecoveryTest extends AdaptiveSchedulerTestBase {
    @Test
    void testStateSizeIsConsideredForLocalRecoveryOnRestart() throws Exception {
        final JobGraph jobGraph = createJobGraphWithCheckpointing(JOB_VERTEX);
        final DeclarativeSlotPool slotPool = getSlotPoolWithFreeSlots(PARALLELISM);
        final List<JobAllocationsInformation> capturedAllocations = new ArrayList<>();
        final boolean localRecoveryEnabled = true;
        final String executionTarget = "local";
        final boolean minimalTaskManagerPreferred = false;
        final SlotAllocator slotAllocator =
                getArgumentCapturingDelegatingSlotAllocator(
                        AdaptiveSchedulerFactory.createSlotSharingSlotAllocator(
                                slotPool,
                                localRecoveryEnabled,
                                executionTarget,
                                minimalTaskManagerPreferred,
                                TaskManagerOptions.TaskManagerLoadBalanceMode.NONE),
                        capturedAllocations);

        scheduler =
                new AdaptiveSchedulerBuilder(
                                jobGraph,
                                singleThreadMainThreadExecutor,
                                EXECUTOR_RESOURCE.getExecutor())
                        .setDeclarativeSlotPool(slotPool)
                        .setSlotAllocator(slotAllocator)
                        .setStateTransitionManagerFactory(
                                createAutoAdvanceStateTransitionManagerFactory())
                        .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0L))
                        .build();

        // Start scheduler
        startTestInstanceInMainThread();

        // Transition job and all subtasks to RUNNING state.
        waitForJobStatusRunning(scheduler);
        runInMainThread(() -> setAllExecutionsToRunning(scheduler));

        // Trigger a checkpoint
        CompletableFuture<CompletedCheckpoint> completedCheckpointFuture =
                supplyInMainThread(() -> scheduler.triggerCheckpoint(CheckpointType.FULL));

        // Verify that checkpoint was registered by scheduler. Required to prevent race condition
        // when checkpoint is acknowledged before start.
        waitForCheckpointInProgress(scheduler);

        // Acknowledge the checkpoint for all tasks with the fake state.
        final Map<OperatorID, OperatorSubtaskState> operatorStates =
                generateFakeKeyedManagedStateForAllOperators(jobGraph);
        runInMainThread(() -> acknowledgePendingCheckpoint(scheduler, 1, operatorStates));

        // Wait for the checkpoint to complete.
        final CompletedCheckpoint completedCheckpoint = completedCheckpointFuture.join();

        // completedCheckpointStore.getLatestCheckpoint() can return null if called immediately
        // after the checkpoint is completed.
        waitForCompletedCheckpoint(scheduler);

        // Fail early if the checkpoint is null.
        assertThat(completedCheckpoint).withFailMessage("Checkpoint shouldn't be null").isNotNull();

        // Emulating new graph creation call on job recovery to ensure that the state is considered
        // for new allocations.
        final List<ExecutionAttemptID> executionAttemptIds =
                supplyInMainThread(this::getExecutionAttemptIDS);

        assertThat(executionAttemptIds).hasSize(PARALLELISM);

        runInMainThread(
                () -> {
                    // fail one of the vertices
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    executionAttemptIds.get(0),
                                    ExecutionState.FAILED,
                                    new Exception("Test exception for local recovery")));
                });

        runInMainThread(
                () -> {
                    // cancel remaining vertices
                    for (int idx = 1; idx < executionAttemptIds.size(); idx++) {
                        scheduler.updateTaskExecutionState(
                                new TaskExecutionState(
                                        executionAttemptIds.get(idx), ExecutionState.CANCELED));
                    }
                });

        waitForJobStatusRunning(scheduler);

        // First allocation during the job start + second allocation after job restart.
        assertThat(capturedAllocations).hasSize(2);
        // Fist allocation won't use state data.
        assertTrue(capturedAllocations.get(0).isEmpty());
        // Second allocation should use data from latest checkpoint.
        assertThat(
                        capturedAllocations
                                .get(1)
                                .getAllocations(JOB_VERTEX.getID())
                                .get(0)
                                .stateSizeInBytes)
                .isGreaterThan(0);
    }

    private List<ExecutionAttemptID> getExecutionAttemptIDS() {
        final Optional<ExecutionGraph> maybeExecutionGraph =
                scheduler
                        .getState()
                        .as(StateWithExecutionGraph.class)
                        .map(StateWithExecutionGraph::getExecutionGraph);
        assertThat(maybeExecutionGraph).isNotEmpty();
        final ExecutionVertex[] taskVertices =
                Objects.requireNonNull(maybeExecutionGraph.get().getJobVertex(JOB_VERTEX.getID()))
                        .getTaskVertices();
        return Arrays.stream(taskVertices)
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .map(Execution::getAttemptId)
                .collect(Collectors.toList());
    }

    private static AdaptiveScheduler.StateTransitionManagerFactory
            createAutoAdvanceStateTransitionManagerFactory() {
        return (context,
                ignoredClock,
                ignoredCooldown,
                ignoredResourceStabilizationTimeout,
                ignoredMaxTriggerDelay) ->
                TestingStateTransitionManager.withOnTriggerEventOnly(
                        () -> {
                            if (context instanceof WaitingForResources) {
                                context.transitionToSubsequentState();
                            }
                        });
    }

    private static Map<OperatorID, OperatorSubtaskState>
            generateFakeKeyedManagedStateForAllOperators(final JobGraph jobGraph)
                    throws IOException {
        final Map<OperatorID, OperatorSubtaskState> operatorStates = new HashMap<>();
        for (final JobVertex jobVertex : jobGraph.getVertices()) {
            final KeyedStateHandle keyedStateHandle =
                    generateKeyGroupState(
                            jobVertex.getID(),
                            KeyGroupRange.of(0, jobGraph.getMaximumParallelism() - 1),
                            false);
            for (OperatorIDPair operatorId : jobVertex.getOperatorIDs()) {
                operatorStates.put(
                        operatorId.getGeneratedOperatorID(),
                        OperatorSubtaskState.builder()
                                .setManagedKeyedState(keyedStateHandle)
                                .build());
            }
        }
        return operatorStates;
    }
}
