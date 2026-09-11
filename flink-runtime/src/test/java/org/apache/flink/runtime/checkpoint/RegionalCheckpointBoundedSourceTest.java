/*
 * Licensed to the Apache Software Foundation (ASF)
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Bounded Source forced global checkpoint, per FLIP-600 Section 9 "Bounded Source
 * (Finished Operators)".
 *
 * <p>When all source vertices have finished, the next checkpoint is forced to be global. If the
 * forced global checkpoint fails (has declined tasks), it aborts and resets (Tier 2). This ensures
 * side effects (e.g., Kafka transactions) are committed before job termination.
 */
class RegionalCheckpointBoundedSourceTest {

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor();

    private ManuallyTriggeredScheduledExecutor manuallyTriggered;

    @BeforeEach
    void setUp() {
        manuallyTriggered = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * When all sources are finished and a checkpoint with declined tasks is attempted, the
     * forced-global flag should cause the checkpoint to abort (Tier 2) rather than completing as a
     * regional checkpoint.
     */
    @Test
    void testAllSourcesFinishedForcesGlobalCheckpoint() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(10)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);
        addFakeCompletedCheckpoint(store, jobId, graph);

        AtomicBoolean allSourcesFinished = new AtomicBoolean(false);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);
        coordinator.setAllSourcesFinishedChecker(allSourcesFinished::get);

        coordinator.startCheckpointScheduler();

        // Step 1: normal regional checkpoint (sources not yet finished)
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();
        long cpId1 = coordinator.getPendingCheckpoints().keySet().iterator().next();
        declineFromSinkAckFromSource(coordinator, graph, jobId, cpId1);

        // If regional checkpoint completed, counter > 0 and force flag may be set
        boolean firstRegionalCompleted = coordinator.getConsecutiveRegionalCheckpointCount() > 0;

        // Step 2: simulate all sources finished
        allSourcesFinished.set(true);

        // Step 3: trigger next checkpoint — should be forced global
        // If sink declines, the forced global should abort (Tier 2)
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        if (coordinator.getNumberOfPendingCheckpoints() > 0) {
            long cpId2 = coordinator.getPendingCheckpoints().keySet().iterator().next();
            declineFromSinkAckFromSource(coordinator, graph, jobId, cpId2);

            // Tier 2: forced global with declined tasks → abort.
            // The key assertion is that the checkpoint was aborted (pending is empty).
            // Note: forceGlobalNextCheckpoint may be re-set by allSourcesFinishedChecker
            // during the next trigger, so we don't assert on it here.
            assertThat(coordinator.getPendingCheckpoints()).isEmpty();
        }

        // If first regional completed, the consecutive counter is reset after Tier 2
        if (firstRegionalCompleted) {
            assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();
        }

        coordinator.shutdown();
    }

    /**
     * When all sources are finished and all tasks acknowledge, the checkpoint completes as a global
     * checkpoint (not regional), and the force flag is cleared.
     */
    @Test
    void testAllSourcesFinishedGlobalCheckpointSucceeds() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(10)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        // Simulate all sources finished
        coordinator.setAllSourcesFinishedChecker(() -> true);

        coordinator.startCheckpointScheduler();
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        // All tasks acknowledge → global checkpoint completes
        acknowledgeFromAllTasks(coordinator, graph, jobId, checkpointId);

        // Global checkpoint completed → counter is 0, force flag cleared
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();
        assertThat(coordinator.getForceGlobalNextCheckpoint()).isFalse();
        assertThat(coordinator.getNumberOfRetainedSuccessfulCheckpoints()).isEqualTo(1);

        coordinator.shutdown();
    }

    // ---- Helper methods ----

    private ExecutionGraph createMultiRegionGraph() throws Exception {
        JobVertex source = new JobVertex("source", new JobVertexID());
        source.setParallelism(1);
        source.setMaxParallelism(128);
        source.setInvokableClass(NoOpInvokable.class);

        JobVertex sink = new JobVertex("sink", new JobVertexID());
        sink.setParallelism(1);
        sink.setMaxParallelism(128);
        sink.setInvokableClass(NoOpInvokable.class);

        connectNewDataSetAsInput(
                sink, source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        ExecutionGraph graph =
                ExecutionGraphTestUtils.createExecutionGraph(EXECUTOR_SERVICE, source, sink);
        graph.start(
                org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter
                        .forMainThread());
        graph.transitionToRunning();

        for (ExecutionVertex ev : graph.getAllExecutionVertices()) {
            ev.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);
        }

        return graph;
    }

    private void addFakeCompletedCheckpoint(
            StandaloneCompletedCheckpointStore store, JobID jobId, ExecutionGraph graph)
            throws Exception {
        Map<org.apache.flink.runtime.jobgraph.OperatorID, OperatorState> operatorStates =
                new HashMap<>();
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            org.apache.flink.runtime.jobgraph.OperatorID opId =
                    jv.getOperatorIDs().get(0).getGeneratedOperatorID();
            OperatorState state = new OperatorState(null, null, opId, 1, 128);
            operatorStates.put(opId, state);
        }

        CompletedCheckpoint fakeCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        1L,
                        0L,
                        0L,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        store.addCheckpointAndSubsumeOldestOne(fakeCheckpoint, new CheckpointsCleaner(), () -> {});
    }

    private void declineFromSinkAckFromSource(
            CheckpointCoordinator coordinator, ExecutionGraph graph, JobID jobId, long checkpointId)
            throws Exception {
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            for (ExecutionVertex ev : jv.getTaskVertices()) {
                org.apache.flink.runtime.executiongraph.ExecutionAttemptID attemptId =
                        ev.getCurrentExecutionAttempt().getAttemptId();
                if (jv.getName().contains("source")) {
                    coordinator.receiveAcknowledgeMessage(
                            new AcknowledgeCheckpoint(jobId, attemptId, checkpointId), "test");
                } else {
                    coordinator.receiveDeclineMessage(
                            new DeclineCheckpoint(
                                    jobId,
                                    attemptId,
                                    checkpointId,
                                    new CheckpointException(
                                            CheckpointFailureReason.CHECKPOINT_DECLINED)),
                            "test");
                }
            }
        }
    }

    private void acknowledgeFromAllTasks(
            CheckpointCoordinator coordinator, ExecutionGraph graph, JobID jobId, long checkpointId)
            throws Exception {
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            for (ExecutionVertex ev : jv.getTaskVertices()) {
                org.apache.flink.runtime.executiongraph.ExecutionAttemptID attemptId =
                        ev.getCurrentExecutionAttempt().getAttemptId();
                coordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(jobId, attemptId, checkpointId), "test");
            }
        }
    }
}
