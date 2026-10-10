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

import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that drive the regional checkpoint <b>success</b> path of {@link CheckpointCoordinator} —
 * the part that actually computes pipeline regions, merges healthy/failed subtask state, marks the
 * fallback state with a reference checkpoint id and finalizes the regional checkpoint.
 *
 * <p>This complements {@code RegionalCheckpointStateAssemblyTest}, which only exercises the abort
 * branches. The crucial difference is that these tests install a {@code regionIdProvider} (via
 * {@link CheckpointCoordinator#setRegionIdProvider}) and acknowledge with real {@link
 * OperatorSubtaskState}, so the state-assembly and completion logic is genuinely executed rather
 * than short-circuited at the {@code regionIdProvider == null} guard.
 */
class RegionalCheckpointSuccessPathTest {

    private ManuallyTriggeredScheduledExecutor manuallyTriggered;

    @BeforeEach
    void setUp() {
        manuallyTriggered = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * A healthy region keeps the freshly acknowledged state; the failed region's subtask state is
     * taken from the historical checkpoint and tagged with the fallback checkpoint id.
     */
    @Test
    void testRegionalCheckpointCompletesWithStateMerge() throws Exception {
        final TwoRegionGraph g = new TwoRegionGraph();
        final long fallbackCheckpointId = 1L;
        addHistoricalCheckpoint(g, fallbackCheckpointId);

        final CheckpointCoordinator coordinator =
                buildCoordinator(g, regionalConfig(0.9, 10), g.store);
        coordinator.setRegionIdProvider(g.regionProvider());
        coordinator.startCheckpointScheduler();

        final long checkpointId = triggerPending(coordinator);

        // Healthy region (source) acknowledges with fresh state; failed region (sink) declines.
        ackWithState(coordinator, g, g.sourceVertex(), checkpointId);
        decline(coordinator, g, g.sinkVertex(), checkpointId);

        // The regional checkpoint must have completed (not aborted).
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();
        final CompletedCheckpoint completed = findCompleted(coordinator, checkpointId);
        assertThat(completed).isNotNull();

        // Healthy region: state present, NOT a reference to a historical checkpoint.
        final OperatorSubtaskState healthy =
                completed.getOperatorStates().get(g.sourceOperatorId).getState(0);
        assertThat(healthy).isNotNull();
        assertThat(healthy.getRefCheckpointId()).isNotPresent();

        // Failed region: state taken from history and marked with the fallback checkpoint id.
        final OperatorSubtaskState fellBack =
                completed.getOperatorStates().get(g.sinkOperatorId).getState(0);
        assertThat(fellBack).isNotNull();
        assertThat(fellBack.getRefCheckpointId()).hasValue(fallbackCheckpointId);

        // The consecutive regional checkpoint counter was incremented exactly once.
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isEqualTo(1);

        coordinator.shutdown();
    }

    /**
     * When the failed region's operator has no subtask state in the historical checkpoint, no
     * fallback state is grafted in (the operator simply has no entry for that subtask), but the
     * checkpoint still completes for the healthy region.
     */
    @Test
    void testFailedRegionWithoutHistoricalSubtaskStateStillCompletes() throws Exception {
        final TwoRegionGraph g = new TwoRegionGraph();
        // Historical checkpoint that contains the sink operator but with NO subtask state.
        addHistoricalCheckpoint(g, 1L, /* includeSinkSubtaskState= */ false);

        final CheckpointCoordinator coordinator =
                buildCoordinator(g, regionalConfig(0.9, 10), g.store);
        coordinator.setRegionIdProvider(g.regionProvider());
        coordinator.startCheckpointScheduler();

        final long checkpointId = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), checkpointId);
        decline(coordinator, g, g.sinkVertex(), checkpointId);

        assertThat(coordinator.getPendingCheckpoints()).isEmpty();
        final CompletedCheckpoint completed = findCompleted(coordinator, checkpointId);
        assertThat(completed).isNotNull();
        // Healthy region state survived.
        assertThat(completed.getOperatorStates().get(g.sourceOperatorId).getState(0)).isNotNull();

        coordinator.shutdown();
    }

    /**
     * The consecutive regional checkpoint counter increments per successful regional checkpoint and
     * resets to zero once a full global checkpoint (all tasks acknowledge) completes.
     */
    @Test
    void testConsecutiveCounterIncrementsThenResetsOnGlobalCheckpoint() throws Exception {
        final TwoRegionGraph g = new TwoRegionGraph();
        addHistoricalCheckpoint(g, 1L);

        final CheckpointCoordinator coordinator =
                buildCoordinator(g, regionalConfig(0.9, 10), g.store);
        coordinator.setRegionIdProvider(g.regionProvider());
        coordinator.startCheckpointScheduler();

        // First regional checkpoint → counter 1.
        long cp1 = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), cp1);
        decline(coordinator, g, g.sinkVertex(), cp1);
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isEqualTo(1);

        // Second regional checkpoint → counter 2.
        long cp2 = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), cp2);
        decline(coordinator, g, g.sinkVertex(), cp2);
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isEqualTo(2);

        // A full global checkpoint resets the counter to 0.
        long cp3 = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), cp3);
        ackWithState(coordinator, g, g.sinkVertex(), cp3);
        assertThat(coordinator.getConsecutiveRegionalCheckpointCount()).isZero();

        coordinator.shutdown();
    }

    /**
     * When the failed region contains an operator coordinator that supports regional checkpoint,
     * its region-fallback state (the bytes returned via the result future) is written into the
     * resulting OperatorState and the coordinator is notified of the regional completion.
     */
    @Test
    void testCoordinatorFallbackStateWrittenAndNotified() throws Exception {
        final TwoRegionGraph g = new TwoRegionGraph();
        addHistoricalCheckpoint(g, 1L);

        final byte[] fallbackBytes = new byte[] {7, 7, 7};
        final CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext coordCtx =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOperatorID(g.sinkOperatorId)
                        .setSupportsRegionCheckpoint(true)
                        .setOnCallingCheckpointCoordinatorForRegionFallback(
                                (cpId, fallbackId, subtasks, future) ->
                                        future.complete(fallbackBytes))
                        .build();

        final CheckpointCoordinator coordinator =
                buildCoordinator(g, regionalConfig(0.9, 10), g.store, coordCtx);
        coordinator.setRegionIdProvider(g.regionProvider());
        coordinator.startCheckpointScheduler();

        final long checkpointId = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), checkpointId);
        decline(coordinator, g, g.sinkVertex(), checkpointId);

        final CompletedCheckpoint completed = findCompleted(coordinator, checkpointId);
        assertThat(completed).isNotNull();

        // The coordinator fallback bytes were written into the sink operator's coordinator state.
        final ByteStreamStateHandle coordinatorState =
                (ByteStreamStateHandle)
                        completed.getOperatorStates().get(g.sinkOperatorId).getCoordinatorState();
        assertThat(coordinatorState).isNotNull();
        assertThat(coordinatorState.getData()).isEqualTo(fallbackBytes);

        // The coordinator was notified via the regional fallback path (it's in the failed region).
        assertThat(coordCtx.getRegionalFallbackCheckpoints()).contains(checkpointId);
        // Healthy-region coordinators would receive notifyRegionalCheckpointComplete; the failed
        // region's coordinator receives notifyRegionalCheckpointFallback instead.
        assertThat(coordCtx.getRegionalCompletedCheckpoints()).doesNotContain(checkpointId);

        coordinator.shutdown();
    }

    /**
     * If the failed region's coordinator does not support regional checkpoint, the regional
     * checkpoint must abort rather than silently proceed.
     */
    @Test
    void testUnsupportedCoordinatorAbortsRegionalCheckpoint() throws Exception {
        final TwoRegionGraph g = new TwoRegionGraph();
        addHistoricalCheckpoint(g, 1L);

        final CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext coordCtx =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOperatorID(g.sinkOperatorId)
                        .setSupportsRegionCheckpoint(false)
                        .setOnCallingCheckpointCoordinator(
                                (cpId, future) -> future.complete(new byte[0]))
                        .build();

        final CheckpointCoordinator coordinator =
                buildCoordinator(g, regionalConfig(0.9, 10), g.store, coordCtx);
        coordinator.setRegionIdProvider(g.regionProvider());
        coordinator.startCheckpointScheduler();

        final long checkpointId = triggerPending(coordinator);
        ackWithState(coordinator, g, g.sourceVertex(), checkpointId);
        decline(coordinator, g, g.sinkVertex(), checkpointId);

        // Coordinator in the failed region doesn't support regional checkpoint → abort.
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();
        assertThat(findCompleted(coordinator, checkpointId)).isNull();

        coordinator.shutdown();
    }

    // ------------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------------

    private static CheckpointCoordinatorConfiguration regionalConfig(
            double maxFailureRatio, int maxConsecutive) {
        return new CheckpointCoordinatorConfigurationBuilder()
                .setRegionalCheckpointEnabled(true)
                .setRegionalMaxFailureRatio(maxFailureRatio)
                .setRegionalMaxConsecutiveFailures(maxConsecutive)
                .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                .build();
    }

    private CheckpointCoordinator buildCoordinator(
            TwoRegionGraph g,
            CheckpointCoordinatorConfiguration config,
            CompletedCheckpointStore store)
            throws Exception {
        return buildCoordinator(g, config, store, null);
    }

    private CheckpointCoordinator buildCoordinator(
            TwoRegionGraph g,
            CheckpointCoordinatorConfiguration config,
            CompletedCheckpointStore store,
            CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext coordCtx)
            throws Exception {
        // Start triggered checkpoint ids well above the historical fallback id to avoid collisions.
        final StandaloneCheckpointIDCounter idCounter = new StandaloneCheckpointIDCounter();
        idCounter.setCount(100L);
        final CheckpointCoordinatorBuilder builder =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(store)
                        .setCheckpointIDCounter(idCounter)
                        .setTimer(manuallyTriggered);
        if (coordCtx != null) {
            builder.setCoordinatorsToCheckpoint(java.util.Collections.singleton(coordCtx));
        }
        return builder.build(g.graph);
    }

    private static CompletedCheckpoint findCompleted(
            CheckpointCoordinator coordinator, long checkpointId) throws Exception {
        return coordinator.getSuccessfulCheckpoints().stream()
                .filter(cp -> cp.getCheckpointID() == checkpointId)
                .findFirst()
                .orElse(null);
    }

    private long triggerPending(CheckpointCoordinator coordinator) {
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();
        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        return coordinator.getPendingCheckpoints().keySet().iterator().next();
    }

    private static void ackWithState(
            CheckpointCoordinator coordinator,
            TwoRegionGraph g,
            ExecutionVertex vertex,
            long checkpointId)
            throws Exception {
        final OperatorID opId =
                vertex.getJobVertex()
                        .getJobVertex()
                        .getOperatorIDs()
                        .get(0)
                        .getGeneratedOperatorID();
        final TaskStateSnapshot snapshot = new TaskStateSnapshot();
        snapshot.putSubtaskStateByOperatorID(
                opId,
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                new org.apache.flink.runtime.state.OperatorStreamStateHandle(
                                        Collections.emptyMap(),
                                        new ByteStreamStateHandle(
                                                "fresh-" + opId, new byte[] {1, 2, 3})))
                        .build());
        coordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        g.graph.getJobID(),
                        vertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        snapshot),
                "test");
    }

    private static void decline(
            CheckpointCoordinator coordinator,
            TwoRegionGraph g,
            ExecutionVertex vertex,
            long checkpointId) {
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        g.graph.getJobID(),
                        vertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                "test");
    }

    private void addHistoricalCheckpoint(TwoRegionGraph g, long checkpointId) throws Exception {
        addHistoricalCheckpoint(g, checkpointId, true);
    }

    /**
     * Seeds the completed checkpoint store with a fallback checkpoint containing operator state.
     */
    private void addHistoricalCheckpoint(
            TwoRegionGraph g, long checkpointId, boolean includeSinkSubtaskState) throws Exception {
        final Map<OperatorID, OperatorState> operatorStates = new HashMap<>();

        final OperatorState sourceState = new OperatorState(null, null, g.sourceOperatorId, 1, 128);
        sourceState.putState(0, historicalSubtaskState("hist-source"));
        operatorStates.put(g.sourceOperatorId, sourceState);

        final OperatorState sinkState = new OperatorState(null, null, g.sinkOperatorId, 1, 128);
        if (includeSinkSubtaskState) {
            sinkState.putState(0, historicalSubtaskState("hist-sink"));
        }
        operatorStates.put(g.sinkOperatorId, sinkState);

        final CompletedCheckpoint historical =
                new CompletedCheckpoint(
                        g.graph.getJobID(),
                        checkpointId,
                        0L,
                        0L,
                        operatorStates,
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null);

        g.store.addCheckpointAndSubsumeOldestOne(historical, new CheckpointsCleaner(), () -> {});
    }

    private static OperatorSubtaskState historicalSubtaskState(String name) {
        return OperatorSubtaskState.builder()
                .setManagedOperatorState(
                        new org.apache.flink.runtime.state.OperatorStreamStateHandle(
                                Collections.emptyMap(),
                                new ByteStreamStateHandle(name, new byte[] {9, 9, 9})))
                .build();
    }

    /**
     * A two-region execution graph: source and sink connected by a BLOCKING ALL_TO_ALL edge, which
     * places them in two separate pipelined regions. The region provider maps each vertex to its
     * owning {@link JobVertexID}, so source and sink belong to distinct regions.
     */
    private static final class TwoRegionGraph {
        private static final java.util.concurrent.ScheduledExecutorService EXECUTOR =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        final ExecutionGraph graph;
        final JobVertexID sourceJvId;
        final JobVertexID sinkJvId;
        final OperatorID sourceOperatorId;
        final OperatorID sinkOperatorId;
        final CompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);

        TwoRegionGraph() throws Exception {
            final JobVertex source = new JobVertex("source", new JobVertexID());
            source.setParallelism(1);
            source.setMaxParallelism(128);
            source.setInvokableClass(NoOpInvokable.class);

            final JobVertex sink = new JobVertex("sink", new JobVertexID());
            sink.setParallelism(1);
            sink.setMaxParallelism(128);
            sink.setInvokableClass(NoOpInvokable.class);

            connectNewDataSetAsInput(
                    sink, source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

            graph = ExecutionGraphTestUtils.createExecutionGraph(EXECUTOR, source, sink);
            graph.start(
                    org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter
                            .forMainThread());
            graph.transitionToRunning();
            for (ExecutionVertex ev : graph.getAllExecutionVertices()) {
                ev.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);
            }

            sourceJvId = source.getID();
            sinkJvId = sink.getID();
            sourceOperatorId = operatorIdOf(sourceJvId);
            sinkOperatorId = operatorIdOf(sinkJvId);
        }

        private OperatorID operatorIdOf(JobVertexID jvId) {
            return graph.getJobVertex(jvId).getOperatorIDs().get(0).getGeneratedOperatorID();
        }

        ExecutionVertex sourceVertex() {
            return graph.getJobVertex(sourceJvId).getTaskVertices()[0];
        }

        ExecutionVertex sinkVertex() {
            return graph.getJobVertex(sinkJvId).getTaskVertices()[0];
        }

        /**
         * Maps each execution vertex to its owning JobVertexID, giving one region per job vertex.
         */
        Function<org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID, Object>
                regionProvider() {
            return id -> id.getJobVertexId();
        }
    }
}
