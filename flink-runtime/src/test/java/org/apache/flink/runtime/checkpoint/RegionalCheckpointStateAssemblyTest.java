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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
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
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the regional checkpoint state assembly logic in {@link CheckpointCoordinator},
 * specifically the {@code tryCompleteRegionalCheckpoint} method.
 */
class RegionalCheckpointStateAssemblyTest {

    private static final ScheduledExecutorService EXECUTOR_SERVICE =
            Executors.newSingleThreadScheduledExecutor();

    private ManuallyTriggeredScheduledExecutor manuallyTriggered;

    @BeforeEach
    void setUp() {
        manuallyTriggered = new ManuallyTriggeredScheduledExecutor();
    }

    /**
     * When the job has only a single pipelined region (ALL_TO_ALL pipelined connectivity), regional
     * checkpoint should abort because there's no isolation benefit.
     */
    @Test
    void testSingleRegionJobAborts() throws Exception {
        JobVertexID sourceId = new JobVertexID();
        JobVertexID sinkId = new JobVertexID();

        // source -> sink connected by PIPELINED edge = single region
        ExecutionGraph graph =
                new CheckpointExecutionGraphBuilder()
                        .addJobVertex(sourceId, true)
                        .addJobVertex(sinkId, false)
                        .build(EXECUTOR_SERVICE);

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.5)
                        .setRegionalMaxConsecutiveFailures(3)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        // Trigger checkpoint
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        ExecutionVertex sourceVertex = graph.getJobVertex(sourceId).getTaskVertices()[0];
        ExecutionVertex sinkVertex = graph.getJobVertex(sinkId).getTaskVertices()[0];

        // Source acknowledges
        coordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        sourceVertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId),
                "test");

        // Sink declines - triggers regional checkpoint evaluation
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        sinkVertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                "test");

        // Single region → should be aborted
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();

        coordinator.shutdown();
    }

    /** When consecutive regional checkpoint limit is exceeded, the checkpoint should be aborted. */
    @Test
    void testConsecutiveLimitExceededAborts() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(0) // zero consecutive allowed
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        // Trigger checkpoint
        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        // Source acknowledges, sink declines
        declineFromSinkAckFromSource(coordinator, graph, jobId, checkpointId);

        // Consecutive limit 0 → should be aborted
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();

        coordinator.shutdown();
    }

    /** When failure ratio exceeds the configured max, the checkpoint should be aborted. */
    @Test
    void testFailureRatioExceededAborts() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        // Max failure ratio of 0.0 means any single region failure exceeds the limit
        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.0)
                        .setRegionalMaxConsecutiveFailures(10)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        declineFromSinkAckFromSource(coordinator, graph, jobId, checkpointId);

        // Failure ratio exceeded → should be aborted
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();

        coordinator.shutdown();
    }

    /**
     * When there is no historical completed checkpoint to use as fallback, regional checkpoint
     * should abort.
     */
    @Test
    void testNoHistoricalCheckpointAborts() throws Exception {
        ExecutionGraph graph = createMultiRegionGraph();
        JobID jobId = graph.getJobID();

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.9)
                        .setRegionalMaxConsecutiveFailures(10)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        // Empty completed store → no historical checkpoint
        CompletedCheckpointStore completedStore = new StandaloneCompletedCheckpointStore(5);

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setCompletedCheckpointStore(completedStore)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        declineFromSinkAckFromSource(coordinator, graph, jobId, checkpointId);

        // No historical checkpoint → should be aborted
        assertThat(coordinator.getPendingCheckpoints()).isEmpty();

        coordinator.shutdown();
    }

    /** When regional checkpoint is disabled, a decline immediately aborts. */
    @Test
    void testRegionalCheckpointDisabledAborts() throws Exception {
        JobVertexID sourceId = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointExecutionGraphBuilder()
                        .addJobVertex(sourceId, true)
                        .build(EXECUTOR_SERVICE);

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(false)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        ExecutionVertex vertex = graph.getJobVertex(sourceId).getTaskVertices()[0];

        // Decline immediately aborts when regional checkpoint is disabled
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        vertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                "test");

        assertThat(coordinator.getPendingCheckpoints()).isEmpty();

        coordinator.shutdown();
    }

    /**
     * Verifies that when regional checkpoint is enabled and a task declines, the checkpoint is not
     * immediately aborted — it waits for all tasks to respond.
     */
    @Test
    void testDeclineBufferedUntilAllRespond() throws Exception {
        JobVertexID sourceId = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointExecutionGraphBuilder()
                        .addJobVertex(sourceId, 2, 128)
                        .build(EXECUTOR_SERVICE);

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setRegionalCheckpointEnabled(true)
                        .setRegionalMaxFailureRatio(0.5)
                        .setRegionalMaxConsecutiveFailures(3)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(config)
                        .setTimer(manuallyTriggered)
                        .build(graph);

        coordinator.startCheckpointScheduler();

        coordinator.triggerCheckpoint(false);
        manuallyTriggered.triggerAll();

        assertThat(coordinator.getNumberOfPendingCheckpoints()).isEqualTo(1);
        long checkpointId = coordinator.getPendingCheckpoints().keySet().iterator().next();

        ExecutionVertex vertex0 = graph.getJobVertex(sourceId).getTaskVertices()[0];

        // First task declines — checkpoint should still be pending (waiting for second task)
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        vertex0.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                "test");

        // Checkpoint should still be pending (second task hasn't responded yet)
        assertThat(coordinator.getPendingCheckpoints()).containsKey(checkpointId);

        coordinator.shutdown();
    }

    // ---- Helper methods ----

    /**
     * Creates a multi-region execution graph with two job vertices (source and sink) connected by a
     * BLOCKING edge, resulting in two separate pipelined regions.
     */
    private ExecutionGraph createMultiRegionGraph() throws Exception {
        JobVertex source = new JobVertex("source", new JobVertexID());
        source.setParallelism(1);
        source.setMaxParallelism(128);
        source.setInvokableClass(NoOpInvokable.class);

        JobVertex sink = new JobVertex("sink", new JobVertexID());
        sink.setParallelism(1);
        sink.setMaxParallelism(128);
        sink.setInvokableClass(NoOpInvokable.class);

        // BLOCKING edge → two separate pipelined regions
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

    /** Acknowledges from "source" vertex, declines from "sink" vertex. */
    private void declineFromSinkAckFromSource(
            CheckpointCoordinator coordinator, ExecutionGraph graph, JobID jobId, long checkpointId)
            throws Exception {
        for (ExecutionJobVertex jv : graph.getAllVertices().values()) {
            for (ExecutionVertex ev : jv.getTaskVertices()) {
                ExecutionAttemptID attemptId = ev.getCurrentExecutionAttempt().getAttemptId();
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
}
