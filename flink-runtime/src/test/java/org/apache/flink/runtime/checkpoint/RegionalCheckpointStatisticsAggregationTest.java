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

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatistics;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the {@code oldest_ref_checkpoint_id} REST field is aggregated correctly from the
 * per-subtask reference ids up through {@link TaskStateStats} (cross-subtask min) and {@link
 * CheckpointStatistics} (cross-task min).
 *
 * <p>This drives the real aggregation code in {@link
 * CheckpointStatistics#generateCheckpointStatistics} that the regional checkpoint REST exposure
 * relies on, rather than only checking the serialization of a manually-set field.
 */
class RegionalCheckpointStatisticsAggregationTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            new TestExecutorExtension<>(
                    java.util.concurrent.Executors::newSingleThreadScheduledExecutor);

    @Test
    void testCrossSubtaskAndCrossTaskMinAggregation() throws Exception {
        final JobVertexID vertexA = new JobVertexID();
        final JobVertexID vertexB = new JobVertexID();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(vertexA, 2, 256)
                        .addJobVertex(vertexB, 2, 256, java.util.Collections.emptyList(), false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final Map<JobVertexID, Integer> tasksPerVertex = new LinkedHashMap<>();
        tasksPerVertex.put(vertexA, graph.getJobVertex(vertexA).getParallelism());
        tasksPerVertex.put(vertexB, graph.getJobVertex(vertexB).getParallelism());

        final CheckpointStatsTracker tracker =
                new DefaultCheckpointStatsTracker(
                        10, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        final PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        42L,
                        1L,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        tasksPerVertex);

        // Vertex A: subtask 0 has no ref (current state), subtask 1 fell back to checkpoint 30.
        pending.reportSubtaskStats(vertexA, subtaskStats(0, null));
        pending.reportSubtaskStats(vertexA, subtaskStats(1, 30L));

        // Vertex B: subtask 0 fell back to checkpoint 20, subtask 1 to checkpoint 25.
        pending.reportSubtaskStats(vertexB, subtaskStats(0, 20L));
        pending.reportSubtaskStats(vertexB, subtaskStats(1, 25L));

        final CompletedCheckpointStats completed = pending.toCompletedCheckpointStats(null, 1984);

        final CheckpointStatistics statistics =
                CheckpointStatistics.generateCheckpointStatistics(completed, true);

        final Map<JobVertexID, TaskCheckpointStatistics> perTask =
                statistics.getCheckpointStatisticsPerTask();

        // Per-task min: vertex A = min(absent, 30) = 30; vertex B = min(20, 25) = 20.
        assertThat(perTask.get(vertexA).getOldestRefCheckpointId()).isEqualTo(30L);
        assertThat(perTask.get(vertexB).getOldestRefCheckpointId()).isEqualTo(20L);

        // Checkpoint-level min across tasks: min(30, 20) = 20.
        assertThat(statistics)
                .isInstanceOf(CheckpointStatistics.CompletedCheckpointStatistics.class);
        assertThat(statistics.getOldestRefCheckpointId()).isEqualTo(20L);
    }

    @Test
    void testNoReferencesYieldsNullAggregate() throws Exception {
        final JobVertexID vertexA = new JobVertexID();
        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(vertexA, 2, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final CheckpointStatsTracker tracker =
                new DefaultCheckpointStatsTracker(
                        10, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        final PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        7L,
                        1L,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        java.util.Collections.singletonMap(
                                vertexA, graph.getJobVertex(vertexA).getParallelism()));

        pending.reportSubtaskStats(vertexA, subtaskStats(0, null));
        pending.reportSubtaskStats(vertexA, subtaskStats(1, null));

        final CheckpointStatistics statistics =
                CheckpointStatistics.generateCheckpointStatistics(
                        pending.toCompletedCheckpointStats(null, 1984), true);

        assertThat(
                        statistics
                                .getCheckpointStatisticsPerTask()
                                .get(vertexA)
                                .getOldestRefCheckpointId())
                .isNull();
        assertThat(statistics.getOldestRefCheckpointId()).isNull();
    }

    private static SubtaskStateStats subtaskStats(int index, Long refCheckpointId) {
        return new SubtaskStateStats(
                index, 1L, 0, 0, 0, 0, 0, 0, 0, 0, false, true, refCheckpointId);
    }
}
