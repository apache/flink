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

import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tracker for checkpoint statistics.
 *
 * <p>This is tightly integrated with the {@link CheckpointCoordinator} in order to ease the
 * gathering of fine-grained statistics.
 *
 * <p>The tracked stats include summary counts, a detailed history of recent and in progress
 * checkpoints as well as summaries about the size, duration and more of recent checkpoints.
 *
 * <p>Data is gathered via callbacks in the {@link CheckpointCoordinator} and related classes like
 * {@link PendingCheckpoint} and {@link CompletedCheckpoint}, which receive the raw stats data in
 * the first place.
 *
 * <p>The statistics are accessed via {@link #createSnapshot()} and exposed via both the web
 * frontend and the {@link Metric} system.
 */
public interface CheckpointStatsTracker {

    /**
     * Callback when a checkpoint is restored.
     *
     * @param restored The restored checkpoint stats.
     */
    @Deprecated
    default void reportRestoredCheckpoint(RestoredCheckpointStats restored) {
        checkNotNull(restored, "Restored checkpoint");
        reportRestoredCheckpoint(
                restored.getCheckpointId(),
                restored.getProperties(),
                restored.getExternalPath(),
                restored.getStateSize());
    }

    void reportRestoredCheckpoint(
            long checkpointID,
            CheckpointProperties properties,
            String externalPath,
            long stateSize);

    /**
     * Callback when a checkpoint completes.
     *
     * @param completed The completed checkpoint stats.
     */
    void reportCompletedCheckpoint(CompletedCheckpointStats completed);

    @Nullable
    PendingCheckpointStats getPendingCheckpointStats(long checkpointId);

    void reportIncompleteStats(
            long checkpointId, ExecutionAttemptID attemptId, CheckpointMetrics metrics);

    void reportInitializationStarted(
            Set<ExecutionAttemptID> toInitialize, long initializationStartTs);

    void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics);

    /**
     * Creates a new pending checkpoint tracker.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Trigger timestamp of the checkpoint.
     * @param props The checkpoint properties.
     * @param vertexToDop mapping of {@link JobVertexID} to DOP
     * @return Tracker for statistics gathering or {@code null} if no stats were tracked.
     */
    @Nullable
    PendingCheckpointStats reportPendingCheckpoint(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            Map<JobVertexID, Integer> vertexToDop);

    void reportFailedCheckpoint(FailedCheckpointStats failed);

    /**
     * Callback when a checkpoint failure without in progress checkpoint. For example, it should be
     * callback when triggering checkpoint failure before creating PendingCheckpoint.
     */
    void reportFailedCheckpointsWithoutInProgress();

    /**
     * Creates a new snapshot of the available stats.
     *
     * @return The latest statistics snapshot.
     */
    CheckpointStatsSnapshot createSnapshot();
}
