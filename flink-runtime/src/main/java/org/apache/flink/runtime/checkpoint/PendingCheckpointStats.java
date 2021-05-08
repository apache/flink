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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toConcurrentMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Statistics for a pending checkpoint that is still in progress.
 *
 * <p>This is the starting point for all checkpoint tracking. The life cycle of instances of this
 * class is tightly coupled to a {@link PendingCheckpoint} instance, which forwards statistics about
 * acknowledged subtasks via {@link #reportSubtaskStats(JobVertexID, SubtaskStateStats)}.
 *
 * <p>Depending on whether the {@link PendingCheckpoint} is finalized successfully or aborted, we
 * replace ourselves with a {@link CompletedCheckpointStats} or {@link FailedCheckpointStats} and
 * notify the {@link CheckpointStatsTracker}.
 *
 * <p>The statistics gathered here are all live updated.
 */
public class PendingCheckpointStats extends AbstractCheckpointStats {

    private static final long serialVersionUID = -973959257699390327L;

    /** Tracker callback when the pending checkpoint is finalized or aborted. */
    private final transient CheckpointStatsTracker.PendingCheckpointStatsCallback trackerCallback;

    /** The current number of acknowledged subtasks. */
    private volatile int currentNumAcknowledgedSubtasks;

    /** Current checkpoint state size over all collected subtasks. */
    private volatile long currentStateSize;

    private volatile long currentProcessedData;

    private volatile long currentPersistedData;

    /** Stats of the latest acknowledged subtask. */
    private volatile SubtaskStateStats latestAcknowledgedSubtask;

    /**
     * Creates a tracker for a {@link PendingCheckpoint}.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Timestamp when the checkpoint was triggered.
     * @param props Checkpoint properties of the checkpoint.
     * @param taskStats Task stats for each involved operator.
     * @param trackerCallback Callback for the {@link CheckpointStatsTracker}.
     */
    PendingCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            Map<JobVertexID, Integer> taskStats,
            CheckpointStatsTracker.PendingCheckpointStatsCallback trackerCallback) {
        this(
                checkpointId,
                triggerTimestamp,
                props,
                taskStats.values().stream().mapToInt(i -> i).sum(),
                taskStats.entrySet().stream()
                        .collect(
                                toConcurrentMap(
                                        Map.Entry::getKey,
                                        e -> new TaskStateStats(e.getKey(), e.getValue()))),
                trackerCallback);
    }

    /**
     * Creates a tracker for a {@link PendingCheckpoint}.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Timestamp when the checkpoint was triggered.
     * @param props Checkpoint properties of the checkpoint.
     * @param totalSubtaskCount Total number of subtasks for the checkpoint.
     * @param taskStats Task stats for each involved operator.
     * @param trackerCallback Callback for the {@link CheckpointStatsTracker}.
     */
    PendingCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            CheckpointStatsTracker.PendingCheckpointStatsCallback trackerCallback) {
        this(
                checkpointId,
                triggerTimestamp,
                props,
                totalSubtaskCount,
                0,
                taskStats,
                trackerCallback,
                0,
                0,
                0,
                null);
    }

    PendingCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            int acknowledgedSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            CheckpointStatsTracker.PendingCheckpointStatsCallback trackerCallback,
            long currentStateSize,
            long processedData,
            long persistedData,
            @Nullable SubtaskStateStats latestAcknowledgedSubtask) {

        super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
        this.trackerCallback = checkNotNull(trackerCallback);
        this.currentStateSize = currentStateSize;
        this.currentProcessedData = processedData;
        this.currentPersistedData = persistedData;
        this.latestAcknowledgedSubtask = latestAcknowledgedSubtask;
        this.currentNumAcknowledgedSubtasks = acknowledgedSubtaskCount;
    }

    @Override
    public CheckpointStatsStatus getStatus() {
        return CheckpointStatsStatus.IN_PROGRESS;
    }

    @Override
    public int getNumberOfAcknowledgedSubtasks() {
        return currentNumAcknowledgedSubtasks;
    }

    @Override
    public long getStateSize() {
        return currentStateSize;
    }

    @Override
    public long getProcessedData() {
        return currentProcessedData;
    }

    @Override
    public long getPersistedData() {
        return currentPersistedData;
    }

    @Override
    public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
        return latestAcknowledgedSubtask;
    }

    // ------------------------------------------------------------------------
    // Callbacks from the PendingCheckpoint instance
    // ------------------------------------------------------------------------

    /**
     * Reports statistics for a single subtask.
     *
     * @param jobVertexId ID of the task/operator the subtask belongs to.
     * @param subtask The statistics for the subtask.
     * @return <code>true</code> if successfully reported or <code>false</code> otherwise.
     */
    boolean reportSubtaskStats(JobVertexID jobVertexId, SubtaskStateStats subtask) {
        TaskStateStats taskStateStats = taskStats.get(jobVertexId);

        if (taskStateStats != null && taskStateStats.reportSubtaskStats(subtask)) {
            if (subtask.isCompleted()) {
                currentNumAcknowledgedSubtasks++;
                latestAcknowledgedSubtask = subtask;
            }

            currentStateSize += subtask.getStateSize();

            long processedData = subtask.getProcessedData();
            if (processedData > 0) {
                currentProcessedData += processedData;
            }

            long persistedData = subtask.getPersistedData();
            if (persistedData > 0) {
                currentPersistedData += persistedData;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Reports a successfully completed pending checkpoint.
     *
     * @param externalPointer Optional external storage path if checkpoint was externalized.
     * @return Callback for the {@link CompletedCheckpoint} instance to notify about disposal.
     */
    CompletedCheckpointStats.DiscardCallback reportCompletedCheckpoint(String externalPointer) {
        CompletedCheckpointStats completed =
                new CompletedCheckpointStats(
                        checkpointId,
                        triggerTimestamp,
                        props,
                        numberOfSubtasks,
                        new HashMap<>(taskStats),
                        currentNumAcknowledgedSubtasks,
                        currentStateSize,
                        currentProcessedData,
                        currentPersistedData,
                        latestAcknowledgedSubtask,
                        externalPointer);

        trackerCallback.reportCompletedCheckpoint(completed);

        return completed.getDiscardCallback();
    }

    /**
     * Reports a failed pending checkpoint.
     *
     * @param failureTimestamp Timestamp of the failure.
     * @param cause Optional cause of the failure.
     */
    void reportFailedCheckpoint(long failureTimestamp, @Nullable Throwable cause) {
        FailedCheckpointStats failed =
                new FailedCheckpointStats(
                        checkpointId,
                        triggerTimestamp,
                        props,
                        numberOfSubtasks,
                        new HashMap<>(taskStats),
                        currentNumAcknowledgedSubtasks,
                        currentStateSize,
                        currentProcessedData,
                        currentPersistedData,
                        failureTimestamp,
                        latestAcknowledgedSubtask,
                        cause);

        trackerCallback.reportFailedCheckpoint(failed);
    }

    @Override
    public String toString() {
        return "PendingCheckpoint(id=" + getCheckpointId() + ")";
    }
}
