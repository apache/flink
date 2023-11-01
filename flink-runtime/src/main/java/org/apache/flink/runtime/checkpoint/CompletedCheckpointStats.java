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

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Statistics for a successfully completed checkpoint.
 *
 * <p>The reported statistics are immutable except for the discarded flag, which is updated via the
 * {@link DiscardCallback} and the {@link CompletedCheckpoint} after an instance of this class has
 * been created.
 */
public class CompletedCheckpointStats extends AbstractCheckpointStats {

    private static final long serialVersionUID = 138833868551861344L;

    /** Total persisted data size over all subtasks of this checkpoint. */
    private final long checkpointedSize;

    /** Total checkpoint state size over all subtasks. */
    private final long stateSize;

    private final long processedData;

    private final long persistedData;

    private final boolean unalignedCheckpoint;

    /** The latest acknowledged subtask stats. */
    private final SubtaskStateStats latestAcknowledgedSubtask;

    /** The external pointer of the checkpoint. */
    private final String externalPointer;

    /** Flag indicating whether the checkpoint was discarded. */
    private volatile boolean discarded;

    CompletedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long stateSize,
            long processedData,
            long persistedData,
            boolean unalignedCheckpoint,
            SubtaskStateStats latestAcknowledgedSubtask,
            String externalPointer) {
        this(
                checkpointId,
                triggerTimestamp,
                props,
                totalSubtaskCount,
                taskStats,
                numAcknowledgedSubtasks,
                stateSize,
                stateSize,
                processedData,
                persistedData,
                unalignedCheckpoint,
                latestAcknowledgedSubtask,
                externalPointer);
    }

    /**
     * Creates a tracker for a {@link CompletedCheckpoint}.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Timestamp when the checkpoint was triggered.
     * @param props Checkpoint properties of the checkpoint.
     * @param totalSubtaskCount Total number of subtasks for the checkpoint.
     * @param taskStats Task stats for each involved operator.
     * @param numAcknowledgedSubtasks Number of acknowledged subtasks.
     * @param checkpointedSize Total persisted data size over all subtasks during the sync and async
     *     phases of this checkpoint.
     * @param stateSize Total checkpoint state size over all subtasks.
     * @param processedData Processed data during the checkpoint.
     * @param persistedData Persisted data during the checkpoint.
     * @param unalignedCheckpoint Whether the checkpoint is unaligned.
     * @param latestAcknowledgedSubtask The latest acknowledged subtask stats.
     * @param externalPointer Optional external path if persisted externally.
     */
    CompletedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long checkpointedSize,
            long stateSize,
            long processedData,
            long persistedData,
            boolean unalignedCheckpoint,
            SubtaskStateStats latestAcknowledgedSubtask,
            String externalPointer) {

        super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
        checkArgument(
                numAcknowledgedSubtasks == totalSubtaskCount, "Did not acknowledge all subtasks.");
        checkArgument(checkpointedSize >= 0, "Negative checkpointed size");
        this.checkpointedSize = checkpointedSize;
        checkArgument(stateSize >= 0, "Negative state size");
        this.stateSize = stateSize;
        this.processedData = processedData;
        this.persistedData = persistedData;
        this.unalignedCheckpoint = unalignedCheckpoint;
        this.latestAcknowledgedSubtask = checkNotNull(latestAcknowledgedSubtask);
        this.externalPointer = externalPointer;
    }

    @Override
    public CheckpointStatsStatus getStatus() {
        return CheckpointStatsStatus.COMPLETED;
    }

    @Override
    public int getNumberOfAcknowledgedSubtasks() {
        return numberOfSubtasks;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    @Override
    public long getCheckpointedSize() {
        return checkpointedSize;
    }

    @Override
    public long getProcessedData() {
        return processedData;
    }

    @Override
    public long getPersistedData() {
        return persistedData;
    }

    @Override
    public boolean isUnalignedCheckpoint() {
        return unalignedCheckpoint;
    }

    @Override
    @Nullable
    public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
        return latestAcknowledgedSubtask;
    }

    // ------------------------------------------------------------------------
    // Completed checkpoint specific methods
    // ------------------------------------------------------------------------

    /** Returns the external pointer of this checkpoint. */
    public String getExternalPath() {
        return externalPointer;
    }

    /**
     * Returns whether the checkpoint has been discarded.
     *
     * @return <code>true</code> if the checkpoint has been discarded, <code>false</code> otherwise.
     */
    public boolean isDiscarded() {
        return discarded;
    }

    /** Mark the checkpoint has been discarded. */
    void discard() {
        discarded = true;
    }

    @Override
    public String toString() {
        return "CompletedCheckpoint(id=" + getCheckpointId() + ")";
    }
}
