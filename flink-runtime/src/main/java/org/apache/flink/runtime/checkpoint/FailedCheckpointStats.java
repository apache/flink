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

/**
 * Statistics for a failed checkpoint.
 *
 * <p>The reported statistics are immutable.
 */
public class FailedCheckpointStats extends AbstractCheckpointStats {

    private static final long serialVersionUID = 8000748529515900106L;

    /** Number of acknowledged tasks. */
    private final int numAcknowledgedSubtasks;

    /** Total checkpoint state size over all subtasks. */
    private final long stateSize;

    private final long processedData;

    private final long persistedData;

    /** Timestamp when the checkpoint was failed at the coordinator. */
    private final long failureTimestamp;

    /**
     * The latest acknowledged subtask stats if any subtask was acknowledged before failing the
     * checkpoint.
     */
    @Nullable private final SubtaskStateStats latestAcknowledgedSubtask;

    /** Optional failure message. */
    @Nullable private final String failureMsg;

    /**
     * Creates a tracker for a failed checkpoint.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Timestamp when the checkpoint was triggered.
     * @param props Checkpoint properties of the checkpoint.
     * @param totalSubtaskCount Total number of subtasks for the checkpoint.
     * @param taskStats Task stats for each involved operator.
     * @param numAcknowledgedSubtasks Number of acknowledged subtasks.
     * @param stateSize Total checkpoint state size over all subtasks.
     * @param processedData Processed data during the checkpoint.
     * @param persistedData Persisted data during the checkpoint.
     * @param failureTimestamp Timestamp when this checkpoint failed.
     * @param latestAcknowledgedSubtask The latest acknowledged subtask stats or <code>null</code>.
     * @param cause Cause of the checkpoint failure or <code>null</code>.
     */
    FailedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long stateSize,
            long processedData,
            long persistedData,
            long failureTimestamp,
            @Nullable SubtaskStateStats latestAcknowledgedSubtask,
            @Nullable Throwable cause) {

        super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
        checkArgument(numAcknowledgedSubtasks >= 0, "Negative number of ACKs");
        this.numAcknowledgedSubtasks = numAcknowledgedSubtasks;
        checkArgument(stateSize >= 0, "Negative state size");
        this.stateSize = stateSize;
        this.processedData = processedData;
        this.persistedData = persistedData;
        this.failureTimestamp = failureTimestamp;
        this.latestAcknowledgedSubtask = latestAcknowledgedSubtask;
        this.failureMsg = cause != null ? cause.getMessage() : null;
    }

    @Override
    public CheckpointStatsStatus getStatus() {
        return CheckpointStatsStatus.FAILED;
    }

    @Override
    public int getNumberOfAcknowledgedSubtasks() {
        return numAcknowledgedSubtasks;
    }

    @Override
    public long getStateSize() {
        return stateSize;
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
    @Nullable
    public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
        return latestAcknowledgedSubtask;
    }

    /** Returns the end to end duration until the checkpoint failure. */
    @Override
    public long getEndToEndDuration() {
        return Math.max(0, failureTimestamp - triggerTimestamp);
    }

    // ------------------------------------------------------------------------
    // Failed checkpoint specific methods
    // ------------------------------------------------------------------------

    /**
     * Returns the timestamp when this checkpoint failed.
     *
     * @return Timestamp when the checkpoint failed.
     */
    public long getFailureTimestamp() {
        return failureTimestamp;
    }

    /**
     * Returns the failure message or <code>null</code> if no cause was provided.
     *
     * @return Failure message of the checkpoint failure or <code>null</code>.
     */
    @Nullable
    public String getFailureMessage() {
        return failureMsg;
    }
}
