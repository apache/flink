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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An array based history of checkpoint stats.
 *
 * <p>The size of the array is constrained by the maximum allowed size. The array starts empty an
 * grows with each added checkpoint until it reaches the maximum number of elements. At this point,
 * the elements wrap around and the least recently added entry is overwritten.
 *
 * <p>Access happens via checkpointsHistory over the statistics and a map that exposes the
 * checkpoint by their ID. Both of these are only guaranteed to reflect the latest state after a
 * call to {@link #createSnapshot()}.
 *
 * <p>Furthermore the history tracks the latest completed and latest failed checkpoint as well as
 * the latest savepoint.
 */
public class CheckpointStatsHistory implements Serializable {

    private static final long serialVersionUID = 7090320677606528415L;

    /** List over all available stats. Only updated on {@link #createSnapshot()}. */
    private final List<AbstractCheckpointStats> checkpointsHistory;

    /** Map of all available stats keyed by their ID. Only updated on {@link #createSnapshot()}. */
    private final Map<Long, AbstractCheckpointStats> checkpointsById;

    /** Maximum array size. */
    private final int maxSize;

    /** Flag indicating whether this the history is read-only. */
    private final boolean readOnly;

    /** Array of checkpointsArray. Writes go against this array. */
    private transient AbstractCheckpointStats[] checkpointsArray;

    /** Next position in {@link #checkpointsArray} to write to. */
    private transient int nextPos;

    /** The latest successfully completed checkpoint. */
    @Nullable private CompletedCheckpointStats latestCompletedCheckpoint;

    /** The latest failed checkpoint. */
    @Nullable private FailedCheckpointStats latestFailedCheckpoint;

    /** The latest successfully completed savepoint. */
    @Nullable private CompletedCheckpointStats latestSavepoint;

    /**
     * Creates a writeable checkpoint history with the given maximum size.
     *
     * <p>The read views are only updated on calls to {@link #createSnapshot()}. Initially they are
     * empty.
     *
     * @param maxSize Maximum history size.
     */
    CheckpointStatsHistory(int maxSize) {
        this(
                false,
                maxSize,
                new AbstractCheckpointStats[0],
                Collections.emptyList(),
                Collections.emptyMap(),
                null,
                null,
                null);
    }

    /**
     * Creates a checkpoint history with the given maximum size and state.
     *
     * <p>The read views are only updated on calls to {@link #createSnapshot()}. Initially they are
     * empty.
     *
     * @param readOnly Flag indicating whether the history is read-only.
     * @param maxSize Maximum history size.
     * @param checkpointsHistory Checkpoints iterable.
     * @param checkpointsById Checkpoints by ID.
     */
    private CheckpointStatsHistory(
            boolean readOnly,
            int maxSize,
            AbstractCheckpointStats[] checkpointArray,
            List<AbstractCheckpointStats> checkpointsHistory,
            Map<Long, AbstractCheckpointStats> checkpointsById,
            @Nullable CompletedCheckpointStats latestCompletedCheckpoint,
            @Nullable FailedCheckpointStats latestFailedCheckpoint,
            @Nullable CompletedCheckpointStats latestSavepoint) {

        this.readOnly = readOnly;
        checkArgument(maxSize >= 0, "Negative maximum size");
        this.maxSize = maxSize;
        this.checkpointsArray = checkpointArray;
        this.checkpointsHistory = checkNotNull(checkpointsHistory);
        this.checkpointsById = checkNotNull(checkpointsById);
        this.latestCompletedCheckpoint = latestCompletedCheckpoint;
        this.latestFailedCheckpoint = latestFailedCheckpoint;
        this.latestSavepoint = latestSavepoint;
    }

    public List<AbstractCheckpointStats> getCheckpoints() {
        return checkpointsHistory;
    }

    public AbstractCheckpointStats getCheckpointById(long checkpointId) {
        return checkpointsById.get(checkpointId);
    }

    @Nullable
    public CompletedCheckpointStats getLatestCompletedCheckpoint() {
        return latestCompletedCheckpoint;
    }

    @Nullable
    public FailedCheckpointStats getLatestFailedCheckpoint() {
        return latestFailedCheckpoint;
    }

    @Nullable
    public CompletedCheckpointStats getLatestSavepoint() {
        return latestSavepoint;
    }

    /**
     * Creates a snapshot of the current state.
     *
     * @return Snapshot of the current state.
     */
    CheckpointStatsHistory createSnapshot() {
        if (readOnly) {
            throw new UnsupportedOperationException(
                    "Can't create a snapshot of a read-only history.");
        }

        List<AbstractCheckpointStats> checkpointsHistory;
        Map<Long, AbstractCheckpointStats> checkpointsById;

        checkpointsById = new HashMap<>(checkpointsArray.length);

        if (maxSize == 0) {
            checkpointsHistory = Collections.emptyList();
        } else {
            AbstractCheckpointStats[] newCheckpointsArray =
                    new AbstractCheckpointStats[checkpointsArray.length];

            System.arraycopy(
                    checkpointsArray,
                    nextPos,
                    newCheckpointsArray,
                    0,
                    checkpointsArray.length - nextPos);
            System.arraycopy(
                    checkpointsArray,
                    0,
                    newCheckpointsArray,
                    checkpointsArray.length - nextPos,
                    nextPos);

            checkpointsHistory = Arrays.asList(newCheckpointsArray);

            // reverse the order such that we start with the youngest checkpoint
            Collections.reverse(checkpointsHistory);

            for (AbstractCheckpointStats checkpoint : checkpointsHistory) {
                checkpointsById.put(checkpoint.getCheckpointId(), checkpoint);
            }
        }

        if (latestCompletedCheckpoint != null) {
            checkpointsById.put(
                    latestCompletedCheckpoint.getCheckpointId(), latestCompletedCheckpoint);
        }

        if (latestFailedCheckpoint != null) {
            checkpointsById.put(latestFailedCheckpoint.getCheckpointId(), latestFailedCheckpoint);
        }

        if (latestSavepoint != null) {
            checkpointsById.put(latestSavepoint.getCheckpointId(), latestSavepoint);
        }

        return new CheckpointStatsHistory(
                true,
                maxSize,
                null,
                checkpointsHistory,
                checkpointsById,
                latestCompletedCheckpoint,
                latestFailedCheckpoint,
                latestSavepoint);
    }

    /**
     * Adds an in progress checkpoint to the checkpoint history.
     *
     * @param pending In progress checkpoint to add.
     */
    void addInProgressCheckpoint(PendingCheckpointStats pending) {
        if (readOnly) {
            throw new UnsupportedOperationException(
                    "Can't create a snapshot of a read-only history.");
        }

        if (maxSize == 0) {
            return;
        }

        checkNotNull(pending, "Pending checkpoint");

        // Grow the array if required. This happens only for the first entries
        // and makes the iterator logic easier, because we don't have any
        // null elements with the growing array.
        if (checkpointsArray.length < maxSize) {
            checkpointsArray = Arrays.copyOf(checkpointsArray, checkpointsArray.length + 1);
        }

        // Wrap around if we are at the end. The next pos is the least recently
        // added checkpoint.
        if (nextPos == checkpointsArray.length) {
            nextPos = 0;
        }

        checkpointsArray[nextPos++] = pending;
    }

    /**
     * Searches for the in progress checkpoint with the given ID and replaces it with the given
     * completed or failed checkpoint.
     *
     * <p>This is bounded by the maximum number of concurrent in progress checkpointsArray, which
     * means that the runtime of this is constant.
     *
     * @param completedOrFailed The completed or failed checkpoint to replace the in progress
     *     checkpoint with.
     * @return <code>true</code> if the checkpoint was replaced or <code>false</code> otherwise.
     */
    boolean replacePendingCheckpointById(AbstractCheckpointStats completedOrFailed) {
        checkArgument(
                !completedOrFailed.getStatus().isInProgress(),
                "Not allowed to replace with in progress checkpoints.");

        if (readOnly) {
            throw new UnsupportedOperationException(
                    "Can't create a snapshot of a read-only history.");
        }

        // Update the latest checkpoint stats
        if (completedOrFailed.getStatus().isCompleted()) {
            CompletedCheckpointStats completed = (CompletedCheckpointStats) completedOrFailed;
            if (completed.getProperties().isSavepoint()
                    && (latestSavepoint == null
                            || completed.getCheckpointId() > latestSavepoint.getCheckpointId())) {

                latestSavepoint = completed;
            } else if (latestCompletedCheckpoint == null
                    || completed.getCheckpointId() > latestCompletedCheckpoint.getCheckpointId()) {

                latestCompletedCheckpoint = completed;
            }
        } else if (completedOrFailed.getStatus().isFailed()) {
            FailedCheckpointStats failed = (FailedCheckpointStats) completedOrFailed;
            if (latestFailedCheckpoint == null
                    || failed.getCheckpointId() > latestFailedCheckpoint.getCheckpointId()) {

                latestFailedCheckpoint = failed;
            }
        }

        if (maxSize == 0) {
            return false;
        }

        long checkpointId = completedOrFailed.getCheckpointId();

        // We start searching from the last inserted position. Since the entries
        // wrap around the array we search until we are at index 0 and then from
        // the end of the array until (start pos + 1).
        int startPos =
                nextPos == checkpointsArray.length ? checkpointsArray.length - 1 : nextPos - 1;

        for (int i = startPos; i >= 0; i--) {
            if (checkpointsArray[i].getCheckpointId() == checkpointId) {
                checkpointsArray[i] = completedOrFailed;
                return true;
            }
        }

        for (int i = checkpointsArray.length - 1; i > startPos; i--) {
            if (checkpointsArray[i].getCheckpointId() == checkpointId) {
                checkpointsArray[i] = completedOrFailed;
                return true;
            }
        }

        return false;
    }
}
