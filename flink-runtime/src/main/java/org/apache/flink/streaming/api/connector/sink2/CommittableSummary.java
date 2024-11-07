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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Experimental;

import java.util.Objects;

/**
 * This class tracks the information about committables belonging to one checkpoint coming from one
 * subtask.
 *
 * <p>It is sent to down-stream consumers to depict the progress of the committing process.
 *
 * @param <CommT> type of the committable
 */
@Experimental
public class CommittableSummary<CommT> implements CommittableMessage<CommT> {
    private final int subtaskId;
    /** May change after recovery. */
    private final int numberOfSubtasks;

    private final long checkpointId;
    /** The number of committables coming from the given subtask in the particular checkpoint. */
    private final int numberOfCommittables;

    @Deprecated
    /** The number of committables that have not been successfully committed. */
    private final int numberOfPendingCommittables;
    /** The number of committables that are not retried and have been failed. */
    private final int numberOfFailedCommittables;

    public CommittableSummary(
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int numberOfCommittables,
            int numberOfFailedCommittables) {
        this(
                subtaskId,
                numberOfSubtasks,
                checkpointId,
                numberOfCommittables,
                0,
                numberOfFailedCommittables);
    }

    @Deprecated
    public CommittableSummary(
            int subtaskId,
            int numberOfSubtasks,
            long checkpointId,
            int numberOfCommittables,
            int numberOfPendingCommittables,
            int numberOfFailedCommittables) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        this.checkpointId = checkpointId;
        this.numberOfCommittables = numberOfCommittables;
        this.numberOfPendingCommittables = numberOfPendingCommittables;
        this.numberOfFailedCommittables = numberOfFailedCommittables;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public long getCheckpointIdOrEOI() {
        return checkpointId;
    }

    public int getNumberOfCommittables() {
        return numberOfCommittables;
    }

    @Deprecated
    public int getNumberOfPendingCommittables() {
        return 0;
    }

    public int getNumberOfFailedCommittables() {
        return numberOfFailedCommittables;
    }

    public <NewCommT> CommittableSummary<NewCommT> map() {
        return new CommittableSummary<>(
                subtaskId,
                numberOfSubtasks,
                checkpointId,
                numberOfCommittables,
                numberOfFailedCommittables);
    }

    @Override
    public String toString() {
        return "CommittableSummary{"
                + "subtaskId="
                + subtaskId
                + ", numberOfSubtasks="
                + numberOfSubtasks
                + ", checkpointId="
                + checkpointId
                + ", numberOfCommittables="
                + numberOfCommittables
                + ", numberOfPendingCommittables="
                + numberOfPendingCommittables
                + ", numberOfFailedCommittables="
                + numberOfFailedCommittables
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittableSummary<?> that = (CommittableSummary<?>) o;
        return subtaskId == that.subtaskId
                && numberOfSubtasks == that.numberOfSubtasks
                && checkpointId == that.checkpointId
                && numberOfCommittables == that.numberOfCommittables
                && numberOfPendingCommittables == that.numberOfPendingCommittables
                && numberOfFailedCommittables == that.numberOfFailedCommittables;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                subtaskId,
                numberOfSubtasks,
                checkpointId,
                numberOfCommittables,
                numberOfPendingCommittables,
                numberOfFailedCommittables);
    }
}
