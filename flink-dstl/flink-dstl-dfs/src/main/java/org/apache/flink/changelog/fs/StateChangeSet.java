/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Comparator.comparingInt;
import static java.util.EnumSet.noneOf;
import static java.util.EnumSet.of;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.CANCELLED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.CONFIRMED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.MATERIALIZED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.UPLOAD_STARTED;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A set of changes made to some state(s) by a single state backend during a single checkpoint.
 * There can be zero or more change sets for a single checkpoint.
 */
@NotThreadSafe
class StateChangeSet {
    private final UUID logId;
    private final List<StateChange> changes;
    private final SequenceNumber sequenceNumber;
    private Status status;
    @Nullable private StateChangeSetUpload currentUpload;
    private boolean isUploadAssociatedWithCheckpoint;
    private final long size;

    public StateChangeSet(
            UUID logId, SequenceNumber sequenceNumber, List<StateChange> changes, Status status) {
        this.logId = logId;
        List<StateChange> copy = new ArrayList<>(changes);
        copy.sort(comparingInt(StateChange::getKeyGroup));
        this.changes = copy;
        this.sequenceNumber = sequenceNumber;
        this.status = status;
        this.size = sizeOf(changes);
    }

    public List<StateChange> getChanges() {
        return changes;
    }

    public long getSize() {
        return size;
    }

    private static long sizeOf(List<StateChange> changes) {
        long size = 0;
        for (StateChange change : changes) {
            size += change.getChange().length;
        }
        return size;
    }

    void startUpload() {
        setStatusOrFail(UPLOAD_STARTED);
        if (currentUpload != null) {
            currentUpload.cancel();
        }
        currentUpload = new StateChangeSetUpload(changes, size, logId, sequenceNumber);
    }

    private void setStatusOrFail(Status newStatus) {
        checkState(setStatus(newStatus), "can't transition from %s to %s", status, newStatus);
    }

    void setConfirmed() {
        setStatusOrFail(CONFIRMED);
        changes.clear();
    }

    void setTruncated() {
        setStatus(MATERIALIZED);
        changes.clear();
        // don't discard any current upload as it can still be relevant for the ongoing checkpoint
    }

    void setCancelled() {
        setStatus(CANCELLED);
        changes.clear();
        discardCurrentUpload();
    }

    public void discardCurrentUpload() {
        if (currentUpload != null) {
            currentUpload.cancel();
            currentUpload = null;
        }
    }

    private boolean setStatus(Status newStatus) {
        return transition(newStatus);
    }

    private boolean transition(Status newStatus) {
        if (status.canTransitionTo(newStatus)) {
            status = newStatus;
            return true;
        } else {
            return false;
        }
    }

    public Status getStatus() {
        return status;
    }

    public void associateUploadWithCheckpoint() {
        isUploadAssociatedWithCheckpoint = true;
    }

    public boolean isUploadAssociatedWithCheckpoint() {
        return isUploadAssociatedWithCheckpoint;
    }

    public StateChangeSetUpload getCurrentUpload() {
        return currentUpload;
    }

    enum Status {
        /**
         * Changes are in memory but not persisted yet (or at least without a guarantee). Will be
         * moved to UPLOAD_STARTED upon trigger checkpoint RPC, checkpoint barrier or reaching some
         * threshold.
         */
        PENDING,
        /**
         * Changes are scheduled for upload or being uploaded. From now on, any materialization
         * event is ignored for this object to prevent any potential waiting checkpoints from
         * becoming invalid.
         */
        UPLOAD_STARTED,
        /**
         * JM confirmed the checkpoint with the changes included. No re-upload will happen anymore.
         */
        CONFIRMED,
        /** State with these changes included was materialized. No upload will happen anymore. */
        MATERIALIZED,
        /** Upload of changes failed permanently. */
        FAILED,
        /** Cancelled e.g. due to shutdown. */
        CANCELLED;
        private static final Map<Status, Set<Status>> transitionsTo = new HashMap<>();

        static {
            transitionsTo.put(PENDING, of(UPLOAD_STARTED, CANCELLED, MATERIALIZED));
            transitionsTo.put(UPLOAD_STARTED, of(CONFIRMED, FAILED, CANCELLED, UPLOAD_STARTED));
            transitionsTo.put(CONFIRMED, of(CANCELLED, MATERIALIZED, CONFIRMED));
            transitionsTo.put(FAILED, of(CANCELLED, MATERIALIZED, FAILED));
            transitionsTo.put(CANCELLED, noneOf(Status.class));
        }

        public boolean canTransitionTo(Status newStatus) {
            return transitionsTo.get(this).contains(newStatus);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "sqn=%s, logId=%s, status=%s, changes=%d",
                sequenceNumber, logId, status, changes.size());
    }
}
