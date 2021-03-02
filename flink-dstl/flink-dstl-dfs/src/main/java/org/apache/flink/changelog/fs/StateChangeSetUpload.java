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
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An upload of {@link StateChangeSet}. Note that there can exist more than such upload for a single
 * change set if the checkpoint that initiated it wasn't confirmed at the time of the next one.
 */
@ThreadSafe
class StateChangeSetUpload {
    private final long size;
    private final UUID logId;
    private final SequenceNumber sequenceNumber;
    private final CompletableFuture<StoreResult> storeResultFuture;
    private final AtomicReference<Status> status = new AtomicReference<>(Status.PENDING);
    private List<StateChange> changes;

    private enum Status {
        PENDING,
        UPLOADING,
        COMPLETED // uploaded/cancelled/failed
    }

    StateChangeSetUpload(
            List<StateChange> changes, long size, UUID logId, SequenceNumber sequenceNumber) {
        this.changes = checkNotNull(changes);
        this.sequenceNumber = checkNotNull(sequenceNumber);
        this.storeResultFuture = new CompletableFuture<>();
        this.size = size;
        this.logId = checkNotNull(logId);
    }

    public void cancel() {
        fail(new CancellationException());
    }

    public void fail(Throwable error) {
        if (status.compareAndSet(Status.PENDING, Status.COMPLETED)) {
            storeResultFuture.completeExceptionally(error);
            dispose(); // only free memory, nothing was uploaded
        }
        // if the upload is COMPLETED then there is a chance it was sent to JM
        // so we shouldn't discard lest invalidate any checkpoints
    }

    public boolean startUpload() {
        // prevent disposal during the upload
        return status.compareAndSet(Status.PENDING, Status.UPLOADING);
    }

    public void complete(StoreResult storeResult, FsStateChangelogCleaner cleaner) {
        if (status.compareAndSet(Status.UPLOADING, Status.COMPLETED)) {
            storeResultFuture.complete(storeResult);
        } else {
            cleaner.cleanupAsync(storeResult);
        }
        dispose();
    }

    private void dispose() {
        checkState(
                status.get() == Status.COMPLETED, "Upload in status %s can't be disposed", status);
        changes = null;
    }

    public long getSize() {
        return size;
    }

    /** @return guaranteed to return not null unless was already completed (successfully or not). */
    @Nullable
    public List<StateChange> getChanges() {
        return changes;
    }

    public UUID getLogId() {
        return logId;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    public CompletableFuture<StoreResult> getStoreResultFuture() {
        return storeResultFuture;
    }
}
