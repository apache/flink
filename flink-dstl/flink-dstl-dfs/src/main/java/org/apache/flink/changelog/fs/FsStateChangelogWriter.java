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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploader.UploadTask;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.SequenceNumberRange;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.flink.util.IOUtils.closeAllQuietly;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Filesystem-based {@link StateChangelogWriter} implementation. Assumes TM-owned state - so no
 * re-uploads.
 *
 * <p>On {@link #append(int, byte[]) append}, it stores the changes locally in memory (without any
 * thread synchronization); {@link SequenceNumber} is not changed.
 *
 * <p>However, if they exceed {@link #preEmptivePersistThresholdInBytes} then {@link
 * #persistInternal(SequenceNumber) persist} is called.
 *
 * <p>On {@link #persist(SequenceNumber) persist}, accumulated changes are sent to the {@link
 * StateChangeUploader} as an immutable {@link StateChangeUploader.UploadTask task}. An {@link
 * FsStateChangelogWriter.UploadCompletionListener upload listener} is also registered. Upon
 * notification it updates the Writer local state (for future persist calls) and completes the
 * future returned to the original caller. The uploader notifies all listeners via a callback in a
 * task.
 *
 * <p>If persist() is called for the same state change before the upload completion then the
 * listener is added but not the upload task (which must already exist).
 *
 * <p>Invariants:
 *
 * <ol>
 *   <li>Every change has at most one associated upload (retries are performed at a lower level)
 *   <li>Every change is present in at most one collection: either {@link #uploaded} OR {@link
 *       #notUploaded}
 *   <li>Changes BEING uploaded are NOT referenced locally - they will be added to uploaded upon
 *       completion
 *   <li>Failed and truncated changes are NOT stored - only their respective highest sequence
 *       numbers
 * </ol>
 */
@NotThreadSafe
class FsStateChangelogWriter implements StateChangelogWriter<ChangelogStateHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriter.class);
    private static final SequenceNumber INITIAL_SQN = SequenceNumber.of(0L);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeUploader uploader;
    private final long preEmptivePersistThresholdInBytes;

    /** Lock to synchronize handling of upload completion with new upload requests. */
    // todo: replace with mailbox executor (after FLINK-23204)
    private final Object lock = new Object();

    /** A list of listener per upload (~ per checkpoint plus pre-emptive uploads). */
    @GuardedBy("lock")
    private final List<UploadCompletionListener> uploadCompletionListeners = new ArrayList<>();

    /** Current {@link SequenceNumber}. */
    private SequenceNumber activeSequenceNumber = INITIAL_SQN;

    /**
     * {@link SequenceNumber} before which changes will NOT be requested, exclusive. Increased after
     * materialization.
     */
    @GuardedBy("lock")
    private SequenceNumber lowestSequenceNumber = INITIAL_SQN;

    /**
     * Active changes, that all share the same {@link #activeSequenceNumber}.
     *
     * <p>When the latter is incremented in {@link #rollover()}, those changes are added to {@link
     * #notUploaded} and {@link #activeChangeSet} is emptied.
     */
    private List<StateChange> activeChangeSet = new ArrayList<>();

    /** {@link #activeChangeSet} size in bytes. */
    private long activeChangeSetSize;

    /** Changes that are not yet uploaded (upload not requested). */
    private final NavigableMap<SequenceNumber, StateChangeSet> notUploaded = new TreeMap<>();

    /** Uploaded changes, ready for use in snapshots. */
    @GuardedBy("lock")
    private final NavigableMap<SequenceNumber, UploadResult> uploaded = new TreeMap<>();

    /**
     * Highest {@link SequenceNumber} for which upload has failed (won't be restarted), inclusive.
     */
    @Nullable
    @GuardedBy("lock")
    private Tuple2<SequenceNumber, Throwable> highestFailed;

    @GuardedBy("lock")
    private boolean closed;

    FsStateChangelogWriter(
            UUID logId,
            KeyGroupRange keyGroupRange,
            StateChangeUploader uploader,
            long preEmptivePersistThresholdInBytes) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.uploader = uploader;
        this.preEmptivePersistThresholdInBytes = preEmptivePersistThresholdInBytes;
    }

    @Override
    public void append(int keyGroup, byte[] value) throws IOException {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        checkState(!closed, "%s is closed", logId);
        activeChangeSet.add(new StateChange(keyGroup, value));
        activeChangeSetSize += value.length;
        if (activeChangeSetSize >= preEmptivePersistThresholdInBytes) {
            LOG.debug(
                    "pre-emptively flush {}Mb of appended changes to the common store",
                    activeChangeSetSize / 1024 / 1024);
            persistInternal(notUploaded.isEmpty() ? activeSequenceNumber : notUploaded.firstKey());
        }
    }

    @Override
    public SequenceNumber initialSequenceNumber() {
        return INITIAL_SQN;
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        LOG.trace("query {} sqn: {}", logId, activeSequenceNumber);
        SequenceNumber tmp = activeSequenceNumber;
        // the returned current sequence number must be able to distinguish between the changes
        // appended before and after this call so we need to use the next sequence number
        // At the same time, we don't want to increment SQN on each append (to avoid too many
        // objects and segments in the resulting file).
        rollover();
        return tmp;
    }

    @Override
    public CompletableFuture<ChangelogStateHandleStreamImpl> persist(SequenceNumber from)
            throws IOException {
        LOG.debug(
                "persist {} starting from sqn {} (incl.), active sqn: {}",
                logId,
                from,
                activeSequenceNumber);
        return persistInternal(from);
    }

    private CompletableFuture<ChangelogStateHandleStreamImpl> persistInternal(SequenceNumber from)
            throws IOException {
        synchronized (lock) {
            ensureCanPersist(from);
            rollover();
            Map<SequenceNumber, StateChangeSet> toUpload = drainTailMap(notUploaded, from);
            NavigableMap<SequenceNumber, UploadResult> readyToReturn = uploaded.tailMap(from, true);
            LOG.debug("collected readyToReturn: {}, toUpload: {}", readyToReturn, toUpload);

            SequenceNumberRange range = SequenceNumberRange.generic(from, activeSequenceNumber);
            if (range.size() == readyToReturn.size()) {
                checkState(toUpload.isEmpty());
                return completedFuture(buildHandle(keyGroupRange, readyToReturn));
            } else {
                CompletableFuture<ChangelogStateHandleStreamImpl> future =
                        new CompletableFuture<>();
                uploadCompletionListeners.add(
                        new UploadCompletionListener(keyGroupRange, range, readyToReturn, future));
                if (!toUpload.isEmpty()) {
                    uploader.upload(
                            new UploadTask(
                                    toUpload.values(),
                                    this::handleUploadSuccess,
                                    this::handleUploadFailure));
                }
                return future;
            }
        }
    }

    private void handleUploadFailure(List<SequenceNumber> failedSqn, Throwable throwable) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            uploadCompletionListeners.removeIf(
                    listener -> listener.onFailure(failedSqn, throwable));
            failedSqn.stream()
                    .max(Comparator.naturalOrder())
                    .filter(sqn -> sqn.compareTo(lowestSequenceNumber) >= 0)
                    .filter(sqn -> highestFailed == null || sqn.compareTo(highestFailed.f0) > 0)
                    .ifPresent(sqn -> highestFailed = Tuple2.of(sqn, throwable));
        }
    }

    private void handleUploadSuccess(List<UploadResult> results) {
        synchronized (lock) {
            if (closed) {
                results.forEach(
                        r -> closeAllQuietly(() -> r.getStreamStateHandle().discardState()));
            } else {
                uploadCompletionListeners.removeIf(listener -> listener.onSuccess(results));
                for (UploadResult result : results) {
                    if (result.sequenceNumber.compareTo(lowestSequenceNumber) >= 0) {
                        uploaded.put(result.sequenceNumber, result);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        LOG.debug("close {}", logId);
        synchronized (lock) {
            checkState(!closed);
            closed = true;
            activeChangeSet.clear();
            activeChangeSetSize = 0;
            notUploaded.clear();
            uploaded.clear();
        }
    }

    @Override
    public void truncate(SequenceNumber to) {
        LOG.debug("truncate {} to sqn {} (excl.)", logId, to);
        checkArgument(to.compareTo(activeSequenceNumber) <= 0);
        synchronized (lock) {
            lowestSequenceNumber = to;
            notUploaded.headMap(lowestSequenceNumber, false).clear();
            uploaded.headMap(lowestSequenceNumber, false).clear();
        }
    }

    private void rollover() {
        if (activeChangeSet.isEmpty()) {
            return;
        }
        notUploaded.put(
                activeSequenceNumber,
                new StateChangeSet(logId, activeSequenceNumber, activeChangeSet));
        activeSequenceNumber = activeSequenceNumber.next();
        LOG.debug("bump active sqn to {}", activeSequenceNumber);
        activeChangeSet = new ArrayList<>();
        activeChangeSetSize = 0;
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {
        // do nothing
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        // do nothing
    }

    private static ChangelogStateHandleStreamImpl buildHandle(
            KeyGroupRange keyGroupRange, NavigableMap<SequenceNumber, UploadResult> results) {
        List<Tuple2<StreamStateHandle, Long>> tuples = new ArrayList<>();
        long size = 0;
        for (UploadResult uploadResult : results.values()) {
            tuples.add(Tuple2.of(uploadResult.getStreamStateHandle(), uploadResult.getOffset()));
            size += uploadResult.getSize();
        }
        return new ChangelogStateHandleStreamImpl(tuples, keyGroupRange, size);
    }

    @VisibleForTesting
    SequenceNumber lastAppendedSqnUnsafe() {
        return activeSequenceNumber;
    }

    private void ensureCanPersist(SequenceNumber from) throws IOException {
        checkNotNull(from);
        if (highestFailed != null && highestFailed.f0.compareTo(from) >= 0) {
            throw new IOException(
                    "The upload for " + highestFailed.f0 + " has already failed previously",
                    highestFailed.f1);
        } else if (lowestSequenceNumber.compareTo(from) > 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Requested changes were truncated (requested: %s, truncated: %s)",
                            from, lowestSequenceNumber));
        } else if (activeSequenceNumber.compareTo(from) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Requested changes were not yet appended (requested: %s, appended: %s)",
                            from, activeSequenceNumber));
        }
    }

    private static final class UploadCompletionListener {
        private final NavigableMap<SequenceNumber, UploadResult> uploaded;
        private final CompletableFuture<ChangelogStateHandleStreamImpl> completionFuture;
        private final KeyGroupRange keyGroupRange;
        private final SequenceNumberRange changeRange;

        private UploadCompletionListener(
                KeyGroupRange keyGroupRange,
                SequenceNumberRange changeRange,
                Map<SequenceNumber, UploadResult> uploaded,
                CompletableFuture<ChangelogStateHandleStreamImpl> completionFuture) {
            checkArgument(
                    !changeRange.isEmpty(), "Empty change range not allowed: %s", changeRange);
            this.uploaded = new TreeMap<>(uploaded);
            this.completionFuture = completionFuture;
            this.keyGroupRange = keyGroupRange;
            this.changeRange = changeRange;
        }

        public boolean onSuccess(List<UploadResult> uploadResults) {
            for (UploadResult uploadResult : uploadResults) {
                if (changeRange.contains(uploadResult.sequenceNumber)) {
                    uploaded.put(uploadResult.sequenceNumber, uploadResult);
                    if (uploaded.size() == changeRange.size()) {
                        completionFuture.complete(buildHandle(keyGroupRange, uploaded));
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean onFailure(List<SequenceNumber> sequenceNumbers, Throwable throwable) {
            IOException ioException =
                    throwable instanceof IOException
                            ? (IOException) throwable
                            : new IOException(throwable);
            for (SequenceNumber sequenceNumber : sequenceNumbers) {
                if (changeRange.contains(sequenceNumber)) {
                    completionFuture.completeExceptionally(ioException);
                    return true;
                }
            }
            return false;
        }
    }

    private static Map<SequenceNumber, StateChangeSet> drainTailMap(
            NavigableMap<SequenceNumber, StateChangeSet> src, SequenceNumber fromInclusive) {
        Map<SequenceNumber, StateChangeSet> tailMap = src.tailMap(fromInclusive, true);
        Map<SequenceNumber, StateChangeSet> toUpload = new HashMap<>(tailMap);
        tailMap.clear();
        return toUpload;
    }
}
