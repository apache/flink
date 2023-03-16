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
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.LocalChangelogRegistry;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.SequenceNumberRange;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * #persistInternal(SequenceNumber, long) persist} is called.
 *
 * <p>On {@link #persist(SequenceNumber, long) persist}, accumulated changes are sent to the {@link
 * StateChangeUploadScheduler} as an immutable {@link StateChangeUploadScheduler.UploadTask task}.
 * An {@link FsStateChangelogWriter.UploadCompletionListener upload listener} is also registered.
 * Upon notification it updates the Writer local state (for future persist calls) and completes the
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
    private static final long DUMMY_PERSIST_CHECKPOINT = -1L;
    private static final SequenceNumber INITIAL_SQN = SequenceNumber.of(0L);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeUploadScheduler uploader;
    private final long preEmptivePersistThresholdInBytes;

    /** A list of listener per upload (~ per checkpoint plus pre-emptive uploads). */
    private final List<UploadCompletionListener> uploadCompletionListeners = new ArrayList<>();

    /** Current {@link SequenceNumber}. */
    private SequenceNumber activeSequenceNumber = INITIAL_SQN;

    /**
     * {@link SequenceNumber} before which changes will NOT be requested, exclusive. Increased after
     * materialization.
     */
    private SequenceNumber lowestSequenceNumber = INITIAL_SQN;

    /**
     * {@link SequenceNumber} after which changes will NOT be requested, inclusive. Decreased on
     * {@link #truncateAndClose(SequenceNumber)}.
     */
    private SequenceNumber highestSequenceNumber = SequenceNumber.of(Long.MAX_VALUE);

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
    private final NavigableMap<SequenceNumber, UploadResult> uploaded = new TreeMap<>();

    /**
     * Highest {@link SequenceNumber} for which upload has failed (won't be restarted), inclusive.
     */
    @Nullable private Tuple2<SequenceNumber, Throwable> highestFailed;

    private boolean closed;

    private final MailboxExecutor mailboxExecutor;

    private final TaskChangelogRegistry changelogRegistry;

    /** The configuration for local recovery. */
    @Nonnull private final LocalRecoveryConfig localRecoveryConfig;

    private final LocalChangelogRegistry localChangelogRegistry;

    FsStateChangelogWriter(
            UUID logId,
            KeyGroupRange keyGroupRange,
            StateChangeUploadScheduler uploader,
            long preEmptivePersistThresholdInBytes,
            MailboxExecutor mailboxExecutor,
            TaskChangelogRegistry changelogRegistry,
            LocalRecoveryConfig localRecoveryConfig,
            LocalChangelogRegistry localChangelogRegistry) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.uploader = uploader;
        this.preEmptivePersistThresholdInBytes = preEmptivePersistThresholdInBytes;
        this.mailboxExecutor = mailboxExecutor;
        this.changelogRegistry = changelogRegistry;
        this.localRecoveryConfig = localRecoveryConfig;
        this.localChangelogRegistry = localChangelogRegistry;
    }

    @Override
    public void appendMeta(byte[] value) throws IOException {
        if (closed) {
            LOG.warn("{} is closed.", logId);
            return;
        }
        LOG.trace("append metadata to {}: {} bytes", logId, value.length);
        activeChangeSet.add(StateChange.ofMetadataChange(value));
        preEmptiveFlushIfNeeded(value);
    }

    @Override
    public void append(int keyGroup, byte[] value) throws IOException {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        if (closed) {
            LOG.warn("{} is closed.", logId);
            return;
        }
        activeChangeSet.add(StateChange.ofDataChange(keyGroup, value));
        preEmptiveFlushIfNeeded(value);
    }

    @Override
    public SequenceNumber initialSequenceNumber() {
        return INITIAL_SQN;
    }

    @Override
    public SequenceNumber nextSequenceNumber() {
        // the returned current sequence number must be able to distinguish between the changes
        // appended before and after this call so we need to use the next sequence number
        // At the same time, we don't want to increment SQN on each append (to avoid too many
        // objects and segments in the resulting file).
        rollover();
        LOG.trace("query {} sqn: {}", logId, activeSequenceNumber);
        return activeSequenceNumber;
    }

    @Override
    public CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> persist(
            SequenceNumber from, long checkpointId) throws IOException {
        LOG.debug(
                "persist {} starting from sqn {} (incl.), active sqn: {}",
                logId,
                from,
                activeSequenceNumber);
        return persistInternal(from, checkpointId);
    }

    private void preEmptiveFlushIfNeeded(byte[] value) throws IOException {
        activeChangeSetSize += value.length;
        if (activeChangeSetSize >= preEmptivePersistThresholdInBytes) {
            LOG.debug(
                    "pre-emptively flush {}MB of appended changes to the common store",
                    activeChangeSetSize / 1024 / 1024);
            persistInternal(
                    notUploaded.isEmpty() ? activeSequenceNumber : notUploaded.firstKey(),
                    DUMMY_PERSIST_CHECKPOINT);
        }
    }

    private CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> persistInternal(
            SequenceNumber from, long checkpointId) throws IOException {
        ensureCanPersist(from);
        rollover();
        Map<SequenceNumber, StateChangeSet> toUpload = drainTailMap(notUploaded, from);
        NavigableMap<SequenceNumber, UploadResult> readyToReturn = uploaded.tailMap(from, true);
        LOG.debug(
                "collected readyToReturn: {}, toUpload: {}, checkpointId: {}.",
                readyToReturn,
                toUpload,
                checkpointId);

        if (checkpointId != DUMMY_PERSIST_CHECKPOINT) {
            for (UploadResult uploadResult : readyToReturn.values()) {
                if (uploadResult.localStreamHandle != null) {
                    localChangelogRegistry.register(uploadResult.localStreamHandle, checkpointId);
                }
            }
        }

        SequenceNumberRange range = SequenceNumberRange.generic(from, activeSequenceNumber);
        if (range.size() == readyToReturn.size()) {
            checkState(toUpload.isEmpty());
            return CompletableFuture.completedFuture(
                    buildSnapshotResult(keyGroupRange, readyToReturn, 0L));
        } else {
            CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>> future =
                    new CompletableFuture<>();
            uploadCompletionListeners.add(
                    new UploadCompletionListener(keyGroupRange, range, readyToReturn, future));
            if (!toUpload.isEmpty()) {
                UploadTask uploadTask =
                        new UploadTask(
                                toUpload.values(),
                                uploadResults -> handleUploadSuccess(uploadResults, checkpointId),
                                this::handleUploadFailure);
                uploader.upload(uploadTask);
            }
            return future;
        }
    }

    private void handleUploadFailure(List<SequenceNumber> failedSqn, Throwable throwable) {
        mailboxExecutor.execute(
                () -> {
                    if (closed) {
                        return;
                    }
                    uploadCompletionListeners.removeIf(
                            listener -> listener.onFailure(failedSqn, throwable));
                    failedSqn.stream()
                            .max(Comparator.naturalOrder())
                            .filter(sqn -> sqn.compareTo(lowestSequenceNumber) >= 0)
                            .filter(
                                    sqn ->
                                            highestFailed == null
                                                    || sqn.compareTo(highestFailed.f0) > 0)
                            .ifPresent(sqn -> highestFailed = Tuple2.of(sqn, throwable));
                },
                "handleUploadFailure");
    }

    private void handleUploadSuccess(List<UploadResult> results, long checkpointId) {
        mailboxExecutor.execute(
                () -> {
                    if (closed) {
                        results.forEach(
                                r ->
                                        closeAllQuietly(
                                                () -> r.getStreamStateHandle().discardState()));
                    } else {
                        uploadCompletionListeners.removeIf(listener -> listener.onSuccess(results));
                        for (UploadResult result : results) {
                            if (checkpointId != DUMMY_PERSIST_CHECKPOINT) {
                                if (result.localStreamHandle != null) {
                                    localChangelogRegistry.register(
                                            result.localStreamHandle, checkpointId);
                                }
                            }
                            SequenceNumber resultSqn = result.sequenceNumber;
                            if (resultSqn.compareTo(lowestSequenceNumber) >= 0
                                    && resultSqn.compareTo(highestSequenceNumber) < 0) {
                                uploaded.put(resultSqn, result);
                            } else {
                                // uploaded already truncated, i.e. materialized state changes,
                                // or closed
                                changelogRegistry.release(result.streamStateHandle);
                                if (result.localStreamHandle != null) {
                                    changelogRegistry.release(result.localStreamHandle);
                                }
                            }
                        }
                    }
                },
                "handleUploadSuccess");
    }

    @Override
    public void close() {
        LOG.debug("close {}", logId);
        checkState(!closed);
        closed = true;
        activeChangeSet.clear();
        activeChangeSetSize = 0;
        notUploaded.clear();
        uploaded.clear();
    }

    @Override
    public void truncate(SequenceNumber to) {
        LOG.debug("truncate {} to sqn {} (excl.)", logId, to);
        checkArgument(to.compareTo(activeSequenceNumber) <= 0);
        lowestSequenceNumber = to;
        notUploaded.headMap(lowestSequenceNumber, false).clear();

        Map<SequenceNumber, UploadResult> toDiscard = uploaded.headMap(to);
        notifyStateNotUsed(toDiscard);
        toDiscard.clear();
    }

    @Override
    public void truncateAndClose(SequenceNumber from) {
        LOG.debug("truncate {} tail from sqn {} (incl.)", logId, from);
        highestSequenceNumber = from;
        notifyStateNotUsed(uploaded.tailMap(from));
        close();
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
    public void confirm(SequenceNumber from, SequenceNumber to, long checkpointId) {
        checkState(from.compareTo(to) <= 0, "Invalid confirm range: [%s,%s)", from, to);
        checkState(
                from.compareTo(activeSequenceNumber) <= 0
                        && to.compareTo(activeSequenceNumber) <= 0,
                "Invalid confirm range: [%s,%s), active sqn: %s",
                from,
                to,
                activeSequenceNumber);
        // it is possible that "uploaded" has already been truncated (after checkpoint subsumption)
        // so do not check that "uploaded" contains the specified range
        LOG.debug("Confirm [{}, {})", from, to);
        uploaded.subMap(from, to).values().stream()
                .map(UploadResult::getStreamStateHandle)
                .forEach(changelogRegistry::stopTracking);

        // transfer the control of localHandle to localStateRegistry.
        uploaded.subMap(from, to).values().stream()
                .map(UploadResult::getLocalStreamHandleStateHandle)
                .filter(localHandle -> localHandle != null)
                .forEach(
                        localHandle -> {
                            changelogRegistry.stopTracking(localHandle);
                        });
        localChangelogRegistry.discardUpToCheckpoint(checkpointId);
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to, long checkpointId) {
        // delete all accumulated local dstl files when abort
        localChangelogRegistry.discardUpToCheckpoint(checkpointId + 1);
    }

    private SnapshotResult<ChangelogStateHandleStreamImpl> buildSnapshotResult(
            KeyGroupRange keyGroupRange,
            NavigableMap<SequenceNumber, UploadResult> results,
            long incrementalSize) {
        List<Tuple2<StreamStateHandle, Long>> tuples = new ArrayList<>();
        long size = 0;
        for (UploadResult uploadResult : results.values()) {
            tuples.add(Tuple2.of(uploadResult.getStreamStateHandle(), uploadResult.getOffset()));
            size += uploadResult.getSize();
        }
        ChangelogStateHandleStreamImpl jmChangelogStateHandle =
                new ChangelogStateHandleStreamImpl(
                        tuples,
                        keyGroupRange,
                        size,
                        incrementalSize,
                        FsStateChangelogStorageFactory.IDENTIFIER);
        if (localRecoveryConfig.isLocalRecoveryEnabled()) {
            size = 0;
            List<Tuple2<StreamStateHandle, Long>> localTuples = new ArrayList<>();
            for (UploadResult uploadResult : results.values()) {
                if (uploadResult.getLocalStreamHandleStateHandle() != null) {
                    localTuples.add(
                            Tuple2.of(
                                    uploadResult.getLocalStreamHandleStateHandle(),
                                    uploadResult.getLocalOffset()));
                    size += uploadResult.getSize();
                }
            }
            ChangelogStateHandleStreamImpl localChangelogStateHandle = null;
            if (localTuples.size() == tuples.size()) {
                localChangelogStateHandle =
                        new ChangelogStateHandleStreamImpl(
                                localTuples,
                                keyGroupRange,
                                size,
                                0L,
                                FsStateChangelogStorageFactory.IDENTIFIER);
                return SnapshotResult.withLocalState(
                        jmChangelogStateHandle, localChangelogStateHandle);

            } else {
                LOG.warn("local handles are different from remote");
            }
        }
        return SnapshotResult.of(jmChangelogStateHandle);
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

    private final class UploadCompletionListener {
        private final NavigableMap<SequenceNumber, UploadResult> uploaded;
        private final CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>>
                completionFuture;
        private final KeyGroupRange keyGroupRange;
        private final SequenceNumberRange changeRange;

        private UploadCompletionListener(
                KeyGroupRange keyGroupRange,
                SequenceNumberRange changeRange,
                Map<SequenceNumber, UploadResult> uploaded,
                CompletableFuture<SnapshotResult<ChangelogStateHandleStreamImpl>>
                        completionFuture) {
            checkArgument(
                    !changeRange.isEmpty(), "Empty change range not allowed: %s", changeRange);
            this.uploaded = new TreeMap<>(uploaded);
            this.completionFuture = completionFuture;
            this.keyGroupRange = keyGroupRange;
            this.changeRange = changeRange;
        }

        public boolean onSuccess(List<UploadResult> uploadResults) {
            long incrementalSize = 0L;
            for (UploadResult uploadResult : uploadResults) {
                if (changeRange.contains(uploadResult.sequenceNumber)) {
                    uploaded.put(uploadResult.sequenceNumber, uploadResult);
                    incrementalSize += uploadResult.getSize();
                    if (uploaded.size() == changeRange.size()) {
                        completionFuture.complete(
                                buildSnapshotResult(keyGroupRange, uploaded, incrementalSize));
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

    private void notifyStateNotUsed(Map<SequenceNumber, UploadResult> notUsedState) {
        LOG.trace("Uploaded state to discard: {}", notUsedState);
        for (UploadResult result : notUsedState.values()) {
            changelogRegistry.release(result.streamStateHandle);
            if (result.localStreamHandle != null) {
                changelogRegistry.release(result.localStreamHandle);
            }
        }
    }
}
