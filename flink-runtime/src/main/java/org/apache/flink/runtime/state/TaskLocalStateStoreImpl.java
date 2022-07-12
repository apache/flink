/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;

/** Main implementation of a {@link TaskLocalStateStore}. */
public class TaskLocalStateStoreImpl implements OwnedTaskLocalStateStore {

    /** Logger for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(TaskLocalStateStoreImpl.class);

    /** Dummy value to use instead of null to satisfy {@link ConcurrentHashMap}. */
    @VisibleForTesting static final TaskStateSnapshot NULL_DUMMY = new TaskStateSnapshot(0, false);

    public static final String TASK_STATE_SNAPSHOT_FILENAME = "_task_state_snapshot";

    /** JobID from the owning subtask. */
    @Nonnull private final JobID jobID;

    /** AllocationID of the owning slot. */
    @Nonnull private final AllocationID allocationID;

    /** JobVertexID of the owning subtask. */
    @Nonnull private final JobVertexID jobVertexID;

    /** Subtask index of the owning subtask. */
    @Nonnegative private final int subtaskIndex;

    /** The configured mode for local recovery. */
    @Nonnull private final LocalRecoveryConfig localRecoveryConfig;

    /** Executor that runs the discarding of released state objects. */
    @Nonnull private final Executor discardExecutor;

    /** Lock for synchronisation on the storage map and the discarded status. */
    @Nonnull private final Object lock = new Object();

    /** Status flag if this store was already discarded. */
    @GuardedBy("lock")
    private boolean disposed;

    /** Maps checkpoint ids to local TaskStateSnapshots. */
    @Nonnull
    @GuardedBy("lock")
    private final SortedMap<Long, TaskStateSnapshot> storedTaskStateByCheckpointID;

    public TaskLocalStateStoreImpl(
            @Nonnull JobID jobID,
            @Nonnull AllocationID allocationID,
            @Nonnull JobVertexID jobVertexID,
            @Nonnegative int subtaskIndex,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull Executor discardExecutor) {

        this.jobID = jobID;
        this.allocationID = allocationID;
        this.jobVertexID = jobVertexID;
        this.subtaskIndex = subtaskIndex;
        this.discardExecutor = discardExecutor;
        this.localRecoveryConfig = localRecoveryConfig;
        this.storedTaskStateByCheckpointID = new TreeMap<>();
        this.disposed = false;
    }

    @Override
    public void storeLocalState(
            @Nonnegative long checkpointId, @Nullable TaskStateSnapshot localState) {

        if (localState == null) {
            localState = NULL_DUMMY;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Stored local state for checkpoint {} in subtask ({} - {} - {}) : {}.",
                    checkpointId,
                    jobID,
                    jobVertexID,
                    subtaskIndex,
                    localState);
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Stored local state for checkpoint {} in subtask ({} - {} - {})",
                    checkpointId,
                    jobID,
                    jobVertexID,
                    subtaskIndex);
        }

        Map.Entry<Long, TaskStateSnapshot> toDiscard = null;

        synchronized (lock) {
            if (disposed) {
                // we ignore late stores and simply discard the state.
                toDiscard = new AbstractMap.SimpleEntry<>(checkpointId, localState);
            } else {
                TaskStateSnapshot previous =
                        storedTaskStateByCheckpointID.put(checkpointId, localState);
                persistLocalStateMetadata(checkpointId, localState);

                if (previous != null) {
                    toDiscard = new AbstractMap.SimpleEntry<>(checkpointId, previous);
                }
            }
        }

        if (toDiscard != null) {
            asyncDiscardLocalStateForCollection(Collections.singletonList(toDiscard));
        }
    }

    /**
     * Writes a task state snapshot file that contains the serialized content of the local state.
     *
     * @param checkpointId identifying the checkpoint
     * @param localState task state snapshot that will be persisted
     */
    private void persistLocalStateMetadata(long checkpointId, TaskStateSnapshot localState) {
        final File taskStateSnapshotFile = getTaskStateSnapshotFile(checkpointId);
        try (ObjectOutputStream oos =
                new ObjectOutputStream(new FileOutputStream(taskStateSnapshotFile))) {
            oos.writeObject(localState);

            LOG.debug(
                    "Successfully written local task state snapshot file {} for checkpoint {}.",
                    taskStateSnapshotFile,
                    checkpointId);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Could not write the local task state snapshot file.");
        }
    }

    @VisibleForTesting
    File getTaskStateSnapshotFile(long checkpointId) {
        final File checkpointDirectory =
                localRecoveryConfig
                        .getLocalStateDirectoryProvider()
                        .orElseThrow(
                                () -> new IllegalStateException("Local recovery must be enabled."))
                        .subtaskSpecificCheckpointDirectory(checkpointId);

        if (!checkpointDirectory.exists() && !checkpointDirectory.mkdirs()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not create the checkpoint directory '%s'", checkpointDirectory));
        }

        return new File(checkpointDirectory, TASK_STATE_SNAPSHOT_FILENAME);
    }

    @Override
    @Nullable
    public TaskStateSnapshot retrieveLocalState(long checkpointID) {

        TaskStateSnapshot snapshot;

        synchronized (lock) {
            snapshot = loadTaskStateSnapshot(checkpointID);
        }

        if (snapshot != null) {
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "Found registered local state for checkpoint {} in subtask ({} - {} - {}) : {}",
                        checkpointID,
                        jobID,
                        jobVertexID,
                        subtaskIndex,
                        snapshot);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Found registered local state for checkpoint {} in subtask ({} - {} - {})",
                        checkpointID,
                        jobID,
                        jobVertexID,
                        subtaskIndex);
            }
        } else {
            LOG.debug(
                    "Did not find registered local state for checkpoint {} in subtask ({} - {} - {})",
                    checkpointID,
                    jobID,
                    jobVertexID,
                    subtaskIndex);
        }

        return (snapshot != NULL_DUMMY) ? snapshot : null;
    }

    @GuardedBy("lock")
    @Nullable
    private TaskStateSnapshot loadTaskStateSnapshot(long checkpointID) {
        return storedTaskStateByCheckpointID.computeIfAbsent(
                checkpointID, this::tryLoadTaskStateSnapshotFromDisk);
    }

    @GuardedBy("lock")
    @Nullable
    private TaskStateSnapshot tryLoadTaskStateSnapshotFromDisk(long checkpointID) {
        final File taskStateSnapshotFile = getTaskStateSnapshotFile(checkpointID);

        if (taskStateSnapshotFile.exists()) {
            TaskStateSnapshot taskStateSnapshot = null;
            try (ObjectInputStream ois =
                    new ObjectInputStream(new FileInputStream(taskStateSnapshotFile))) {
                taskStateSnapshot = (TaskStateSnapshot) ois.readObject();

                LOG.debug(
                        "Loaded task state snapshot for checkpoint {} successfully from disk.",
                        checkpointID);
            } catch (IOException | ClassNotFoundException e) {
                LOG.debug(
                        "Could not read task state snapshot file {} for checkpoint {}. Deleting the corresponding local state.",
                        taskStateSnapshotFile,
                        checkpointID);

                discardLocalStateForCheckpoint(checkpointID, Optional.empty());
            }

            return taskStateSnapshot;
        }

        return null;
    }

    @Override
    @Nonnull
    public LocalRecoveryConfig getLocalRecoveryConfig() {
        return localRecoveryConfig;
    }

    @Override
    public void confirmCheckpoint(long confirmedCheckpointId) {

        LOG.debug(
                "Received confirmation for checkpoint {} in subtask ({} - {} - {}). Starting to prune history.",
                confirmedCheckpointId,
                jobID,
                jobVertexID,
                subtaskIndex);

        pruneCheckpoints(
                (snapshotCheckpointId) -> snapshotCheckpointId < confirmedCheckpointId, true);
    }

    @Override
    public void abortCheckpoint(long abortedCheckpointId) {

        LOG.debug(
                "Received abort information for checkpoint {} in subtask ({} - {} - {}). Starting to prune history.",
                abortedCheckpointId,
                jobID,
                jobVertexID,
                subtaskIndex);

        boolean success =
                pruneCheckpoints(
                        snapshotCheckpointId -> snapshotCheckpointId == abortedCheckpointId, false);
        if (!success) {
            discardExecutor.execute(() -> tryDeleteLocalStateDirectory(abortedCheckpointId));
        }
    }

    @Override
    public void pruneMatchingCheckpoints(@Nonnull LongPredicate matcher) {

        pruneCheckpoints(matcher, false);
    }

    /** Disposes the state of all local snapshots managed by this object. */
    @Override
    public CompletableFuture<Void> dispose() {

        Collection<Map.Entry<Long, TaskStateSnapshot>> statesCopy;

        synchronized (lock) {
            disposed = true;
            statesCopy = new ArrayList<>(storedTaskStateByCheckpointID.entrySet());
            storedTaskStateByCheckpointID.clear();
        }

        return CompletableFuture.runAsync(
                () -> {
                    // discard all remaining state objects.
                    syncDiscardLocalStateForCollection(statesCopy);

                    // delete the local state subdirectory that belong to this subtask.
                    LocalRecoveryDirectoryProvider directoryProvider =
                            localRecoveryConfig
                                    .getLocalStateDirectoryProvider()
                                    .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled());

                    for (int i = 0; i < directoryProvider.allocationBaseDirsCount(); ++i) {
                        File subtaskBaseDirectory = directoryProvider.selectSubtaskBaseDirectory(i);
                        try {
                            deleteDirectory(subtaskBaseDirectory);
                        } catch (IOException e) {
                            LOG.warn(
                                    "Exception when deleting local recovery subtask base directory {} in subtask ({} - {} - {})",
                                    subtaskBaseDirectory,
                                    jobID,
                                    jobVertexID,
                                    subtaskIndex,
                                    e);
                        }
                    }
                },
                discardExecutor);
    }

    private void asyncDiscardLocalStateForCollection(
            Collection<Map.Entry<Long, TaskStateSnapshot>> toDiscard) {
        if (!toDiscard.isEmpty()) {
            discardExecutor.execute(() -> syncDiscardLocalStateForCollection(toDiscard));
        }
    }

    private void syncDiscardLocalStateForCollection(
            Collection<Map.Entry<Long, TaskStateSnapshot>> toDiscard) {
        for (Map.Entry<Long, TaskStateSnapshot> entry : toDiscard) {
            discardLocalStateForCheckpoint(entry.getKey(), Optional.of(entry.getValue()));
        }
    }

    /**
     * Helper method that discards state objects with an executor and reports exceptions to the log.
     */
    private void discardLocalStateForCheckpoint(long checkpointID, Optional<TaskStateSnapshot> o) {

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "Discarding local task state snapshot of checkpoint {} for subtask ({} - {} - {}).",
                    checkpointID,
                    jobID,
                    jobVertexID,
                    subtaskIndex);
        } else {
            LOG.debug(
                    "Discarding local task state snapshot {} of checkpoint {} for subtask ({} - {} - {}).",
                    o,
                    checkpointID,
                    jobID,
                    jobVertexID,
                    subtaskIndex);
        }

        o.ifPresent(
                taskStateSnapshot -> {
                    try {
                        taskStateSnapshot.discardState();
                    } catch (Exception discardEx) {
                        LOG.warn(
                                "Exception while discarding local task state snapshot of checkpoint {} in subtask ({} - {} - {}).",
                                checkpointID,
                                jobID,
                                jobVertexID,
                                subtaskIndex,
                                discardEx);
                    }
                });
        tryDeleteLocalStateDirectory(checkpointID);
    }

    private void tryDeleteLocalStateDirectory(long checkpointID) {
        Optional<LocalRecoveryDirectoryProvider> directoryProviderOptional =
                localRecoveryConfig.getLocalStateDirectoryProvider();

        if (directoryProviderOptional.isPresent()) {
            File checkpointDir =
                    directoryProviderOptional
                            .get()
                            .subtaskSpecificCheckpointDirectory(checkpointID);

            LOG.debug(
                    "Deleting local state directory {} of checkpoint {} for subtask ({} - {} - {}).",
                    checkpointDir,
                    checkpointID,
                    jobID,
                    jobVertexID,
                    subtaskIndex);

            try {
                deleteDirectory(checkpointDir);
            } catch (IOException ex) {
                LOG.warn(
                        "Exception while deleting local state directory of checkpoint {} in subtask ({} - {} - {}).",
                        checkpointID,
                        jobID,
                        jobVertexID,
                        subtaskIndex,
                        ex);
            }
        }
    }

    /** Helper method to delete a directory. */
    private void deleteDirectory(File directory) throws IOException {
        Path path = new Path(directory.toURI());
        FileSystem fileSystem = path.getFileSystem();
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    /**
     * Pruning the useless checkpoints, it should be called only when holding the {@link #lock}.
     *
     * @param pruningChecker the checker which determine if a checkpoint needs to be pruned.
     * @param breakOnceCheckerFalse indicate whether break the loop once check result is false.
     * @return true if pruning checkpoints is success, false if no checkpoints is pruned.
     */
    private boolean pruneCheckpoints(LongPredicate pruningChecker, boolean breakOnceCheckerFalse) {

        final List<Map.Entry<Long, TaskStateSnapshot>> toRemove = new ArrayList<>();

        synchronized (lock) {
            Iterator<Map.Entry<Long, TaskStateSnapshot>> entryIterator =
                    storedTaskStateByCheckpointID.entrySet().iterator();

            while (entryIterator.hasNext()) {

                Map.Entry<Long, TaskStateSnapshot> snapshotEntry = entryIterator.next();
                long entryCheckpointId = snapshotEntry.getKey();

                if (pruningChecker.test(entryCheckpointId)) {
                    toRemove.add(snapshotEntry);
                    entryIterator.remove();
                } else if (breakOnceCheckerFalse) {
                    break;
                }
            }
        }

        asyncDiscardLocalStateForCollection(toRemove);
        return !toRemove.isEmpty();
    }

    @Override
    public String toString() {
        return "TaskLocalStateStore{"
                + "jobID="
                + jobID
                + ", jobVertexID="
                + jobVertexID
                + ", allocationID="
                + allocationID.toHexString()
                + ", subtaskIndex="
                + subtaskIndex
                + ", localRecoveryConfig="
                + localRecoveryConfig
                + ", storedCheckpointIDs="
                + storedTaskStateByCheckpointID.keySet()
                + '}';
    }
}
