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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link CompletedCheckpointStore}. Combined with different {@link
 * org.apache.flink.runtime.persistence.StateHandleStore}, we could persist the completed
 * checkpoints to various storage.
 *
 * <p>During recovery, the latest checkpoint is read from {@link StateHandleStore}. If there is more
 * than one, only the latest one is used and older ones are discarded (even if the maximum number of
 * retained checkpoints is greater than one).
 *
 * <p>If there is a network partition and multiple JobManagers run concurrent checkpoints for the
 * same program, it is OK to take any valid successful checkpoint as long as the "history" of
 * checkpoints is consistent. Currently, after recovery we start out with only a single checkpoint
 * to circumvent those situations.
 */
public class DefaultCompletedCheckpointStore<R extends ResourceVersion<R>>
        implements CompletedCheckpointStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultCompletedCheckpointStore.class);

    /** Completed checkpoints state handle store. */
    private final StateHandleStore<CompletedCheckpoint, R> checkpointStateHandleStore;

    /** The maximum number of checkpoints to retain (at least 1). */
    private final int maxNumberOfCheckpointsToRetain;

    /**
     * Local copy of the completed checkpoints in state handle store. This is restored from state
     * handle store when recovering and is maintained in parallel to the state in state handle store
     * during normal operations.
     */
    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    private final Executor ioExecutor;

    private final CheckpointStoreUtil completedCheckpointStoreUtil;

    /** False if store has been shutdown. */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * Creates a {@link DefaultCompletedCheckpointStore} instance.
     *
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at least
     *     1). Adding more checkpoints than this results in older checkpoints being discarded. On
     *     recovery, we will only start with a single checkpoint.
     * @param stateHandleStore Completed checkpoints in external store
     * @param completedCheckpointStoreUtil utilities for completed checkpoint store
     * @param executor to execute blocking calls
     */
    public DefaultCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain,
            StateHandleStore<CompletedCheckpoint, R> stateHandleStore,
            CheckpointStoreUtil completedCheckpointStoreUtil,
            Collection<CompletedCheckpoint> completedCheckpoints,
            Executor executor) {
        checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
        this.checkpointStateHandleStore = checkNotNull(stateHandleStore);
        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
        this.completedCheckpoints.addAll(completedCheckpoints);
        this.ioExecutor = checkNotNull(executor);
        this.completedCheckpointStoreUtil = checkNotNull(completedCheckpointStoreUtil);
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return true;
    }

    /**
     * Synchronously writes the new checkpoints to state handle store and asynchronously removes
     * older ones.
     *
     * @param checkpoint Completed checkpoint to add.
     * @throws PossibleInconsistentStateException if adding the checkpoint failed and leaving the
     *     system in a possibly inconsistent state, i.e. it's uncertain whether the checkpoint
     *     metadata was fully written to the underlying systems or not.
     */
    @Override
    public void addCheckpoint(
            final CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        Preconditions.checkState(running.get(), "Checkpoint store has already been shutdown.");
        checkNotNull(checkpoint, "Checkpoint");

        final String path =
                completedCheckpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());

        // Now add the new one. If it fails, we don't want to lose existing data.
        checkpointStateHandleStore.addAndLock(path, checkpoint);

        completedCheckpoints.addLast(checkpoint);

        CheckpointSubsumeHelper.subsume(
                completedCheckpoints,
                maxNumberOfCheckpointsToRetain,
                completedCheckpoint ->
                        tryRemoveCompletedCheckpoint(
                                completedCheckpoint,
                                completedCheckpoint.shouldBeDiscardedOnSubsume(),
                                checkpointsCleaner,
                                postCleanup));

        LOG.debug("Added {} to {}.", checkpoint, path);
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() {
        return new ArrayList<>(completedCheckpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfCheckpointsToRetain;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        if (running.compareAndSet(true, false)) {
            if (jobStatus.isGloballyTerminalState()) {
                LOG.info("Shutting down");
                for (CompletedCheckpoint checkpoint : completedCheckpoints) {
                    try {
                        tryRemoveCompletedCheckpoint(
                                checkpoint,
                                checkpoint.shouldBeDiscardedOnShutdown(jobStatus),
                                checkpointsCleaner,
                                () -> {});
                    } catch (Exception e) {
                        LOG.warn("Fail to remove checkpoint during shutdown.", e);
                    }
                }
                completedCheckpoints.clear();
                checkpointStateHandleStore.clearEntries();
            } else {
                LOG.info("Suspending");
                // Clear the local handles, but don't remove any state
                completedCheckpoints.clear();
                checkpointStateHandleStore.releaseAll();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Private methods
    // ---------------------------------------------------------------------------------------------------------

    private void tryRemoveCompletedCheckpoint(
            CompletedCheckpoint completedCheckpoint,
            boolean shouldDiscard,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        if (tryRemove(completedCheckpoint.getCheckpointID())) {
            checkpointsCleaner.cleanCheckpoint(
                    completedCheckpoint, shouldDiscard, postCleanup, ioExecutor);
        }
    }

    /**
     * Tries to remove the checkpoint identified by the given checkpoint id.
     *
     * @param checkpointId identifying the checkpoint to remove
     * @return true if the checkpoint could be removed
     */
    private boolean tryRemove(long checkpointId) throws Exception {
        return checkpointStateHandleStore.releaseAndTryRemove(
                completedCheckpointStoreUtil.checkpointIDToName(checkpointId));
    }
}
