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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

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

    private static final Comparator<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>>
            STRING_COMPARATOR = Comparator.comparing(o -> o.f1);

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
            Executor executor) {

        checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");

        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

        this.checkpointStateHandleStore = checkNotNull(stateHandleStore);

        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

        this.ioExecutor = checkNotNull(executor);

        this.completedCheckpointStoreUtil = checkNotNull(completedCheckpointStoreUtil);
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return true;
    }

    /**
     * Recover all the valid checkpoints from state handle store. All the successfully recovered
     * checkpoints will be added to {@link #completedCheckpoints} sorted by checkpoint id.
     */
    @Override
    public void recover() throws Exception {
        LOG.info("Recovering checkpoints from {}.", checkpointStateHandleStore);

        // Get all there is first
        final List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> initialCheckpoints =
                checkpointStateHandleStore.getAllAndLock();

        initialCheckpoints.sort(STRING_COMPARATOR);

        final int numberOfInitialCheckpoints = initialCheckpoints.size();

        LOG.info(
                "Found {} checkpoints in {}.",
                numberOfInitialCheckpoints,
                checkpointStateHandleStore);
        if (haveAllDownloaded(initialCheckpoints)) {
            LOG.info(
                    "All {} checkpoints found are already downloaded.", numberOfInitialCheckpoints);
            return;
        }

        // Try and read the state handles from storage. We try until we either successfully read
        // all of them or when we reach a stable state, i.e. when we successfully read the same set
        // of checkpoints in two tries. We do it like this to protect against transient outages
        // of the checkpoint store (for example a DFS): if the DFS comes online midway through
        // reading a set of checkpoints we would run the risk of reading only a partial set
        // of checkpoints while we could in fact read the other checkpoints as well if we retried.
        // Waiting until a stable state protects against this while also being resilient against
        // checkpoints being actually unreadable.
        //
        // These considerations are also important in the scope of incremental checkpoints, where
        // we use ref-counting for shared state handles and might accidentally delete shared state
        // of checkpoints that we don't read due to transient storage outages.
        final List<CompletedCheckpoint> lastTryRetrievedCheckpoints =
                new ArrayList<>(numberOfInitialCheckpoints);
        final List<CompletedCheckpoint> retrievedCheckpoints =
                new ArrayList<>(numberOfInitialCheckpoints);
        Exception retrieveException = null;
        do {
            LOG.info("Trying to fetch {} checkpoints from storage.", numberOfInitialCheckpoints);

            lastTryRetrievedCheckpoints.clear();
            lastTryRetrievedCheckpoints.addAll(retrievedCheckpoints);

            retrievedCheckpoints.clear();

            for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpointStateHandle :
                    initialCheckpoints) {

                CompletedCheckpoint completedCheckpoint;

                try {
                    completedCheckpoint = retrieveCompletedCheckpoint(checkpointStateHandle);
                    if (completedCheckpoint != null) {
                        retrievedCheckpoints.add(completedCheckpoint);
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Could not retrieve checkpoint, not adding to list of recovered checkpoints.",
                            e);
                    retrieveException = e;
                }
            }

        } while (retrievedCheckpoints.size() != numberOfInitialCheckpoints
                && !CompletedCheckpoint.checkpointsMatch(
                        lastTryRetrievedCheckpoints, retrievedCheckpoints));

        // Clear local handles in order to prevent duplicates on recovery. The local handles should
        // reflect
        // the state handle store contents.
        completedCheckpoints.clear();
        completedCheckpoints.addAll(retrievedCheckpoints);

        if (completedCheckpoints.isEmpty() && numberOfInitialCheckpoints > 0) {
            throw new FlinkException(
                    "Could not read any of the "
                            + numberOfInitialCheckpoints
                            + " checkpoints from storage.",
                    retrieveException);
        } else if (completedCheckpoints.size() != numberOfInitialCheckpoints) {
            LOG.warn(
                    "Could only fetch {} of {} checkpoints from storage.",
                    completedCheckpoints.size(),
                    numberOfInitialCheckpoints);
        }
    }

    /**
     * Synchronously writes the new checkpoints to state handle store and asynchronously removes
     * older ones.
     *
     * @param checkpoint Completed checkpoint to add.
     */
    @Override
    public void addCheckpoint(
            final CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {

        checkNotNull(checkpoint, "Checkpoint");

        final String path =
                completedCheckpointStoreUtil.checkpointIDToName(checkpoint.getCheckpointID());

        // Now add the new one. If it fails, we don't want to loose existing data.
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

    private boolean haveAllDownloaded(
            List<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> checkpointPointers) {
        if (completedCheckpoints.size() != checkpointPointers.size()) {
            return false;
        }
        Set<Long> localIds =
                completedCheckpoints.stream()
                        .map(CompletedCheckpoint::getCheckpointID)
                        .collect(Collectors.toSet());
        for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> initialCheckpoint :
                checkpointPointers) {
            if (!localIds.contains(
                    completedCheckpointStoreUtil.nameToCheckpointID(initialCheckpoint.f1))) {
                return false;
            }
        }
        return true;
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

    private CompletedCheckpoint retrieveCompletedCheckpoint(
            Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandle)
            throws FlinkException {
        long checkpointId = completedCheckpointStoreUtil.nameToCheckpointID(stateHandle.f1);

        LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

        try {
            return stateHandle.f0.retrieveState();
        } catch (ClassNotFoundException cnfe) {
            throw new FlinkException(
                    "Could not retrieve checkpoint "
                            + checkpointId
                            + " from state handle under "
                            + stateHandle.f1
                            + ". This indicates that you are trying to recover from state written by an "
                            + "older Flink version which is not compatible. Try cleaning the state handle store.",
                    cnfe);
        } catch (IOException ioe) {
            throw new FlinkException(
                    "Could not retrieve checkpoint "
                            + checkpointId
                            + " from state handle under "
                            + stateHandle.f1
                            + ". This indicates that the retrieved state handle is broken. Try cleaning the "
                            + "state handle store.",
                    ioe);
        }
    }
}
