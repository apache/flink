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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#NONE}.
 */
public class StandaloneCompletedCheckpointStore extends AbstractCompleteCheckpointStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(StandaloneCompletedCheckpointStore.class);

    /** The maximum number of checkpoints to retain (at least 1). */
    private final int maxNumberOfCheckpointsToRetain;

    /** The completed checkpoints. */
    private final ArrayDeque<CompletedCheckpoint> checkpoints;

    private final Executor ioExecutor;

    @VisibleForTesting
    public StandaloneCompletedCheckpointStore(int maxNumberOfCheckpointsToRetain) {
        this(
                maxNumberOfCheckpointsToRetain,
                SharedStateRegistry.DEFAULT_FACTORY,
                Executors.directExecutor(),
                /* Using the default restore mode in tests to detect any breaking changes early. */
                RestoreMode.DEFAULT);
    }

    /**
     * Creates {@link StandaloneCompletedCheckpointStore}.
     *
     * @param restoreMode
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at least
     *     1). Adding more checkpoints than this results in older checkpoints being discarded.
     */
    public StandaloneCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RestoreMode restoreMode) {
        this(
                maxNumberOfCheckpointsToRetain,
                sharedStateRegistryFactory,
                new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1),
                ioExecutor,
                restoreMode);
    }

    private StandaloneCompletedCheckpointStore(
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            ArrayDeque<CompletedCheckpoint> checkpoints,
            Executor ioExecutor,
            RestoreMode restoreMode) {
        super(sharedStateRegistryFactory.create(ioExecutor, checkpoints, restoreMode));
        checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
        this.checkpoints = checkpoints;
        this.ioExecutor = checkNotNull(ioExecutor);
    }

    @Nullable
    @Override
    public CompletedCheckpoint addCheckpointAndSubsumeOldestOne(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {

        checkpoints.addLast(checkpoint);

        CompletedCheckpoint completedCheckpoint =
                CheckpointSubsumeHelper.subsume(
                                checkpoints,
                                maxNumberOfCheckpointsToRetain,
                                (cc) -> {
                                    cc.markAsDiscardedOnSubsume();
                                    checkpointsCleaner.addSubsumedCheckpoint(cc);
                                })
                        .orElse(null);

        findLowest(checkpoints)
                .ifPresent(
                        id ->
                                checkpointsCleaner.cleanSubsumedCheckpoints(
                                        id,
                                        getSharedStateRegistry().unregisterUnusedState(id),
                                        postCleanup,
                                        ioExecutor));

        return completedCheckpoint;
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() {
        return new ArrayList<>(checkpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return checkpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfCheckpointsToRetain;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        super.shutdown(jobStatus, checkpointsCleaner);
        try {
            LOG.info("Shutting down");

            long lowestRetained = Long.MAX_VALUE;
            for (CompletedCheckpoint checkpoint : checkpoints) {
                if (checkpoint.shouldBeDiscardedOnShutdown(jobStatus)) {
                    checkpoint.markAsDiscardedOnShutdown(jobStatus).discard();
                } else {
                    LOG.info(
                            "Checkpoint with ID {} at '{}' not discarded.",
                            checkpoint.getCheckpointID(),
                            checkpoint.getExternalPointer());
                    lowestRetained = Math.min(checkpoint.getCheckpointID(), lowestRetained);
                }
            }
            if (jobStatus.isGloballyTerminalState()) {
                // Now discard the shared state of not subsumed checkpoints - only if:
                // - the job is in a globally terminal state. Otherwise,
                // it can be a suspension, after which this state might still be needed.
                // - checkpoint is not retained (it might be used externally)
                // - checkpoint handle removal succeeded (e.g. from ZK) - otherwise, it might still
                // be used in recovery if the job status is lost
                checkpointsCleaner.cleanSubsumedCheckpoints(
                        lowestRetained,
                        getSharedStateRegistry().unregisterUnusedState(lowestRetained),
                        () -> {},
                        ioExecutor);
            }

        } finally {
            checkpoints.clear();
        }
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return false;
    }
}
