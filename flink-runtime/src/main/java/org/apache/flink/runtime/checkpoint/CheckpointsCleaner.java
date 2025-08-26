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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Delegate class responsible for checkpoints cleaning and counting the number of checkpoints yet to
 * clean.
 */
@ThreadSafe
public class CheckpointsCleaner implements Serializable, AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointsCleaner.class);
    private static final long serialVersionUID = 2545865801947537790L;

    private final boolean parallelMode;
    private final Object lock = new Object();

    @GuardedBy("lock")
    private int numberOfCheckpointsToClean;

    @GuardedBy("lock")
    @Nullable
    private CompletableFuture<Void> cleanUpFuture;

    /** All subsumed checkpoints. */
    @GuardedBy("lock")
    private final List<CompletedCheckpoint> subsumedCheckpoints = new ArrayList<>();

    public CheckpointsCleaner() {
        this.parallelMode = CheckpointingOptions.CLEANER_PARALLEL_MODE.defaultValue();
    }

    public CheckpointsCleaner(boolean parallelMode) {
        this.parallelMode = parallelMode;
    }

    int getNumberOfCheckpointsToClean() {
        synchronized (lock) {
            return numberOfCheckpointsToClean;
        }
    }

    public void cleanCheckpoint(
            Checkpoint checkpoint,
            boolean shouldDiscard,
            Runnable postCleanAction,
            Executor executor) {
        LOG.debug(
                "Clean checkpoint {} parallel-mode={} shouldDiscard={}",
                checkpoint.getCheckpointID(),
                parallelMode,
                shouldDiscard);
        if (shouldDiscard) {
            incrementNumberOfCheckpointsToClean();

            Checkpoint.DiscardObject discardObject = checkpoint.markAsDiscarded();
            CompletableFuture<Void> discardFuture =
                    parallelMode
                            ? discardObject.discardAsync(executor)
                            : FutureUtils.runAsync(discardObject::discard, executor);
            discardFuture.handle(
                    (Object outerIgnored, Throwable outerThrowable) -> {
                        if (outerThrowable != null) {
                            LOG.warn(
                                    "Could not properly discard completed checkpoint {}.",
                                    checkpoint.getCheckpointID(),
                                    outerThrowable);
                        }

                        decrementNumberOfCheckpointsToClean();
                        postCleanAction.run();
                        return null;
                    });
        } else {
            executor.execute(postCleanAction);
        }
    }

    /**
     * Add one subsumed checkpoint to CheckpointsCleaner, the subsumed checkpoint would be discarded
     * at {@link #cleanSubsumedCheckpoints(long, Set, Runnable, Executor)}.
     *
     * @param completedCheckpoint which is subsumed.
     */
    public void addSubsumedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        synchronized (lock) {
            subsumedCheckpoints.add(completedCheckpoint);
        }
    }

    /**
     * Clean checkpoint that is not in the given {@param stillInUse}.
     *
     * @param upTo lowest CheckpointID which is still valid.
     * @param stillInUse the state of those checkpoints are still referenced.
     * @param postCleanAction post action after cleaning.
     * @param executor is used to perform the cleanup logic.
     */
    public void cleanSubsumedCheckpoints(
            long upTo, Set<Long> stillInUse, Runnable postCleanAction, Executor executor) {
        synchronized (lock) {
            Iterator<CompletedCheckpoint> iterator = subsumedCheckpoints.iterator();
            while (iterator.hasNext()) {
                CompletedCheckpoint checkpoint = iterator.next();
                if (checkpoint.getCheckpointID() < upTo
                        && !stillInUse.contains(checkpoint.getCheckpointID())) {
                    try {
                        LOG.debug("Try to discard checkpoint {}.", checkpoint.getCheckpointID());
                        cleanCheckpoint(
                                checkpoint,
                                checkpoint.shouldBeDiscardedOnSubsume(),
                                postCleanAction,
                                executor);
                        iterator.remove();
                    } catch (Exception e) {
                        LOG.warn("Fail to discard the old checkpoint {}.", checkpoint);
                    }
                }
            }
        }
    }

    public void cleanCheckpointOnFailedStoring(
            CompletedCheckpoint completedCheckpoint, Executor executor) {
        cleanCheckpoint(completedCheckpoint, true, () -> {}, executor);
    }

    private void incrementNumberOfCheckpointsToClean() {
        synchronized (lock) {
            checkState(cleanUpFuture == null, "CheckpointsCleaner has already been closed");
            numberOfCheckpointsToClean++;
        }
    }

    private void decrementNumberOfCheckpointsToClean() {
        synchronized (lock) {
            numberOfCheckpointsToClean--;
            maybeCompleteCloseUnsafe();
        }
    }

    private void maybeCompleteCloseUnsafe() {
        if (numberOfCheckpointsToClean == 0 && cleanUpFuture != null) {
            cleanUpFuture.complete(null);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (cleanUpFuture == null) {
                cleanUpFuture = new CompletableFuture<>();
            }
            maybeCompleteCloseUnsafe();
            subsumedCheckpoints.clear();
            return cleanUpFuture;
        }
    }
}
