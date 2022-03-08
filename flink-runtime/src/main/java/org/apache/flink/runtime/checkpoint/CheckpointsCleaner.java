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

import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
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

    @GuardedBy("this")
    private int numberOfCheckpointsToClean;

    @GuardedBy("this")
    @Nullable
    private CompletableFuture<Void> cleanUpFuture;

    synchronized int getNumberOfCheckpointsToClean() {
        return numberOfCheckpointsToClean;
    }

    public void cleanCheckpoint(
            Checkpoint checkpoint,
            boolean shouldDiscard,
            Runnable postCleanAction,
            Executor executor) {
        Checkpoint.DiscardObject discardObject =
                shouldDiscard ? checkpoint.markAsDiscarded() : Checkpoint.NOOP_DISCARD_OBJECT;

        cleanup(checkpoint, discardObject::discard, postCleanAction, executor);
    }

    public void cleanCheckpointOnFailedStoring(
            CompletedCheckpoint completedCheckpoint, Executor executor) {
        Checkpoint.DiscardObject discardObject = completedCheckpoint.markAsDiscarded();
        cleanup(completedCheckpoint, discardObject::discard, () -> {}, executor);
    }

    private void cleanup(
            Checkpoint checkpoint,
            RunnableWithException cleanupAction,
            Runnable postCleanupAction,
            Executor executor) {
        incrementNumberOfCheckpointsToClean();
        executor.execute(
                () -> {
                    try {
                        cleanupAction.run();
                    } catch (Exception e) {
                        LOG.warn(
                                "Could not properly discard completed checkpoint {}.",
                                checkpoint.getCheckpointID(),
                                e);
                    } finally {
                        decrementNumberOfCheckpointsToClean();
                        postCleanupAction.run();
                    }
                });
    }

    private synchronized void incrementNumberOfCheckpointsToClean() {
        checkState(cleanUpFuture == null, "CheckpointsCleaner has already been closed");
        numberOfCheckpointsToClean++;
    }

    private synchronized void decrementNumberOfCheckpointsToClean() {
        numberOfCheckpointsToClean--;
        maybeCompleteCloseUnsafe();
    }

    private void maybeCompleteCloseUnsafe() {
        if (numberOfCheckpointsToClean == 0 && cleanUpFuture != null) {
            cleanUpFuture.complete(null);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (cleanUpFuture == null) {
            cleanUpFuture = new CompletableFuture<>();
        }
        maybeCompleteCloseUnsafe();
        return cleanUpFuture;
    }
}
