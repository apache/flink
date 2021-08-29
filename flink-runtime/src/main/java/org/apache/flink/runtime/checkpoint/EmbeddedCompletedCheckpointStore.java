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
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** An embedded in-memory checkpoint store, which supports shutdown and suspend. */
public class EmbeddedCompletedCheckpointStore implements CompletedCheckpointStore {

    private static void throwAlreadyShutdownException(JobStatus status) {
        throw new IllegalStateException(
                String.format("Store has been already shutdown with %s.", status));
    }

    private final ArrayDeque<CompletedCheckpoint> checkpoints = new ArrayDeque<>(2);

    private final AtomicReference<JobStatus> shutdownStatus = new AtomicReference<>();

    private final int maxRetainedCheckpoints;

    public EmbeddedCompletedCheckpointStore() {
        this(1);
    }

    public EmbeddedCompletedCheckpointStore(int maxRetainedCheckpoints) {
        this(maxRetainedCheckpoints, Collections.emptyList());
    }

    public EmbeddedCompletedCheckpointStore(
            int maxRetainedCheckpoints, Collection<CompletedCheckpoint> initialCheckpoints) {
        Preconditions.checkArgument(maxRetainedCheckpoints > 0);
        this.maxRetainedCheckpoints = maxRetainedCheckpoints;
        this.checkpoints.addAll(initialCheckpoints);
    }

    @Override
    public void addCheckpoint(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        if (shutdownStatus.get() != null) {
            throwAlreadyShutdownException(shutdownStatus.get());
        }
        checkpoints.addLast(checkpoint);
        CheckpointSubsumeHelper.subsume(
                checkpoints, maxRetainedCheckpoints, CompletedCheckpoint::discardOnSubsume);
    }

    @VisibleForTesting
    void removeOldestCheckpoint() throws Exception {
        CompletedCheckpoint checkpointToSubsume = checkpoints.removeFirst();
        checkpointToSubsume.discardOnSubsume();
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        if (shutdownStatus.compareAndSet(null, jobStatus)) {
            if (jobStatus.isGloballyTerminalState()) {
                // We are done with this store. We should leave no checkpoints for recovery.
                checkpoints.clear();
            }
        } else {
            throwAlreadyShutdownException(shutdownStatus.get());
        }
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
        return maxRetainedCheckpoints;
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return false;
    }

    @VisibleForTesting
    public Optional<JobStatus> getShutdownStatus() {
        return Optional.ofNullable(shutdownStatus.get());
    }
}
