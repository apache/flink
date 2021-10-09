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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Test {@link CompletedCheckpointStore} implementation for testing the shutdown behavior. */
public final class TestingCompletedCheckpointStore implements CompletedCheckpointStore {

    private final CompletableFuture<JobStatus> shutdownStatus;

    public TestingCompletedCheckpointStore(CompletableFuture<JobStatus> shutdownStatus) {
        this.shutdownStatus = shutdownStatus;
    }

    @Override
    public void addCheckpoint(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public CompletedCheckpoint getLatestCheckpoint() {
        return null;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner) {
        shutdownStatus.complete(jobStatus);
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() {
        return Collections.emptyList();
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
