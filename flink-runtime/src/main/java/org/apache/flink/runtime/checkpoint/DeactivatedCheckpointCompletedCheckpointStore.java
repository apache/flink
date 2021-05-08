/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.List;

/**
 * This class represents a {@link CompletedCheckpointStore} if checkpointing has been enabled.
 * Consequently, no component should use methods other than {@link
 * CompletedCheckpointStore#shutdown}.
 */
public enum DeactivatedCheckpointCompletedCheckpointStore implements CompletedCheckpointStore {
    INSTANCE;

    @Override
    public void recover() throws Exception {
        throw unsupportedOperationException();
    }

    @Override
    public void addCheckpoint(
            CompletedCheckpoint checkpoint,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup)
            throws Exception {
        throw unsupportedOperationException();
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {}

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        throw unsupportedOperationException();
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        throw unsupportedOperationException();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        throw unsupportedOperationException();
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        throw unsupportedOperationException();
    }

    private UnsupportedOperationException unsupportedOperationException() {
        return new UnsupportedOperationException(
                String.format(
                        "The %s cannot store completed checkpoints.", getClass().getSimpleName()));
    }
}
