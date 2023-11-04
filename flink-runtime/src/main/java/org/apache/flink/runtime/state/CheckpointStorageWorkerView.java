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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;

import java.io.IOException;

/**
 * This interface implements the durable storage of checkpoint data and metadata streams. An
 * individual checkpoint or savepoint is stored to a {@link CheckpointStorageLocation} which created
 * by {@link CheckpointStorageCoordinatorView}.
 *
 * <p>Methods of this interface act as a worker role in task manager.
 */
@Internal
public interface CheckpointStorageWorkerView {

    /**
     * Resolves a storage location reference into a CheckpointStreamFactory.
     *
     * <p>The reference may be the {@link CheckpointStorageLocationReference#isDefaultReference()
     * default reference}, in which case the method should return the default location, taking
     * existing configuration and checkpoint ID into account.
     *
     * @param checkpointId The ID of the checkpoint that the location is initialized for.
     * @param reference The checkpoint location reference.
     * @return A checkpoint storage location reflecting the reference and checkpoint ID.
     * @throws IOException Thrown, if the storage location cannot be initialized from the reference.
     */
    CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException;

    /**
     * Opens a stream to persist checkpoint state data that is owned strictly by tasks and not
     * attached to the life cycle of a specific checkpoint.
     *
     * <p>This method should be used when the persisted data cannot be immediately dropped once the
     * checkpoint that created it is dropped. Examples are write-ahead-logs. For those, the state
     * can only be dropped once the data has been moved to the target system, which may sometimes
     * take longer than one checkpoint (if the target system is temporarily unable to keep up).
     *
     * <p>The fact that the job manager does not own the life cycle of this type of state means also
     * that it is strictly the responsibility of the tasks to handle the cleanup of this data.
     *
     * <p>Developer note: In the future, we may be able to make this a special case of "shared
     * state", where the task re-emits the shared state reference as long as it needs to hold onto
     * the persisted state data.
     *
     * @return A checkpoint state stream to the location for state owned by tasks.
     * @throws IOException Thrown, if the stream cannot be opened.
     */
    CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException;

    /**
     * A complementary method to {@link #createTaskOwnedStateStream()}. Creates a toolset that gives
     * access to additional operations that can be performed in the task owned state location.
     *
     * @return A toolset for additional operations for state owned by tasks.
     */
    CheckpointStateToolset createTaskOwnedCheckpointStateToolset();

    /**
     * Return {@link org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageAccess} if
     * file merging is enabled Otherwise, return itself. File merging is supported by subclasses of
     * {@link org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess}.
     */
    default CheckpointStorageWorkerView toFileMergingStorage(
            FileMergingSnapshotManager mergingSnapshotManager, Environment environment)
            throws IOException {
        return this;
    }
}
