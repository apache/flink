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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.DirectoryStreamStateHandle;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/** An implementation of checkpoint storage location for file merging checkpoints. */
public class FsMergingCheckpointStorageLocation extends FsCheckpointStorageLocation {

    /** ID of the subtask that performs the checkpoint. */
    private final FileMergingSnapshotManager.SubtaskKey subtaskKey;

    /** The delegate for creating the checkpoint output streams. */
    private final FileMergingSnapshotManager fileMergingSnapshotManager;

    /** ID of the checkpoint. */
    private final long checkpointId;

    /** A supplier for converting this location back into a non-fileMerging version. */
    private final Supplier<FsCheckpointStorageLocation> backwardsConvertor;

    public FsMergingCheckpointStorageLocation(
            FileMergingSnapshotManager.SubtaskKey subtaskKey,
            FileSystem fileSystem,
            Path checkpointDir,
            Path sharedStateDir,
            Path taskOwnedStateDir,
            CheckpointStorageLocationReference reference,
            int fileStateSizeThreshold,
            int writeBufferSize,
            FileMergingSnapshotManager fileMergingSnapshotManager,
            long checkpointId) {
        super(
                fileSystem,
                checkpointDir,
                sharedStateDir,
                taskOwnedStateDir,
                reference,
                fileStateSizeThreshold,
                writeBufferSize);

        this.subtaskKey = subtaskKey;
        this.checkpointId = checkpointId;
        this.fileMergingSnapshotManager = fileMergingSnapshotManager;
        this.backwardsConvertor =
                () ->
                        new FsCheckpointStorageLocation(
                                fileSystem,
                                checkpointDir,
                                sharedStateDir,
                                taskOwnedStateDir,
                                reference,
                                fileStateSizeThreshold,
                                writeBufferSize);
    }

    public CheckpointStreamFactory toNonFileMerging() {
        return backwardsConvertor.get();
    }

    public DirectoryStreamStateHandle getExclusiveStateHandle() {
        return fileMergingSnapshotManager.getManagedDirStateHandle(
                subtaskKey, CheckpointedStateScope.EXCLUSIVE);
    }

    public DirectoryStreamStateHandle getSharedStateHandle() {
        return fileMergingSnapshotManager.getManagedDirStateHandle(
                subtaskKey, CheckpointedStateScope.SHARED);
    }

    @Override
    public boolean canFastDuplicate(StreamStateHandle stateHandle, CheckpointedStateScope scope)
            throws IOException {
        return false;
    }

    @Override
    public List<StreamStateHandle> duplicate(
            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) throws IOException {
        return null;
    }

    @Override
    public FileMergingCheckpointStateOutputStream createCheckpointStateOutputStream(
            CheckpointedStateScope scope) throws IOException {
        return fileMergingSnapshotManager.createCheckpointStateOutputStream(
                subtaskKey, checkpointId, scope);
    }
}
