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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.io.IOException;

/** An implementation of file merging checkpoint storage to file systems. */
public class FsMergingCheckpointStorageAccess extends FsCheckpointStorageAccess {

    /** FileMergingSnapshotManager manages files and meta information for checkpoints. */
    private final FileMergingSnapshotManager fileMergingSnapshotManager;

    /** The identity of subtask. */
    private final FileMergingSnapshotManager.SubtaskKey subtaskKey;

    public FsMergingCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize,
            FileMergingSnapshotManager fileMergingSnapshotManager,
            Environment environment)
            throws IOException {
        super(
                fs,
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
        this.fileMergingSnapshotManager = fileMergingSnapshotManager;
        this.subtaskKey =
                new SubtaskKey(
                        OperatorID.fromJobVertexID(environment.getJobVertexId()),
                        environment.getTaskInfo());
    }

    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
        super.initializeBaseLocationsForCheckpoint();
        fileMergingSnapshotManager.initFileSystem(
                fileSystem, checkpointsDirectory, sharedStateDirectory, taskOwnedStateDirectory);
        fileMergingSnapshotManager.registerSubtaskForSharedStates(subtaskKey);
    }
}
