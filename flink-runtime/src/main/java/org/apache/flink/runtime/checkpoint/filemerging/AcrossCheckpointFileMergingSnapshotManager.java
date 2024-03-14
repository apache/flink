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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.Executor;

/** A {@link FileMergingSnapshotManager} that merging files across checkpoints. */
public class AcrossCheckpointFileMergingSnapshotManager extends FileMergingSnapshotManagerBase {

    private final PhysicalFilePool filePool;

    public AcrossCheckpointFileMergingSnapshotManager(
            String id, long maxFileSize, PhysicalFilePool.Type filePoolType, Executor ioExecutor) {
        super(id, maxFileSize, filePoolType, ioExecutor);
        filePool = createPhysicalPool();
    }

    @Override
    @Nonnull
    protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointID, CheckpointedStateScope scope)
            throws IOException {
        return filePool.pollFile(subtaskKey, scope);
    }

    @Override
    protected void discardCheckpoint(long checkpointId) {}

    @Override
    protected void returnPhysicalFileForNextReuse(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile)
            throws IOException {

        if (shouldSyncAfterClosingLogicalFile) {
            FSDataOutputStream os = physicalFile.getOutputStream();
            if (os != null) {
                os.sync();
            }
        }

        if (!filePool.tryPutFile(subtaskKey, physicalFile)) {
            physicalFile.close();
        }
    }
}
