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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/** A {@link FileMergingSnapshotManager} that merging files within a checkpoint. */
public class WithinCheckpointFileMergingSnapshotManager extends FileMergingSnapshotManagerBase {

    /**
     * For WITHIN_BOUNDARY mode, physical files are NOT shared among multiple checkpoints. This map
     * contains all physical files that are still writable and not occupied by a writer. The key of
     * this map is checkpoint id, which collectively determine the physical file pool to be reused.
     */
    private final Map<Long, PhysicalFilePool> writablePhysicalFilePool;

    public WithinCheckpointFileMergingSnapshotManager(
            String id, long maxFileSize, PhysicalFilePool.Type filePoolType, Executor ioExecutor) {
        // currently there is no file size limit For WITHIN_BOUNDARY mode
        super(id, maxFileSize, filePoolType, ioExecutor);
        writablePhysicalFilePool = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  CheckpointListener
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(SubtaskKey subtaskKey, long checkpointId)
            throws Exception {
        super.notifyCheckpointComplete(subtaskKey, checkpointId);
        removeAndCloseFiles(subtaskKey, checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(SubtaskKey subtaskKey, long checkpointId) throws Exception {
        super.notifyCheckpointAborted(subtaskKey, checkpointId);
        removeAndCloseFiles(subtaskKey, checkpointId);
    }

    /**
     * Remove files that belongs to specific subtask and checkpoint from the reuse pool. And close
     * these files.
     */
    private void removeAndCloseFiles(SubtaskKey subtaskKey, long checkpointId) throws Exception {
        PhysicalFilePool filePool;
        synchronized (writablePhysicalFilePool) {
            filePool = writablePhysicalFilePool.get(checkpointId);
        }
        if (filePool != null) {
            filePool.close(subtaskKey);
            if (filePool.isEmpty()) {
                synchronized (writablePhysicalFilePool) {
                    writablePhysicalFilePool.remove(checkpointId);
                }
            }
        }
    }

    @Override
    @Nonnull
    protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope)
            throws IOException {
        PhysicalFilePool filePool = getOrCreateFilePool(checkpointId);
        return filePool.pollFile(subtaskKey, scope);
    }

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

        PhysicalFilePool physicalFilePool = getOrCreateFilePool(checkpointId);
        if (!physicalFilePool.tryPutFile(subtaskKey, physicalFile)) {
            physicalFile.close();
        }
    }

    @Override
    protected void discardCheckpoint(long checkpointId) throws IOException {
        PhysicalFilePool filePool;
        synchronized (writablePhysicalFilePool) {
            filePool = writablePhysicalFilePool.get(checkpointId);
        }
        if (filePool != null) {
            filePool.close();
        }
    }

    private PhysicalFilePool getOrCreateFilePool(long checkpointId) {
        synchronized (writablePhysicalFilePool) {
            PhysicalFilePool filePool = writablePhysicalFilePool.get(checkpointId);
            if (filePool == null) {
                filePool = createPhysicalPool();
                writablePhysicalFilePool.put(checkpointId, filePool);
            }
            return filePool;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (PhysicalFilePool filePool : writablePhysicalFilePool.values()) {
            filePool.close();
        }
        writablePhysicalFilePool.clear();
    }
}
