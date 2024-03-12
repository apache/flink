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

import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/** A pool for reusing {@link PhysicalFile}. This implementation should be thread-safe. */
public abstract class PhysicalFilePool implements Closeable {

    /** Types of supported physical file pool. */
    public enum Type {
        BLOCKING,
        NON_BLOCKING
    }

    /** creator to create a physical file. */
    protected final PhysicalFile.PhysicalFileCreator physicalFileCreator;

    /** Max size for a physical file. */
    protected final long maxFileSize;

    /** Map maintaining queues of different subtasks for reusing shared physical files. */
    protected final Map<FileMergingSnapshotManager.SubtaskKey, Queue<PhysicalFile>>
            sharedPhysicalFilePoolBySubtask;

    /** Queue maintaining exclusive physical files for reusing. */
    protected final Queue<PhysicalFile> exclusivePhysicalFilePool;

    public PhysicalFilePool(
            long maxFileSize, PhysicalFile.PhysicalFileCreator physicalFileCreator) {
        this.physicalFileCreator = physicalFileCreator;
        this.maxFileSize = maxFileSize;
        this.sharedPhysicalFilePoolBySubtask = new ConcurrentHashMap<>();
        this.exclusivePhysicalFilePool = createFileQueue();
    }

    /**
     * Try to put a physical file into file pool.
     *
     * @param subtaskKey the key of current subtask.
     * @param physicalFile target physical file.
     * @return true if file is in the pool, false otherwise.
     */
    public abstract boolean tryPutFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, PhysicalFile physicalFile)
            throws IOException;

    /**
     * Poll a physical file from the pool.
     *
     * @param subtaskKey the key of current subtask.
     * @param scope the scope of the checkpoint.
     * @return a physical file.
     */
    @Nonnull
    public abstract PhysicalFile pollFile(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope)
            throws IOException;

    /**
     * Create and return a file queue.
     *
     * @return a created file queue.
     */
    protected abstract Queue<PhysicalFile> createFileQueue();

    /**
     * Get or create a file queue for specific subtaskKey and checkpoint scope.
     *
     * @param subtaskKey the key of current subtask.
     * @param scope the scope of the checkpoint.
     * @return an existing or created file queue.
     */
    protected Queue<PhysicalFile> getFileQueue(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, CheckpointedStateScope scope) {
        return CheckpointedStateScope.SHARED.equals(scope)
                ? sharedPhysicalFilePoolBySubtask.computeIfAbsent(
                        subtaskKey, key -> createFileQueue())
                : exclusivePhysicalFilePool;
    }

    /**
     * Return whether the pool is empty or not.
     *
     * @return whether the pool is empty or not.
     */
    public boolean isEmpty() {
        return sharedPhysicalFilePoolBySubtask.isEmpty() && exclusivePhysicalFilePool.isEmpty();
    }

    /**
     * Close files in shared file pool by subtaskKey and close all files in exclusive file pool.
     *
     * @param subtaskKey the key of current subtask.
     * @throws IOException if anything goes wrong when closing files.
     */
    public void close(FileMergingSnapshotManager.SubtaskKey subtaskKey) throws IOException {
        if (sharedPhysicalFilePoolBySubtask.containsKey(subtaskKey)) {
            closeFilesInQueue(sharedPhysicalFilePoolBySubtask.remove(subtaskKey));
        }
        closeFilesInQueue(exclusivePhysicalFilePool);
    }

    @Override
    public void close() throws IOException {
        closeFilesInQueue(exclusivePhysicalFilePool);
        for (Queue<PhysicalFile> queue : sharedPhysicalFilePoolBySubtask.values()) {
            closeFilesInQueue(queue);
        }
        sharedPhysicalFilePoolBySubtask.clear();
    }

    private void closeFilesInQueue(Queue<PhysicalFile> queue) throws IOException {
        while (!queue.isEmpty()) {
            PhysicalFile physicalFile = queue.poll();
            if (physicalFile != null) {
                physicalFile.close();
            }
        }
    }
}
