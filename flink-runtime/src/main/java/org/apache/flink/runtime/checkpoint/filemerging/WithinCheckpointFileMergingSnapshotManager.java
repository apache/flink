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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointedStateScope;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/** A {@link FileMergingSnapshotManager} that merging files within a checkpoint. */
public class WithinCheckpointFileMergingSnapshotManager extends FileMergingSnapshotManagerBase {

    /** A dummy subtask key to reuse files among subtasks for private states. */
    private static final SubtaskKey DUMMY_SUBTASK_KEY = new SubtaskKey("dummy", -1, -1);

    /**
     * For WITHIN_BOUNDARY mode, physical files are NOT shared among multiple checkpoints. This map
     * contains all physical files that are still writable and not occupied by a writer. The key of
     * this map consist of checkpoint id, subtask key, and checkpoint scope, which collectively
     * determine the physical file to be reused.
     */
    private final Map<Tuple3<Long, SubtaskKey, CheckpointedStateScope>, PhysicalFile>
            writablePhysicalFilePool;

    public WithinCheckpointFileMergingSnapshotManager(String id, Executor ioExecutor) {
        // currently there is no file size limit For WITHIN_BOUNDARY mode
        super(id, ioExecutor);
        writablePhysicalFilePool = new HashMap<>();
    }

    @Override
    @Nonnull
    protected PhysicalFile getOrCreatePhysicalFileForCheckpoint(
            SubtaskKey subtaskKey, long checkpointId, CheckpointedStateScope scope)
            throws IOException {
        // TODO: FLINK-32076 will add a file pool for each subtask key.
        Tuple3<Long, SubtaskKey, CheckpointedStateScope> fileKey =
                Tuple3.of(
                        checkpointId,
                        scope == CheckpointedStateScope.SHARED ? subtaskKey : DUMMY_SUBTASK_KEY,
                        scope);
        PhysicalFile file;
        synchronized (writablePhysicalFilePool) {
            file = writablePhysicalFilePool.remove(fileKey);
            if (file == null) {
                file = createPhysicalFile(subtaskKey, scope);
            }
        }
        return file;
    }

    @Override
    protected void returnPhysicalFileForNextReuse(
            SubtaskKey subtaskKey, long checkpointId, PhysicalFile physicalFile)
            throws IOException {
        // TODO: FLINK-32076 will add a file pool for reusing.
        CheckpointedStateScope scope = physicalFile.getScope();
        Tuple3<Long, SubtaskKey, CheckpointedStateScope> fileKey =
                Tuple3.of(
                        checkpointId,
                        scope == CheckpointedStateScope.SHARED ? subtaskKey : DUMMY_SUBTASK_KEY,
                        scope);
        PhysicalFile current;
        synchronized (writablePhysicalFilePool) {
            current = writablePhysicalFilePool.putIfAbsent(fileKey, physicalFile);
        }
        // TODO: We sync the file when return to the reuse pool for safety. Actually it could be
        // optimized after FLINK-32075.
        if (shouldSyncAfterClosingLogicalFile) {
            FSDataOutputStream os = physicalFile.getOutputStream();
            if (os != null) {
                os.sync();
            }
        }
        if (current != physicalFile) {
            physicalFile.close();
        }
    }
}
