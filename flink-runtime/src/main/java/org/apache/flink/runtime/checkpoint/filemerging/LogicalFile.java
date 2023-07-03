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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringBasedID;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager.SubtaskKey;

/**
 * An abstraction of logical files in file-merging checkpoints. It stands for a data segment, that
 * is to say a single file before file-merging.
 */
public class LogicalFile {

    /** ID for {@link LogicalFile}. It should be unique for each file. */
    public static class LogicalFileId extends StringBasedID {

        public LogicalFileId(String keyString) {
            super(keyString);
        }

        public Path getFilePath() {
            return new Path(getKeyString());
        }

        public static LogicalFileId generateRandomId() {
            return new LogicalFileId(UUID.randomUUID().toString());
        }
    }

    /** ID for this file. */
    LogicalFileId fileId;

    /**
     * The id of the last checkpoint that use this logical file. This acts as a watermark
     * determining whether this logical file could be removed.
     *
     * @see #discardWithCheckpointId(long)
     * @see #advanceLastCheckpointId(long)
     */
    private long lastUsedCheckpointID = -1L;

    /** Whether this logical file is removed by checkpoint subsumption/abortion. */
    boolean discarded = false;

    /** The physical file where this logical file is stored. This should never be null. */
    @Nonnull private final PhysicalFile physicalFile;

    /** The offset of the physical file that this logical file start from. */
    private final int startOffset;

    /** The length of this logical file. */
    private final int length;

    /** The id of the subtask that this logical file belongs to. */
    @Nonnull private final SubtaskKey subtaskKey;

    public LogicalFile(
            LogicalFileId fileId,
            @Nonnull PhysicalFile physicalFile,
            int startOffset,
            int length,
            @Nonnull SubtaskKey subtaskKey) {
        this.fileId = fileId;
        this.physicalFile = physicalFile;
        this.startOffset = startOffset;
        this.length = length;
        this.subtaskKey = subtaskKey;
        physicalFile.incRefCount();
    }

    public LogicalFileId getFileId() {
        return fileId;
    }

    /**
     * A logical file may share across checkpoints (especially for shared state). When this logical
     * file is used/reused by a checkpoint, update the last checkpoint id that uses this logical
     * file.
     *
     * @param checkpointId the checkpoint that uses this logical file.
     */
    public void advanceLastCheckpointId(long checkpointId) {
        if (checkpointId > lastUsedCheckpointID) {
            this.lastUsedCheckpointID = checkpointId;
        }
    }

    /**
     * When a checkpoint that uses this logical file is subsumed or aborted, discard this logical
     * file. If this file is used by a later checkpoint, the file should not be discarded. Note that
     * the removal of logical may cause the deletion of physical file.
     *
     * @param checkpointId the checkpoint that is notified subsumed or aborted.
     * @throws IOException if anything goes wrong with file system.
     */
    public void discardWithCheckpointId(long checkpointId) throws IOException {
        if (!discarded && checkpointId >= lastUsedCheckpointID) {
            physicalFile.decRefCount();
            discarded = true;
        }
    }

    public long getLastUsedCheckpointID() {
        return lastUsedCheckpointID;
    }

    @Nonnull
    public PhysicalFile getPhysicalFile() {
        return physicalFile;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getLength() {
        return length;
    }

    @Nonnull
    public SubtaskKey getSubtaskKey() {
        return subtaskKey;
    }

    @VisibleForTesting
    public boolean isDiscarded() {
        return discarded;
    }

    @Override
    public int hashCode() {
        return fileId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogicalFile that = (LogicalFile) o;
        return fileId.equals(that.fileId);
    }
}
