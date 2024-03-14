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

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/** A builder that builds the {@link FileMergingSnapshotManager}. */
public class FileMergingSnapshotManagerBuilder {

    /** The id for identifying a {@link FileMergingSnapshotManager}. */
    private final String id;

    /** The file merging type. */
    private final FileMergingType fileMergingType;

    /** Max size for a file. TODO: Make it configurable. */
    private long maxFileSize = 32 * 1024 * 1024;

    /** Type of physical file pool. TODO: Make it configurable. */
    private PhysicalFilePool.Type filePoolType = PhysicalFilePool.Type.NON_BLOCKING;

    @Nullable private Executor ioExecutor = null;

    /**
     * Initialize the builder.
     *
     * @param id the id of the manager.
     */
    public FileMergingSnapshotManagerBuilder(String id, FileMergingType type) {
        this.id = id;
        this.fileMergingType = type;
    }

    /** Set the max file size. */
    public FileMergingSnapshotManagerBuilder setMaxFileSize(long maxFileSize) {
        Preconditions.checkArgument(maxFileSize > 0);
        this.maxFileSize = maxFileSize;
        return this;
    }

    /** Set the type of physical file pool. */
    public FileMergingSnapshotManagerBuilder setFilePoolType(PhysicalFilePool.Type filePoolType) {
        this.filePoolType = filePoolType;
        return this;
    }

    /**
     * Set the executor for io operation in manager. If null(default), all io operation will be
     * executed synchronously.
     */
    public FileMergingSnapshotManagerBuilder setIOExecutor(@Nullable Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    /**
     * Create file-merging snapshot manager based on configuration.
     *
     * @return the created manager.
     */
    public FileMergingSnapshotManager build() {
        switch (fileMergingType) {
            case MERGE_WITHIN_CHECKPOINT:
                return new WithinCheckpointFileMergingSnapshotManager(
                        id,
                        maxFileSize,
                        filePoolType,
                        ioExecutor == null ? Runnable::run : ioExecutor);
            case MERGE_ACROSS_CHECKPOINT:
                return new AcrossCheckpointFileMergingSnapshotManager(
                        id,
                        maxFileSize,
                        filePoolType,
                        ioExecutor == null ? Runnable::run : ioExecutor);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported type %s when creating file merging manager",
                                fileMergingType));
        }
    }
}
