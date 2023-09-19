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

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/** A builder that builds the {@link FileMergingSnapshotManager}. */
public class FileMergingSnapshotManagerBuilder {

    /** The id for identify a {@link FileMergingSnapshotManager}. */
    private final String id;

    @Nullable private Executor ioExecutor = null;

    /**
     * Initialize the builder.
     *
     * @param id the id of the manager.
     */
    public FileMergingSnapshotManagerBuilder(String id) {
        this.id = id;
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
     * <p>TODO (FLINK-32074): Support another type of FileMergingSnapshotManager that merges files
     * across different checkpoints.
     *
     * @return the created manager.
     */
    public FileMergingSnapshotManager build() {
        return new WithinCheckpointFileMergingSnapshotManager(
                id, ioExecutor == null ? Runnable::run : ioExecutor);
    }
}
