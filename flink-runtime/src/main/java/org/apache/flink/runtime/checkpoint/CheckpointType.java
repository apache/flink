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

package org.apache.flink.runtime.checkpoint;

/** The type of checkpoint to perform. */
public enum CheckpointType {

    /** A checkpoint, full or incremental. */
    CHECKPOINT(
            false, PostCheckpointAction.NONE, "Checkpoint", SharingFilesStrategy.FORWARD_BACKWARD),

    /** A regular savepoint. */
    SAVEPOINT(true, PostCheckpointAction.NONE, "Savepoint", SharingFilesStrategy.NO_SHARING),

    /** A savepoint taken while suspending the job. */
    SAVEPOINT_SUSPEND(
            true,
            PostCheckpointAction.SUSPEND,
            "Suspend Savepoint",
            SharingFilesStrategy.NO_SHARING),

    /** A savepoint taken while terminating the job. */
    SAVEPOINT_TERMINATE(
            true,
            PostCheckpointAction.TERMINATE,
            "Terminate Savepoint",
            SharingFilesStrategy.NO_SHARING),

    FULL_CHECKPOINT(
            false, PostCheckpointAction.NONE, "Full Checkpoint", SharingFilesStrategy.FORWARD);

    private final boolean isSavepoint;

    private final PostCheckpointAction postCheckpointAction;

    private final String name;

    private final SharingFilesStrategy sharingFilesStrategy;

    CheckpointType(
            final boolean isSavepoint,
            final PostCheckpointAction postCheckpointAction,
            final String name,
            SharingFilesStrategy sharingFilesStrategy) {

        this.isSavepoint = isSavepoint;
        this.postCheckpointAction = postCheckpointAction;
        this.name = name;
        this.sharingFilesStrategy = sharingFilesStrategy;
    }

    public boolean isSavepoint() {
        return isSavepoint;
    }

    public boolean isSynchronous() {
        return postCheckpointAction != PostCheckpointAction.NONE;
    }

    public PostCheckpointAction getPostCheckpointAction() {
        return postCheckpointAction;
    }

    public boolean shouldAdvanceToEndOfTime() {
        return shouldDrain();
    }

    public boolean shouldDrain() {
        return getPostCheckpointAction() == PostCheckpointAction.TERMINATE;
    }

    public boolean shouldIgnoreEndOfInput() {
        return getPostCheckpointAction() == PostCheckpointAction.SUSPEND;
    }

    public String getName() {
        return name;
    }

    public SharingFilesStrategy getSharingFilesStrategy() {
        return sharingFilesStrategy;
    }

    /** What's the intended action after the checkpoint (relevant for stopping with savepoint). */
    public enum PostCheckpointAction {
        NONE,
        SUSPEND,
        TERMINATE
    }

    /** Defines what files can be shared across snapshots. */
    public enum SharingFilesStrategy {
        // current snapshot can share files with previous snapshots.
        // new snapshots can use files of the current snapshot
        FORWARD_BACKWARD,
        // later snapshots can share files with the current snapshot
        FORWARD,
        // current snapshot can not use files of older ones, future snapshots can
        // not use files of the current one.
        NO_SHARING;
    }
}
