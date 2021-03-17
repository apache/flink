/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class contains the combined results from the snapshot of a state backend:
 *
 * <ul>
 *   <li>A state object representing the state that will be reported to the Job Manager to
 *       acknowledge the checkpoint.
 *   <li>A state object that represents the state for the {@link TaskLocalStateStoreImpl}.
 * </ul>
 *
 * Both state objects are optional and can be null, e.g. if there was no state to snapshot in the
 * backend. A local state object that is not null also requires a state to report to the job manager
 * that is not null, because the Job Manager always owns the ground truth about the checkpointed
 * state.
 */
public class SnapshotResult<T extends StateObject> implements StateObject {

    private static final long serialVersionUID = 1L;

    /** An singleton instance to represent an empty snapshot result. */
    private static final SnapshotResult<?> EMPTY = new SnapshotResult<>(null, null);

    /**
     * This is the state snapshot that will be reported to the Job Manager to acknowledge a
     * checkpoint.
     */
    private final T jobManagerOwnedSnapshot;

    /**
     * This is the state snapshot that will be reported to the Job Manager to acknowledge a
     * checkpoint.
     */
    private final T taskLocalSnapshot;

    /**
     * Creates a {@link SnapshotResult} for the given jobManagerOwnedSnapshot and taskLocalSnapshot.
     * If the jobManagerOwnedSnapshot is null, taskLocalSnapshot must also be null.
     *
     * @param jobManagerOwnedSnapshot Snapshot for report to job manager. Can be null.
     * @param taskLocalSnapshot Snapshot for report to local state manager. This is optional and
     *     requires jobManagerOwnedSnapshot to be not null if this is not also null.
     */
    private SnapshotResult(T jobManagerOwnedSnapshot, T taskLocalSnapshot) {

        if (jobManagerOwnedSnapshot == null && taskLocalSnapshot != null) {
            throw new IllegalStateException(
                    "Cannot report local state snapshot without corresponding remote state!");
        }

        this.jobManagerOwnedSnapshot = jobManagerOwnedSnapshot;
        this.taskLocalSnapshot = taskLocalSnapshot;
    }

    @Nullable
    public T getJobManagerOwnedSnapshot() {
        return jobManagerOwnedSnapshot;
    }

    @Nullable
    public T getTaskLocalSnapshot() {
        return taskLocalSnapshot;
    }

    @Override
    public void discardState() throws Exception {

        Exception aggregatedExceptions = null;

        if (jobManagerOwnedSnapshot != null) {
            try {
                jobManagerOwnedSnapshot.discardState();
            } catch (Exception remoteDiscardEx) {
                aggregatedExceptions = remoteDiscardEx;
            }
        }

        if (taskLocalSnapshot != null) {
            try {
                taskLocalSnapshot.discardState();
            } catch (Exception localDiscardEx) {
                aggregatedExceptions =
                        ExceptionUtils.firstOrSuppressed(localDiscardEx, aggregatedExceptions);
            }
        }

        if (aggregatedExceptions != null) {
            throw aggregatedExceptions;
        }
    }

    @Override
    public long getStateSize() {
        return jobManagerOwnedSnapshot != null ? jobManagerOwnedSnapshot.getStateSize() : 0L;
    }

    @SuppressWarnings("unchecked")
    public static <T extends StateObject> SnapshotResult<T> empty() {
        return (SnapshotResult<T>) EMPTY;
    }

    public static <T extends StateObject> SnapshotResult<T> of(@Nullable T jobManagerState) {
        return jobManagerState != null ? new SnapshotResult<>(jobManagerState, null) : empty();
    }

    public static <T extends StateObject> SnapshotResult<T> withLocalState(
            @Nonnull T jobManagerState, @Nonnull T localState) {
        return new SnapshotResult<>(jobManagerState, localState);
    }
}
