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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.EmptySegmentFileStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.util.Preconditions;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Restore operation that restores file-merging information belonging to one subtask for {@link
 * FileMergingSnapshotManager}.
 */
public class SubtaskFileMergingManagerRestoreOperation {

    /** The restored checkpoint id. */
    private final long checkpointId;

    /** The restored job id. */
    private final JobID jobID;

    /** The restored Task info. */
    private final TaskInfo taskInfo;

    /** The id of the operator to which the subtask belongs. */
    private final OperatorID operatorID;

    private final FileMergingSnapshotManager fileMergingSnapshotManager;

    /** The state which belongs to the restored subtask. */
    private final OperatorSubtaskState subtaskState;

    public SubtaskFileMergingManagerRestoreOperation(
            long checkpointId,
            FileMergingSnapshotManager fileMergingSnapshotManager,
            JobID jobID,
            TaskInfo taskInfo,
            OperatorID operatorID,
            OperatorSubtaskState subtaskState) {
        this.checkpointId = checkpointId;
        this.fileMergingSnapshotManager = fileMergingSnapshotManager;
        this.jobID = jobID;
        this.taskInfo = Preconditions.checkNotNull(taskInfo);
        this.operatorID = Preconditions.checkNotNull(operatorID);
        this.subtaskState = Preconditions.checkNotNull(subtaskState);
    }

    public void restore() {
        FileMergingSnapshotManager.SubtaskKey subtaskKey =
                new FileMergingSnapshotManager.SubtaskKey(jobID, operatorID, taskInfo);

        Stream<? extends StateObject> keyedStateHandles =
                Stream.concat(
                                subtaskState.getManagedKeyedState().stream(),
                                subtaskState.getRawKeyedState().stream())
                        .flatMap(this::getChildrenStreamHandles);

        Stream<? extends StateObject> operatorStateHandles =
                Stream.concat(
                                subtaskState.getManagedOperatorState().stream(),
                                subtaskState.getRawOperatorState().stream())
                        .flatMap(this::getChildrenStreamHandles);

        // TODO support channel state restore for unaligned checkpoint.

        Stream<SegmentFileStateHandle> segmentStateHandles =
                Stream.of(keyedStateHandles, operatorStateHandles)
                        .flatMap(Function.identity())
                        .filter(
                                handle ->
                                        (handle instanceof SegmentFileStateHandle)
                                                && !(handle instanceof EmptySegmentFileStateHandle))
                        .map(handle -> (SegmentFileStateHandle) handle);
        fileMergingSnapshotManager.restoreStateHandles(
                checkpointId, subtaskKey, segmentStateHandles);
    }

    private Stream<? extends StateObject> getChildrenStreamHandles(KeyedStateHandle parentHandle) {
        if (parentHandle instanceof IncrementalRemoteKeyedStateHandle) {
            return ((IncrementalRemoteKeyedStateHandle) parentHandle).streamSubHandles();
        }
        if (parentHandle instanceof KeyGroupsStateHandle) {
            return Stream.of(((KeyGroupsStateHandle) parentHandle).getDelegateStateHandle());
        }
        // TODO support changelog keyed state handle
        return Stream.of(parentHandle);
    }

    private Stream<StreamStateHandle> getChildrenStreamHandles(OperatorStateHandle parentHandle) {
        return Stream.of(parentHandle.getDelegateStateHandle());
    }
}
