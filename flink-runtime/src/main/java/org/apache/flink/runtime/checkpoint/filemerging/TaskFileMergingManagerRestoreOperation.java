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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Restore operation that restores file-merging information belonging to one task for {@link
 * FileMergingSnapshotManager}.
 */
public class TaskFileMergingManagerRestoreOperation {

    /** The restored checkpoint id. */
    private final long checkpointId;

    /** The restored Task info. */
    private final TaskInfo taskInfo;

    private final FileMergingSnapshotManager fileMergingSnapshotManager;

    /** The TaskStateSnapshot which belongs to the restored Task. */
    private final TaskStateSnapshot taskStateSnapshot;

    public TaskFileMergingManagerRestoreOperation(
            long checkpointId,
            FileMergingSnapshotManager fileMergingSnapshotManager,
            TaskInfo taskInfo,
            TaskStateSnapshot taskStateSnapshot) {
        this.checkpointId = checkpointId;
        this.taskInfo = taskInfo;
        this.fileMergingSnapshotManager = fileMergingSnapshotManager;
        this.taskStateSnapshot = taskStateSnapshot;
    }

    public void restore() {
        for (Map.Entry<OperatorID, OperatorSubtaskState> operatorSubTaskState :
                taskStateSnapshot.getSubtaskStateMappings()) {
            FileMergingSnapshotManager.SubtaskKey subtaskKey =
                    new FileMergingSnapshotManager.SubtaskKey(
                            operatorSubTaskState.getKey(), taskInfo);
            restoreOperatorSubtaskStateHandles(subtaskKey, operatorSubTaskState.getValue());
        }
    }

    private void restoreOperatorSubtaskStateHandles(
            FileMergingSnapshotManager.SubtaskKey subtaskKey, OperatorSubtaskState subtaskState) {

        Stream<StreamStateHandle> keyedStateHandles =
                Stream.concat(
                                subtaskState.getManagedKeyedState().stream(),
                                subtaskState.getRawKeyedState().stream())
                        .flatMap(this::getChildrenStreamHandles);

        Stream<StreamStateHandle> operatorStateHandles =
                Stream.concat(
                                subtaskState.getManagedOperatorState().stream(),
                                subtaskState.getRawOperatorState().stream())
                        .flatMap(this::getChildrenStreamHandles);

        // TODO support channel state restore for unaligned checkpoint.

        Stream<SegmentFileStateHandle> segmentStateHandles =
                Stream.of(keyedStateHandles, operatorStateHandles)
                        .flatMap(Function.identity())
                        .filter(handle -> handle instanceof SegmentFileStateHandle)
                        .map(handle -> (SegmentFileStateHandle) handle);
        fileMergingSnapshotManager.restoreStateHandles(
                checkpointId, subtaskKey, segmentStateHandles);
    }

    private Stream<StreamStateHandle> getChildrenStreamHandles(KeyedStateHandle parentHandle) {
        if (parentHandle instanceof IncrementalRemoteKeyedStateHandle) {
            return ((IncrementalRemoteKeyedStateHandle) parentHandle).streamSubHandles();
        }
        if (parentHandle instanceof KeyGroupsStateHandle) {
            return Stream.of(((KeyGroupsStateHandle) parentHandle).getDelegateStateHandle());
        }
        // TODO support changelog keyed state handle
        if (parentHandle instanceof StreamStateHandle) {
            return Stream.of((StreamStateHandle) parentHandle);
        }
        throw new UnsupportedOperationException(
                "Unsupported KeyedStateHandle type:" + parentHandle);
    }

    private Stream<StreamStateHandle> getChildrenStreamHandles(OperatorStateHandle parentHandle) {
        return Stream.of(parentHandle.getDelegateStateHandle());
    }
}
