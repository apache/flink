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

package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * A {@link OperatorStreamStateHandle} that works for file merging checkpoints.
 *
 * <p>Operator states are stored in `taskownd/` dir when file merging is enabled. When an operator
 * state dir is not referenced by any checkpoint, {@link SharedStateRegistry} will discard it. The
 * shared subtask dir of fire merging is also tracked by {@link
 * FileMergingOperatorStreamStateHandle}.
 *
 * <p>The shared subtask dir of file merging is created when task initialization, which will be
 * discarded when no checkpoint refer to it.
 */
public class FileMergingOperatorStreamStateHandle extends OperatorStreamStateHandle
        implements CompositeStateHandle {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(FileMergingOperatorStreamStateHandle.class);

    /** The directory handle of file merging under 'taskowed/', one for each job. */
    private final DirectoryStreamStateHandle taskOwnedDirHandle;

    /**
     * The directory handle of file merging under 'shared/', one for each subtask.
     *
     * @see org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager the layout of
     *     file merging checkpoint directory.
     */
    private final DirectoryStreamStateHandle sharedDirHandle;

    private transient SharedStateRegistry sharedStateRegistry;

    public FileMergingOperatorStreamStateHandle(
            DirectoryStreamStateHandle taskOwnedDirHandle,
            DirectoryStreamStateHandle sharedDirHandle,
            Map<String, StateMetaInfo> stateNameToPartitionOffsets,
            StreamStateHandle delegateStateHandle) {
        super(stateNameToPartitionOffsets, delegateStateHandle);
        this.taskOwnedDirHandle = taskOwnedDirHandle;
        this.sharedDirHandle = sharedDirHandle;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointId) {
        Preconditions.checkState(
                sharedStateRegistry != stateRegistry,
                "The state handle has already registered its shared states to the given registry.");

        sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

        LOG.trace(
                "Registering FileMergingOperatorStreamStateHandle for checkpoint {} from backend.",
                checkpointId);
        // Only register the directory here, leave the delegateStateHandle unregistered, since the
        // OperatorSubtaskState will only take care of the keyed state while leaving others
        // unregistered.
        stateRegistry.registerReference(
                taskOwnedDirHandle.createStateRegistryKey(), taskOwnedDirHandle, checkpointId);
        stateRegistry.registerReference(
                sharedDirHandle.createStateRegistryKey(), sharedDirHandle, checkpointId);
    }

    @Override
    public void discardState() throws Exception {
        SharedStateRegistry registry = this.sharedStateRegistry;
        final boolean isRegistered = (registry != null);

        LOG.trace(
                "Discarding FileMergingOperatorStreamStateHandle (registered = {}) from backend.",
                isRegistered);

        try {
            getDelegateStateHandle().discardState();
        } catch (Exception e) {
            LOG.warn("Could not properly discard directory state handle.", e);
        }
    }

    @Override
    public long getCheckpointedSize() {
        return getDelegateStateHandle().getStateSize();
    }

    public DirectoryStreamStateHandle getSharedDirHandle() {
        return sharedDirHandle;
    }

    public DirectoryStreamStateHandle getTaskOwnedDirHandle() {
        return taskOwnedDirHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileMergingOperatorStreamStateHandle that = (FileMergingOperatorStreamStateHandle) o;

        return super.equals(that)
                && taskOwnedDirHandle.equals(that.taskOwnedDirHandle)
                && sharedDirHandle.equals(that.sharedDirHandle);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(taskOwnedDirHandle);
        result = 31 * result + Objects.hashCode(sharedDirHandle);
        return result;
    }

    @Override
    public String toString() {
        return "FileMergingOperatorStreamStateHandle{"
                + super.toString()
                + ", taskOwnedDirHandle="
                + taskOwnedDirHandle
                + ", sharedDirHandle="
                + sharedDirHandle
                + '}';
    }
}
