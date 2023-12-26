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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * State handle for local copies of {@link IncrementalRemoteKeyedStateHandle}. Consists of a {@link
 * DirectoryStateHandle} that represents the directory of the native RocksDB snapshot, the key
 * groups, and a stream state handle for Flink's state meta data file.
 */
public class IncrementalLocalKeyedStateHandle extends AbstractIncrementalStateHandle {

    private static final long serialVersionUID = 1L;

    private final DirectoryStateHandle directoryStateHandle;

    public IncrementalLocalKeyedStateHandle(
            @Nonnull UUID backendIdentifier,
            @Nonnegative long checkpointId,
            @Nonnull DirectoryStateHandle directoryStateHandle,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnull StreamStateHandle metaDataState,
            @Nonnull List<HandleAndLocalPath> sharedState) {

        super(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                new ArrayList<>(sharedState),
                metaDataState,
                StateHandleID.randomStateHandleId());
        this.directoryStateHandle = directoryStateHandle;
    }

    @Override
    public CheckpointBoundKeyedStateHandle rebound(long checkpointId) {
        return new IncrementalLocalKeyedStateHandle(
                backendIdentifier,
                checkpointId,
                getDirectoryStateHandle(),
                getKeyGroupRange(),
                getMetaDataStateHandle(),
                getSharedStateHandles());
    }

    @Override
    public void discardState() throws Exception {

        Exception collectedEx = null;

        try {
            directoryStateHandle.discardState();
        } catch (Exception e) {
            collectedEx = e;
        }

        try {
            metaStateHandle.discardState();
        } catch (Exception e) {
            collectedEx = ExceptionUtils.firstOrSuppressed(e, collectedEx);
        }

        if (collectedEx != null) {
            throw collectedEx;
        }
    }

    @Override
    public long getStateSize() {
        return directoryStateHandle.getStateSize() + metaStateHandle.getStateSize();
    }

    @Override
    public String toString() {
        return "IncrementalLocalKeyedStateHandle{"
                + "directoryStateHandle="
                + directoryStateHandle
                + "} "
                + super.toString();
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        // Nothing to do, this is for local use only.
    }

    @Override
    public long getCheckpointedSize() {
        return directoryStateHandle.getStateSize();
    }

    @Nonnull
    public DirectoryStateHandle getDirectoryStateHandle() {
        return directoryStateHandle;
    }
}
