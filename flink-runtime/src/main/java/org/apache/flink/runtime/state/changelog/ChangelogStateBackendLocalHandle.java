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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;

import javax.annotation.Nullable;

import java.util.List;

/**
 * State handle for local copies of {@link ChangelogStateHandleStreamImpl}. Consists of a
 * remoteHandle that maintains the mapping of local handle and remote handle, like
 * sharedStateHandleIDs in {@link org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle}.
 */
public class ChangelogStateBackendLocalHandle implements ChangelogStateBackendHandle {
    private static final long serialVersionUID = 1L;
    private final List<KeyedStateHandle> localMaterialized;
    private final List<ChangelogStateHandle> localNonMaterialized;
    private final ChangelogStateBackendHandleImpl remoteHandle;

    public ChangelogStateBackendLocalHandle(
            List<KeyedStateHandle> localMaterialized,
            List<ChangelogStateHandle> localNonMaterialized,
            ChangelogStateBackendHandleImpl remoteHandle) {
        this.localMaterialized = localMaterialized;
        this.localNonMaterialized = localNonMaterialized;
        this.remoteHandle = remoteHandle;
    }

    @Override
    public List<KeyedStateHandle> getMaterializedStateHandles() {
        return localMaterialized;
    }

    @Override
    public List<ChangelogStateHandle> getNonMaterializedStateHandles() {
        return localNonMaterialized;
    }

    @Override
    public long getMaterializationID() {
        return remoteHandle.getMaterializationID();
    }

    @Override
    public ChangelogStateBackendHandle rebound(long checkpointId) {
        throw new UnsupportedOperationException("Should not call here.");
    }

    public List<KeyedStateHandle> getRemoteMaterializedStateHandles() {
        return remoteHandle.getMaterializedStateHandles();
    }

    public List<ChangelogStateHandle> getRemoteNonMaterializedStateHandles() {
        return remoteHandle.getNonMaterializedStateHandles();
    }

    @Override
    public long getCheckpointId() {
        return remoteHandle.getCheckpointId();
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        remoteHandle.registerSharedStates(stateRegistry, checkpointID);
    }

    @Override
    public long getCheckpointedSize() {
        return remoteHandle.getCheckpointedSize();
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return remoteHandle.getKeyGroupRange();
    }

    @Nullable
    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        throw new UnsupportedOperationException(
                "This is a local state handle for the TM side only.");
    }

    @Override
    public StateHandleID getStateHandleId() {
        return remoteHandle.getStateHandleId();
    }

    @Override
    public void discardState() throws Exception {}

    @Override
    public long getStateSize() {
        return remoteHandle.getStateSize();
    }
}
