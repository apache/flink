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

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Abstract superclass for all {@link IncrementalKeyedStateHandle}. */
public abstract class AbstractIncrementalStateHandle implements IncrementalKeyedStateHandle {
    private static final long serialVersionUID = 1L;

    /** The checkpoint Id. */
    protected final long checkpointId;

    /**
     * UUID to identify the backend which created this state handle. This is in creating the key for
     * the {@link SharedStateRegistry}.
     */
    protected final UUID backendIdentifier;

    /** The key-group range covered by this state handle. */
    protected final KeyGroupRange keyGroupRange;

    /** Shared state in the incremental checkpoint. */
    protected final List<HandleAndLocalPath> sharedState;

    /** Primary meta data state of the incremental checkpoint. */
    protected final StreamStateHandle metaStateHandle;

    /** Unique id for this state handle. */
    protected final StateHandleID stateHandleId;

    public AbstractIncrementalStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            List<HandleAndLocalPath> sharedState,
            StreamStateHandle metaStateHandle,
            StateHandleID stateHandleId) {
        this.checkpointId = checkpointId;
        this.keyGroupRange = keyGroupRange;
        this.backendIdentifier = backendIdentifier;
        this.sharedState = sharedState;
        this.metaStateHandle = metaStateHandle;
        this.stateHandleId = stateHandleId;
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    @Nonnull
    @Override
    public UUID getBackendIdentifier() {
        return backendIdentifier;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Nonnull
    @Override
    public List<HandleAndLocalPath> getSharedStateHandles() {
        return sharedState;
    }

    @Nonnull
    @Override
    public StreamStateHandle getMetaDataStateHandle() {
        return metaStateHandle;
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleId;
    }

    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        return KeyGroupRange.EMPTY_KEY_GROUP_RANGE.equals(
                        getKeyGroupRange().getIntersection(keyGroupRange))
                ? null
                : this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractIncrementalStateHandle that = (AbstractIncrementalStateHandle) o;
        return Objects.equals(stateHandleId, that.stateHandleId);
    }

    @Override
    public int hashCode() {
        return stateHandleId.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractIncrementalStateHandle{"
                + "checkpointId="
                + checkpointId
                + ", backendIdentifier="
                + backendIdentifier
                + ", keyGroupRange="
                + keyGroupRange
                + ", sharedState="
                + sharedState
                + ", metaStateHandle="
                + metaStateHandle
                + ", stateHandleId="
                + stateHandleId
                + '}';
    }
}
