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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The handle to states of an incremental snapshot.
 *
 * <p>The states contained in an incremental snapshot include:
 *
 * <ul>
 *   <li>Created shared state which includes shared files produced since the last completed
 *       checkpoint. These files can be referenced by succeeding checkpoints if the checkpoint
 *       succeeds to complete.
 *   <li>Referenced shared state which includes the shared files materialized in previous
 *       checkpoints. Until we this is registered to a {@link SharedStateRegistry}, all referenced
 *       shared state handles are only placeholders, so that we do not send state handles twice from
 *       which we know that they already exist on the checkpoint coordinator.
 *   <li>Private state which includes all other files, typically mutable, that cannot be shared by
 *       other checkpoints.
 *   <li>Backend meta state which includes the information of existing states.
 * </ul>
 *
 * When this should become a completed checkpoint on the checkpoint coordinator, it must first be
 * registered with a {@link SharedStateRegistry}, so that all placeholder state handles to
 * previously existing state are replaced with the originals.
 *
 * <p>IMPORTANT: This class currently overrides equals and hash code only for testing purposes. They
 * should not be called from production code. This means this class is also not suited to serve as a
 * key, e.g. in hash maps.
 */
public class IncrementalRemoteKeyedStateHandle implements IncrementalKeyedStateHandle {

    public static final long UNKNOWN_CHECKPOINTED_SIZE = -1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalRemoteKeyedStateHandle.class);

    private static final long serialVersionUID = -8328808513197388231L;

    /**
     * UUID to identify the backend which created this state handle. This is in creating the key for
     * the {@link SharedStateRegistry}.
     */
    private final UUID backendIdentifier;

    /** The key-group range covered by this state handle. */
    private final KeyGroupRange keyGroupRange;

    /** The checkpoint Id. */
    private final long checkpointId;

    /** Shared state in the incremental checkpoint. */
    private final List<HandleAndLocalPath> sharedState;

    /** Private state in the incremental checkpoint. */
    private final List<HandleAndLocalPath> privateState;

    /** Primary meta data state of the incremental checkpoint. */
    private final StreamStateHandle metaStateHandle;

    private final long persistedSizeOfThisCheckpoint;

    private final StateHandleID stateHandleId;

    /**
     * Once the shared states are registered, it is the {@link SharedStateRegistry}'s responsibility
     * to cleanup those shared states. But in the cases where the state handle is discarded before
     * performing the registration, the handle should delete all the shared states created by it.
     *
     * <p>This variable is not null iff the handles was registered.
     */
    private transient SharedStateRegistry sharedStateRegistry;

    @VisibleForTesting
    public IncrementalRemoteKeyedStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle) {
        this(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaStateHandle,
                UNKNOWN_CHECKPOINTED_SIZE,
                new StateHandleID(UUID.randomUUID().toString()));
    }

    public IncrementalRemoteKeyedStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle,
            long persistedSizeOfThisCheckpoint) {

        this(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaStateHandle,
                persistedSizeOfThisCheckpoint,
                new StateHandleID(UUID.randomUUID().toString()));
    }

    protected IncrementalRemoteKeyedStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle,
            long persistedSizeOfThisCheckpoint,
            StateHandleID stateHandleId) {
        this.backendIdentifier = Preconditions.checkNotNull(backendIdentifier);
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
        this.checkpointId = checkpointId;
        this.sharedState = Preconditions.checkNotNull(sharedState);
        this.privateState = Preconditions.checkNotNull(privateState);
        this.metaStateHandle = Preconditions.checkNotNull(metaStateHandle);
        this.sharedStateRegistry = null;
        this.persistedSizeOfThisCheckpoint =
                persistedSizeOfThisCheckpoint == UNKNOWN_CHECKPOINTED_SIZE
                        ? getStateSize()
                        : persistedSizeOfThisCheckpoint;
        this.stateHandleId = stateHandleId;
    }

    public static IncrementalRemoteKeyedStateHandle restore(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle,
            long persistedSizeOfThisCheckpoint,
            StateHandleID stateHandleId) {
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaStateHandle,
                persistedSizeOfThisCheckpoint,
                stateHandleId);
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public CheckpointBoundKeyedStateHandle rebound(long checkpointId) {
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaStateHandle,
                persistedSizeOfThisCheckpoint,
                stateHandleId);
    }

    public List<HandleAndLocalPath> getSharedState() {
        return sharedState;
    }

    public List<HandleAndLocalPath> getPrivateState() {
        return privateState;
    }

    public StreamStateHandle getMetaStateHandle() {
        return metaStateHandle;
    }

    @Nonnull
    public UUID getBackendIdentifier() {
        return backendIdentifier;
    }

    @Nonnull
    @Override
    public List<HandleAndLocalPath> getSharedStateHandles() {
        return getSharedState();
    }

    public SharedStateRegistry getSharedStateRegistry() {
        return sharedStateRegistry;
    }

    @Override
    public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        return KeyGroupRange.EMPTY_KEY_GROUP_RANGE.equals(
                        this.keyGroupRange.getIntersection(keyGroupRange))
                ? null
                : this;
    }

    @Override
    public StateHandleID getStateHandleId() {
        return stateHandleId;
    }

    @Override
    public void discardState() throws Exception {

        SharedStateRegistry registry = this.sharedStateRegistry;
        final boolean isRegistered = (registry != null);

        LOG.trace(
                "Discarding IncrementalRemoteKeyedStateHandle (registered = {}) for checkpoint {} from backend with id {}.",
                isRegistered,
                checkpointId,
                backendIdentifier);

        try {
            metaStateHandle.discardState();
        } catch (Exception e) {
            LOG.warn("Could not properly discard meta data.", e);
        }

        try {
            StateUtil.bestEffortDiscardAllStateObjects(
                    privateState.stream()
                            .map(HandleAndLocalPath::getHandle)
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            LOG.warn("Could not properly discard misc file states.", e);
        }

        // discard only on TM; on JM, shared state is removed on subsumption
        if (!isRegistered) {
            try {
                StateUtil.bestEffortDiscardAllStateObjects(
                        sharedState.stream()
                                .map(HandleAndLocalPath::getHandle)
                                .collect(Collectors.toList()));
            } catch (Exception e) {
                LOG.warn("Could not properly discard new sst file states.", e);
            }
        }
    }

    @Override
    public long getStateSize() {
        long size = StateUtil.getStateSize(metaStateHandle);

        for (HandleAndLocalPath handleAndLocalPath : sharedState) {
            size += handleAndLocalPath.getStateSize();
        }

        for (HandleAndLocalPath handleAndLocalPath : privateState) {
            size += handleAndLocalPath.getStateSize();
        }

        return size;
    }

    @Override
    public long getCheckpointedSize() {
        return persistedSizeOfThisCheckpoint;
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {

        // This is a quick check to avoid that we register twice with the same registry. However,
        // the code allows to
        // register again with a different registry. The implication is that ownership is
        // transferred to this new
        // registry. This should only happen in case of a restart, when the CheckpointCoordinator
        // creates a new
        // SharedStateRegistry for the current attempt and the old registry becomes meaningless. We
        // also assume that
        // an old registry object from a previous run is due to be GCed and will never be used for
        // registration again.
        Preconditions.checkState(
                sharedStateRegistry != stateRegistry,
                "The state handle has already registered its shared states to the given registry.");

        sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

        LOG.trace(
                "Registering IncrementalRemoteKeyedStateHandle for checkpoint {} from backend with id {}.",
                checkpointId,
                backendIdentifier);

        for (HandleAndLocalPath handleAndLocalPath : sharedState) {
            StreamStateHandle reference =
                    stateRegistry.registerReference(
                            SharedStateRegistryKey.forStreamStateHandle(
                                    handleAndLocalPath.getHandle()),
                            handleAndLocalPath.getHandle(),
                            checkpointID);

            // This step consolidates our shared handles with the registry, which will replace
            // placeholder state handle with already registered, actual state handles.
            // Because of SharedStateRegistryKey is based on the physical id of the stream handle,
            // no de-duplication will be performed. see FLINK-29913.
            handleAndLocalPath.replaceHandle(reference);
        }
    }

    @VisibleForTesting
    IncrementalRemoteKeyedStateHandle copy() {
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                privateState,
                metaStateHandle,
                persistedSizeOfThisCheckpoint,
                stateHandleId);
    }

    /**
     * This method is should only be called in tests! This should never serve as key in a hash map.
     */
    @VisibleForTesting
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IncrementalRemoteKeyedStateHandle that = (IncrementalRemoteKeyedStateHandle) o;

        if (!getStateHandleId().equals(that.getStateHandleId())) {
            return false;
        }
        if (getCheckpointId() != that.getCheckpointId()) {
            return false;
        }
        if (!getBackendIdentifier().equals(that.getBackendIdentifier())) {
            return false;
        }
        if (!getKeyGroupRange().equals(that.getKeyGroupRange())) {
            return false;
        }
        if (!getSharedState().equals(that.getSharedState())) {
            return false;
        }
        if (!getPrivateState().equals(that.getPrivateState())) {
            return false;
        }
        return getMetaStateHandle().equals(that.getMetaStateHandle());
    }

    /** This method should only be called in tests! This should never serve as key in a hash map. */
    @VisibleForTesting
    @Override
    public int hashCode() {
        int result = getBackendIdentifier().hashCode();
        result = 31 * result + getKeyGroupRange().hashCode();
        result = 31 * result + (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
        result = 31 * result + getSharedState().hashCode();
        result = 31 * result + getPrivateState().hashCode();
        result = 31 * result + getMetaStateHandle().hashCode();
        result = 31 * result + getStateHandleId().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "IncrementalRemoteKeyedStateHandle{"
                + "backendIdentifier="
                + backendIdentifier
                + ", stateHandleId="
                + stateHandleId
                + ", keyGroupRange="
                + keyGroupRange
                + ", checkpointId="
                + checkpointId
                + ", sharedState="
                + sharedState
                + ", privateState="
                + privateState
                + ", metaStateHandle="
                + metaStateHandle
                + ", registered="
                + (sharedStateRegistry != null)
                + '}';
    }
}
