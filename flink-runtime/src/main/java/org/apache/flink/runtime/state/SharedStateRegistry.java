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

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.jobgraph.RestoreMode;

import java.util.Set;

/**
 * This registry manages state that is shared across (incremental) checkpoints, and is responsible
 * for deleting shared state that is no longer used in any valid checkpoint.
 *
 * <p>A {@code SharedStateRegistry} will be deployed in the {@link
 * org.apache.flink.runtime.checkpoint.CheckpointCoordinator CheckpointCoordinator} to keep track of
 * usage of {@link StreamStateHandle}s by a key that (logically) identifies them.
 */
public interface SharedStateRegistry extends AutoCloseable {

    /** A singleton object for the default implementation of a {@link SharedStateRegistryFactory} */
    SharedStateRegistryFactory DEFAULT_FACTORY =
            (deleteExecutor, checkpoints, restoreMode) -> {
                SharedStateRegistry sharedStateRegistry =
                        new SharedStateRegistryImpl(deleteExecutor);
                for (CompletedCheckpoint checkpoint : checkpoints) {
                    checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry, restoreMode);
                }
                return sharedStateRegistry;
            };

    /**
     * Shortcut for {@link #registerReference(SharedStateRegistryKey, StreamStateHandle, long,
     * boolean)} with preventDiscardingCreatedCheckpoint = false.
     */
    default StreamStateHandle registerReference(
            SharedStateRegistryKey registrationKey, StreamStateHandle state, long checkpointID) {
        return registerReference(registrationKey, state, checkpointID, false);
    }

    /**
     * Register a reference to the given shared state in the registry. The registry key should be
     * based on the physical identifier of the state. If there is already a state handle registered
     * under the same key and the 'new' state is not equal to the old one, an exception will be
     * thrown.
     *
     * <p>IMPORTANT: the caller must use the returned state handle instead of the passed one because
     * the registry might replace or update it.
     *
     * @param state the shared state for which we register a reference.
     * @param checkpointID which uses the state
     * @param preventDiscardingCreatedCheckpoint as long as this state is still in use. The
     *     "checkpoint that created the state" is recorded on the first state registration.
     * @return the state handle registered under the given key. It might differ from the passed
     *     state handle, e.g. if it was a placeholder.
     */
    StreamStateHandle registerReference(
            SharedStateRegistryKey registrationKey,
            StreamStateHandle state,
            long checkpointID,
            boolean preventDiscardingCreatedCheckpoint);

    /**
     * Unregister state that is not referenced by the given checkpoint ID or any newer.
     *
     * @param lowestCheckpointID which is still valid.
     * @return a set of checkpointID which is still in use.
     */
    Set<Long> unregisterUnusedState(long lowestCheckpointID);

    /**
     * Register given shared states in the registry.
     *
     * <p>NOTE: For state from checkpoints from other jobs or runs (i.e. after recovery), please use
     * {@link #registerAllAfterRestored(CompletedCheckpoint, RestoreMode)}
     *
     * @param stateHandles The shared states to register.
     * @param checkpointID which uses the states.
     */
    void registerAll(Iterable<? extends CompositeStateHandle> stateHandles, long checkpointID);

    /**
     * Set the lowest checkpoint ID below which no state is discarded, inclusive.
     *
     * <p>After recovery from an incremental checkpoint, its state should NOT be discarded, even if
     * {@link #unregisterUnusedState(long) not used} anymore (unless recovering in {@link
     * RestoreMode#CLAIM CLAIM} mode).
     *
     * <p>This should hold for both cases: when recovering from that initial checkpoint; and from
     * any subsequent checkpoint derived from it.
     */
    void registerAllAfterRestored(CompletedCheckpoint checkpoint, RestoreMode mode);

    void checkpointCompleted(long checkpointId);
}
