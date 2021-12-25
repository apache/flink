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
            (deleteExecutor, checkpoints) -> {
                SharedStateRegistry sharedStateRegistry =
                        new SharedStateRegistryImpl(deleteExecutor);
                for (CompletedCheckpoint checkpoint : checkpoints) {
                    checkpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
                }
                return sharedStateRegistry;
            };

    /**
     * Register a reference to the given shared state in the registry. If there is already a state
     * handle registered under the given key, the "new" state handle is disposed .
     *
     * <p>IMPORTANT: caller should check the state handle returned by the result, because the
     * registry is performing de-duplication and could potentially return a handle that is supposed
     * to replace the one from the registration request.
     *
     * @param state the shared state for which we register a reference.
     * @param checkpointID which uses the state
     * @return the result of this registration request, consisting of the state handle that is
     *     registered under the key by the end of the operation and its current reference count.
     */
    StreamStateHandle registerReference(
            SharedStateRegistryKey registrationKey, StreamStateHandle state, long checkpointID);
    /**
     * Unregister state that is not referenced by the given checkpoint ID or any newer.
     *
     * @param lowestCheckpointID which is still valid
     */
    void unregisterUnusedState(long lowestCheckpointID);

    /**
     * Register given shared states in the registry.
     *
     * @param stateHandles The shared states to register.
     * @param checkpointID which uses the states.
     */
    void registerAll(Iterable<? extends CompositeStateHandle> stateHandles, long checkpointID);

    void checkpointCompleted(long checkpointId);
}
