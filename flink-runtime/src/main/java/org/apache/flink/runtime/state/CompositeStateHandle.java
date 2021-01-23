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

/**
 * Base of all snapshots that are taken by {@link StateBackend}s and some other components in tasks.
 *
 * <p>Each snapshot is composed of a collection of {@link StateObject}s some of which may be
 * referenced by other checkpoints. The shared states will be registered at the given {@link
 * SharedStateRegistry} when the handle is received by the {@link
 * org.apache.flink.runtime.checkpoint.CheckpointCoordinator} and will be discarded when the
 * checkpoint is discarded.
 *
 * <p>The {@link SharedStateRegistry} is responsible for the discarding of registered shared states.
 * Before their first registration through {@link #registerSharedStates(SharedStateRegistry)}, newly
 * created shared state is still owned by this handle and considered as private state until it is
 * registered for the first time. Registration transfers ownership to the {@link
 * SharedStateRegistry}. The composite state handle should only delete all private states in the
 * {@link StateObject#discardState()} method, the {@link SharedStateRegistry} is responsible for
 * deleting shared states after they were registered.
 */
public interface CompositeStateHandle extends StateObject {

    /**
     * Register both newly created and already referenced shared states in the given {@link
     * SharedStateRegistry}. This method is called when the checkpoint successfully completes or is
     * recovered from failures.
     *
     * <p>After this is completed, newly created shared state is considered as published is no
     * longer owned by this handle. This means that it should no longer be deleted as part of calls
     * to {@link #discardState()}. Instead, {@link #discardState()} will trigger an unregistration
     * from the registry.
     *
     * @param stateRegistry The registry where shared states are registered.
     */
    void registerSharedStates(SharedStateRegistry stateRegistry);
}
