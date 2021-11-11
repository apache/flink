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
import org.apache.flink.util.Preconditions;

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
                    sharedStateRegistry.registerAll(checkpoint.getOperatorStates().values());
                }
                return sharedStateRegistry;
            };

    /**
     * Register a reference to the given shared state in the registry. This does the following: We
     * check if the state handle is actually new by the registrationKey. If it is new, we register
     * it with a reference count of 1. If there is already a state handle registered under the given
     * key, we dispose the given "new" state handle, uptick the reference count of the previously
     * existing state handle and return it as a replacement with the result.
     *
     * <p>IMPORTANT: caller should check the state handle returned by the result, because the
     * registry is performing de-duplication and could potentially return a handle that is supposed
     * to replace the one from the registration request.
     *
     * @param state the shared state for which we register a reference.
     * @return the result of this registration request, consisting of the state handle that is
     *     registered under the key by the end of the operation and its current reference count.
     */
    Result registerReference(SharedStateRegistryKey registrationKey, StreamStateHandle state);

    /**
     * Releases one reference to the given shared state in the registry. This decreases the
     * reference count by one. Once the count reaches zero, the shared state is deleted.
     *
     * @param registrationKey the shared state for which we release a reference.
     * @return the result of the request, consisting of the reference count after this operation and
     *     the state handle, or null if the state handle was deleted through this request. Returns
     *     null if the registry was previously closed.
     */
    SharedStateRegistry.Result unregisterReference(SharedStateRegistryKey registrationKey);

    /**
     * Register given shared states in the registry.
     *
     * @param stateHandles The shared states to register.
     */
    void registerAll(Iterable<? extends CompositeStateHandle> stateHandles);

    /** An entry in the registry, tracking the handle and the corresponding reference count. */
    class SharedStateEntry {

        /** The shared state handle */
        public final StreamStateHandle stateHandle;

        /** The current reference count of the state handle */
        private int referenceCount;

        public SharedStateEntry(StreamStateHandle value) {
            this.stateHandle = value;
            this.referenceCount = 1;
        }

        public StreamStateHandle getStateHandle() {
            return stateHandle;
        }

        public int getReferenceCount() {
            return referenceCount;
        }

        public void increaseReferenceCount() {
            ++referenceCount;
        }

        public void decreaseReferenceCount() {
            --referenceCount;
        }

        @Override
        public String toString() {
            return "SharedStateEntry{"
                    + "stateHandle="
                    + stateHandle
                    + ", referenceCount="
                    + referenceCount
                    + '}';
        }
    }

    /** The result of an attempt to (un)/reference state */
    class Result {

        /** The (un)registered state handle from the request */
        private final StreamStateHandle reference;

        /** The reference count to the state handle after the request to (un)register */
        private final int referenceCount;

        public Result(SharedStateEntry sharedStateEntry) {
            this.reference = sharedStateEntry.getStateHandle();
            this.referenceCount = sharedStateEntry.getReferenceCount();
        }

        public Result(StreamStateHandle reference, int referenceCount) {
            Preconditions.checkArgument(referenceCount >= 0);

            this.reference = reference;
            this.referenceCount = referenceCount;
        }

        public StreamStateHandle getReference() {
            return reference;
        }

        public int getReferenceCount() {
            return referenceCount;
        }

        @Override
        public String toString() {
            return "Result{" + "reference=" + reference + '}';
        }
    }
}
