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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A handle to ChangelogStateBackend state. Consists of the base and delta parts. Base part
 * references materialized state (e.g. SST files), while delta part references state changes that
 * were not not materialized at the time of the snapshot. Both are potentially empty lists as there
 * can be no state or multiple states (e.g. after rescaling).
 */
@Internal
public interface ChangelogStateBackendHandle extends KeyedStateHandle {
    List<KeyedStateHandle> getMaterializedStateHandles();

    List<ChangelogStateHandle> getNonMaterializedStateHandles();

    long getMaterializationID();

    class ChangelogStateBackendHandleImpl implements ChangelogStateBackendHandle {
        private static final long serialVersionUID = 1L;

        private final List<KeyedStateHandle> materialized;
        private final List<ChangelogStateHandle> nonMaterialized;
        private final KeyGroupRange keyGroupRange;

        private final long materializationID;
        private final long persistedSizeOfThisCheckpoint;
        private final StateHandleID stateHandleID;

        public ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long materializationID,
                long persistedSizeOfThisCheckpoint) {
            this(
                    materialized,
                    nonMaterialized,
                    keyGroupRange,
                    materializationID,
                    persistedSizeOfThisCheckpoint,
                    StateHandleID.randomStateHandleId());
        }

        private ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long materializationID,
                long persistedSizeOfThisCheckpoint,
                StateHandleID stateHandleId) {
            this.materialized = unmodifiableList(materialized);
            this.nonMaterialized = unmodifiableList(nonMaterialized);
            this.keyGroupRange = keyGroupRange;
            this.persistedSizeOfThisCheckpoint = persistedSizeOfThisCheckpoint;
            checkArgument(keyGroupRange.getNumberOfKeyGroups() > 0);
            this.materializationID = materializationID;
            this.stateHandleID = stateHandleId;
        }

        public static ChangelogStateBackendHandleImpl restore(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long materializationID,
                long persistedSizeOfThisCheckpoint,
                StateHandleID stateHandleId) {
            return new ChangelogStateBackendHandleImpl(
                    materialized,
                    nonMaterialized,
                    keyGroupRange,
                    materializationID,
                    persistedSizeOfThisCheckpoint,
                    stateHandleId);
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
            for (KeyedStateHandle keyedStateHandle : materialized) {
                // Use the unique and invariant UUID as the state registry key for a specific keyed
                // state handle. To avoid unexpected unregister, this registry key would not change
                // even rescaled.
                stateRegistry.registerReference(
                        new SharedStateRegistryKey(keyedStateHandle.getStateHandleId().toString()),
                        new StreamStateHandleWrapper(keyedStateHandle),
                        checkpointID);
            }
            stateRegistry.registerAll(materialized, checkpointID);
            stateRegistry.registerAll(nonMaterialized, checkpointID);
        }

        @Override
        public void discardState() throws Exception {
            // Do nothing: state will be discarded by SharedStateRegistry once JM receives it and a
            // newer checkpoint completes without using it.
            // if the checkpoints always failed, it would leave orphan files there.
            // The above cases will be addressed by FLINK-23139 and/or FLINK-24852.
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Nullable
        @Override
        public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
            // todo: revisit/review
            KeyGroupRange intersection = this.keyGroupRange.getIntersection(keyGroupRange);
            if (intersection.getNumberOfKeyGroups() == 0) {
                return null;
            }
            List<KeyedStateHandle> basePart =
                    this.materialized.stream()
                            .map(entry -> entry.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            List<ChangelogStateHandle> deltaPart =
                    this.nonMaterialized.stream()
                            .map(
                                    handle ->
                                            (ChangelogStateHandle)
                                                    handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new ChangelogStateBackendHandleImpl(
                    basePart,
                    deltaPart,
                    intersection,
                    materializationID,
                    persistedSizeOfThisCheckpoint);
        }

        @Override
        public StateHandleID getStateHandleId() {
            return stateHandleID;
        }

        @Override
        public long getStateSize() {
            return materialized.stream().mapToLong(StateObject::getStateSize).sum()
                    + nonMaterialized.stream().mapToLong(StateObject::getStateSize).sum();
        }

        @Override
        public long getCheckpointedSize() {
            return persistedSizeOfThisCheckpoint;
        }

        @Override
        public List<KeyedStateHandle> getMaterializedStateHandles() {
            return materialized;
        }

        @Override
        public List<ChangelogStateHandle> getNonMaterializedStateHandles() {
            return nonMaterialized;
        }

        @Override
        public long getMaterializationID() {
            return materializationID;
        }

        @Override
        public String toString() {
            return String.format(
                    "keyGroupRange=%s, basePartSize=%d, deltaPartSize=%d",
                    keyGroupRange, materialized.size(), nonMaterialized.size());
        }

        /**
         * This wrapper class is introduced as current {@link SharedStateRegistry} only accept
         * StreamStateHandle to register, remove it once FLINK-25862 is resolved.
         */
        private static class StreamStateHandleWrapper implements StreamStateHandle {
            private static final long serialVersionUID = 1L;

            private final KeyedStateHandle keyedStateHandle;

            StreamStateHandleWrapper(KeyedStateHandle keyedStateHandle) {
                this.keyedStateHandle = keyedStateHandle;
            }

            @Override
            public void discardState() throws Exception {
                keyedStateHandle.discardState();
            }

            @Override
            public long getStateSize() {
                return keyedStateHandle.getStateSize();
            }

            @Override
            public FSDataInputStream openInputStream() throws IOException {
                throw new UnsupportedOperationException("Should not call here.");
            }

            @Override
            public Optional<byte[]> asBytesIfInMemory() {
                throw new UnsupportedOperationException("Should not call here.");
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                StreamStateHandleWrapper that = (StreamStateHandleWrapper) o;
                return Objects.equals(
                        keyedStateHandle.getStateHandleId(),
                        that.keyedStateHandle.getStateHandleId());
            }

            @Override
            public int hashCode() {
                return Objects.hash(keyedStateHandle.getStateHandleId());
            }

            @Override
            public String toString() {
                return "Wrapped{" + keyedStateHandle + '}';
            }
        }
    }
}
