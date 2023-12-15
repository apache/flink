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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.StateHandleDummyUtil;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointTestUtils;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryChangelogStateHandle;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/** Test utils for changelog * */
public class ChangelogTestUtils {

    public static ChangelogStateBackendHandle createChangelogStateBackendHandle() {
        return createChangelogStateBackendHandle(
                StateHandleDummyUtil.createNewKeyedStateHandle(new KeyGroupRange(0, 1)));
    }

    public static ChangelogStateBackendHandle createChangelogStateBackendHandle(
            KeyedStateHandle keyedStateHandle) {
        return new ChangelogStateBackendHandleImpl(
                Collections.singletonList(keyedStateHandle),
                Collections.emptyList(),
                new KeyGroupRange(0, 1),
                1L,
                1L,
                0L);
    }

    public static IncrementalStateHandleWrapper createDummyIncrementalStateHandle(
            long checkpointId) {
        return new IncrementalStateHandleWrapper(
                CheckpointTestUtils.createDummyIncrementalKeyedStateHandle(
                        checkpointId, ThreadLocalRandom.current()));
    }

    public static ChangelogStateHandleWrapper createDummyChangelogStateHandle(long from, long to) {
        return new ChangelogStateHandleWrapper(
                new InMemoryChangelogStateHandle(
                        Collections.emptyList(),
                        SequenceNumber.of(from),
                        SequenceNumber.of(to),
                        new KeyGroupRange(1, 1)));
    }

    public static class IncrementalStateHandleWrapper extends IncrementalRemoteKeyedStateHandle {
        private static final long serialVersionUID = 1L;
        private final IncrementalRemoteKeyedStateHandle stateHandle;
        private volatile boolean isDiscarded;

        IncrementalStateHandleWrapper(IncrementalRemoteKeyedStateHandle stateHandle) {
            super(
                    stateHandle.getBackendIdentifier(),
                    stateHandle.getKeyGroupRange(),
                    stateHandle.getCheckpointId(),
                    stateHandle.getSharedState(),
                    stateHandle.getPrivateState(),
                    stateHandle.getMetaDataStateHandle(),
                    stateHandle.getCheckpointedSize(),
                    stateHandle.getStateHandleId());
            this.stateHandle = stateHandle;
        }

        @Override
        public void discardState() throws Exception {
            super.discardState();
            isDiscarded = true;
        }

        boolean isDiscarded() {
            return isDiscarded;
        }

        IncrementalStateHandleWrapper deserialize() {
            return new IncrementalStateHandleWrapper(stateHandle.copy());
        }

        @Override
        public boolean equals(Object o) {
            // override original IncrementalRemoteKeyedStateHandle#equals() method via comparing
            // the memory address directly. This is to ensure state handle generated via
            // #deserialize is different from the original one, which let the
            // SharedStateRegistryImpl treat them are different via Objects#equals.
            // More information can refer to FLINK-26101.
            return (this == o);
        }
    }

    public static class ChangelogStateHandleWrapper extends InMemoryChangelogStateHandle
            implements TestStreamStateHandle {
        private static final long serialVersionUID = 1L;
        private volatile boolean isDiscarded;

        public ChangelogStateHandleWrapper(InMemoryChangelogStateHandle stateHandle) {
            super(
                    stateHandle.getChanges(),
                    SequenceNumber.of(stateHandle.getFrom()),
                    SequenceNumber.of(stateHandle.getTo()),
                    stateHandle.getKeyGroupRange());
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
            // unlike original InMemoryChangelogStateHandle, register the reference here
            // to verify #isDiscarded
            stateRegistry.registerReference(getSharedStateRegistryKey(), this, checkpointID);
        }

        private SharedStateRegistryKey getSharedStateRegistryKey() {
            return new SharedStateRegistryKey(getKeyGroupRange() + "_" + getFrom() + "_" + getTo());
        }

        @Override
        public void discardState() {
            super.discardState();
            isDiscarded = true;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            throw new UnsupportedOperationException();
        }

        boolean isDiscarded() {
            return isDiscarded;
        }
    }
}
