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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
import static org.apache.flink.runtime.state.ChangelogTestUtils.ChangelogStateHandleWrapper;
import static org.apache.flink.runtime.state.ChangelogTestUtils.IncrementalStateHandleWrapper;
import static org.apache.flink.runtime.state.ChangelogTestUtils.createDummyChangelogStateHandle;
import static org.apache.flink.runtime.state.ChangelogTestUtils.createDummyIncrementalStateHandle;
import static org.assertj.core.api.Assertions.assertThat;

class SharedStateRegistryTest {
    private static final String RESTORED_STATE_ID = "restored-state";

    /** Validate that all states can be correctly registered at the registry. */
    @Test
    void testRegistryNormal() {

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        // register one state
        TestSharedState firstState = new TestSharedState("first");
        StreamStateHandle result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstState, 0L);
        assertThat(result).isSameAs(firstState);
        assertThat(firstState.isDiscarded()).isFalse();

        // register another state
        TestSharedState secondState = new TestSharedState("second");
        result =
                sharedStateRegistry.registerReference(
                        secondState.getRegistrationKey(), secondState, 0L);
        assertThat(result).isSameAs(secondState);
        assertThat(firstState.isDiscarded()).isFalse();
        assertThat(secondState.isDiscarded()).isFalse();

        sharedStateRegistry.unregisterUnusedState(1L);
        assertThat(secondState.isDiscarded()).isTrue();
        assertThat(firstState.isDiscarded()).isTrue();
    }

    /** Validate that unregister a nonexistent checkpoint will not throw exception */
    @Test
    void testUnregisterWithUnexistedKey() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        sharedStateRegistry.unregisterUnusedState(-1);
        sharedStateRegistry.unregisterUnusedState(Long.MAX_VALUE);
    }

    @Test
    void testRegisterChangelogStateBackendHandles() throws InterruptedException {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        long materializationId1 = 1L;
        // the base materialized state on TM side
        ChangelogTestUtils.IncrementalStateHandleWrapper materializedStateBase1 =
                createDummyIncrementalStateHandle(materializationId1);

        // we deserialize the state handle due to FLINK-25479 to mock on JM side
        IncrementalStateHandleWrapper materializedState1 = materializedStateBase1.deserialize();
        ChangelogStateHandleWrapper nonMaterializedState1 = createDummyChangelogStateHandle(1, 2);
        long materializationId = 1L;
        long checkpointId1 = 41;
        ChangelogStateBackendHandleImpl changelogStateBackendHandle1 =
                new ChangelogStateBackendHandleImpl(
                        Collections.singletonList(materializedState1),
                        Collections.singletonList(nonMaterializedState1),
                        materializedStateBase1.getKeyGroupRange(),
                        checkpointId1,
                        materializationId,
                        nonMaterializedState1.getStateSize());
        changelogStateBackendHandle1.registerSharedStates(sharedStateRegistry, checkpointId1);
        sharedStateRegistry.checkpointCompleted(checkpointId1);
        sharedStateRegistry.unregisterUnusedState(checkpointId1);

        IncrementalStateHandleWrapper materializedState2 = materializedStateBase1.deserialize();
        ChangelogStateHandleWrapper nonMaterializedState2 = createDummyChangelogStateHandle(2, 3);
        long checkpointId2 = 42;
        ChangelogStateBackendHandleImpl changelogStateBackendHandle2 =
                new ChangelogStateBackendHandleImpl(
                        Collections.singletonList(materializedState2),
                        Collections.singletonList(nonMaterializedState2),
                        materializedStateBase1.getKeyGroupRange(),
                        checkpointId2,
                        materializationId,
                        nonMaterializedState2.getStateSize());
        changelogStateBackendHandle2.registerSharedStates(sharedStateRegistry, checkpointId2);
        sharedStateRegistry.checkpointCompleted(checkpointId2);
        sharedStateRegistry.unregisterUnusedState(checkpointId2);

        // the 1st materialized state would not be discarded since the 2nd changelog state backend
        // handle still use it.
        assertThat(materializedState1.isDiscarded()).isFalse();
        // FLINK-26101, check whether the multi registered state not discarded.
        assertThat(materializedState2.isDiscarded()).isFalse();
        assertThat(nonMaterializedState1.isDiscarded()).isTrue();

        long materializationId2 = 2L;
        IncrementalStateHandleWrapper materializedStateBase2 =
                createDummyIncrementalStateHandle(materializationId2);

        IncrementalStateHandleWrapper materializedState3 = materializedStateBase2.deserialize();
        long checkpointId3 = 43L;
        ChangelogStateBackendHandleImpl changelogStateBackendHandle3 =
                new ChangelogStateBackendHandleImpl(
                        Collections.singletonList(materializedState3),
                        Collections.singletonList(nonMaterializedState2),
                        materializedState3.getKeyGroupRange(),
                        checkpointId3,
                        materializationId2,
                        0L);
        changelogStateBackendHandle3.registerSharedStates(sharedStateRegistry, checkpointId3);
        sharedStateRegistry.checkpointCompleted(checkpointId3);
        sharedStateRegistry.unregisterUnusedState(checkpointId3);

        // the 1st materialized state would be discarded since we have a newer materialized
        // state registered.
        assertThat(materializedState1.isDiscarded()).isTrue();
        // the 2nd non-materialized state would not be discarded as 3rd changelog state backend
        // handle still use it.
        assertThat(nonMaterializedState2.isDiscarded()).isFalse();
    }

    @Test
    void testUnregisterUnusedSavepointState() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        TestingStreamStateHandle handle = new TestingStreamStateHandle();

        registerInitialCheckpoint(
                sharedStateRegistry,
                RESTORED_STATE_ID,
                CheckpointProperties.forSavepoint(false, SavepointFormatType.NATIVE));

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID), handle, 2L);
        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID), handle, 3L);
        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey("new-state"), new TestingStreamStateHandle(), 4L);

        assertThat(sharedStateRegistry.unregisterUnusedState(3))
                .withFailMessage(
                        "Only the initial checkpoint should be retained because its state is in use")
                .containsExactly(1L);
        assertThat(sharedStateRegistry.unregisterUnusedState(4))
                .withFailMessage("The initial checkpoint state is unused so it could be discarded")
                .isEmpty();
    }

    @Test
    void testUnregisterNonInitialCheckpoint() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        String stateId = "stateId";
        byte[] stateContent = stateId.getBytes(StandardCharsets.UTF_8);

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(stateId),
                new TestingStreamStateHandle(stateId, stateContent),
                1L);
        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(stateId),
                new TestingStreamStateHandle(stateId, stateContent),
                2L);
        assertThat(sharedStateRegistry.unregisterUnusedState(2))
                .withFailMessage("First (non-initial) checkpoint could be discarded")
                .isEmpty();
    }

    @Test
    void testUnregisterInitialCheckpoint() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        TestingStreamStateHandle handle = new TestingStreamStateHandle();

        registerInitialCheckpoint(
                sharedStateRegistry,
                RESTORED_STATE_ID,
                CheckpointProperties.forCheckpoint(RETAIN_ON_CANCELLATION));

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID), handle, 2L);

        assertThat(sharedStateRegistry.unregisterUnusedState(2))
                .withFailMessage(
                        "(retained) checkpoint - should NOT be considered in use even if its state is in use")
                .isEmpty();
    }

    /** Emulate turning changelog on while recovering from a retained checkpoint. */
    @Test
    void testUnregisterInitialCheckpointUsedInChangelog() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        TestingStreamStateHandle handle = new TestingStreamStateHandle();

        // "normal" restored checkpoint
        registerInitialCheckpoint(
                sharedStateRegistry,
                RESTORED_STATE_ID,
                CheckpointProperties.forCheckpoint(RETAIN_ON_CANCELLATION));

        // "changelog" checkpoint wrapping some initial state
        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID),
                handle,
                2L,
                true /* should prevent deletion */);

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID),
                handle,
                3L,
                false /* should NOT change anything - deletion should still be prevented */);

        assertThat(sharedStateRegistry.unregisterUnusedState(3))
                .withFailMessage(
                        "(retained) checkpoint - should be considered in use as long as its state is in use by changelog")
                .containsExactly(1L);
    }

    private void registerInitialCheckpoint(
            SharedStateRegistry sharedStateRegistry,
            String stateId,
            CheckpointProperties properties) {
        IncrementalRemoteKeyedStateHandle initialHandle =
                IncrementalRemoteKeyedStateHandle.restore(
                        UUID.randomUUID(),
                        KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                        1L,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new ByteStreamStateHandle("meta", new byte[1]),
                        1024L,
                        new StateHandleID(stateId));

        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 1, 1);
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedKeyedState(initialHandle).build());

        sharedStateRegistry.registerAllAfterRestored(
                new CompletedCheckpoint(
                        new JobID(),
                        1L,
                        1L,
                        1L,
                        Collections.singletonMap(operatorID, operatorState),
                        Collections.emptyList(),
                        CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null,
                        properties),
                RestoreMode.DEFAULT);
    }

    private static class TestSharedState implements TestStreamStateHandle {
        private static final long serialVersionUID = 4468635881465159780L;

        private SharedStateRegistryKey key;

        private boolean discarded;

        TestSharedState(String key) {
            this.key = new SharedStateRegistryKey(key);
            this.discarded = false;
        }

        public SharedStateRegistryKey getRegistrationKey() {
            return key;
        }

        @Override
        public void discardState() throws Exception {
            this.discarded = true;
        }

        @Override
        public long getStateSize() {
            return key.toString().length();
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        public boolean isDiscarded() {
            return discarded;
        }
    }
}
