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

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static java.util.Collections.singleton;
import static junit.framework.TestCase.assertFalse;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
import static org.apache.flink.runtime.state.ChangelogTestUtils.ChangelogStateHandleWrapper;
import static org.apache.flink.runtime.state.ChangelogTestUtils.IncrementalStateHandleWrapper;
import static org.apache.flink.runtime.state.ChangelogTestUtils.createDummyChangelogStateHandle;
import static org.apache.flink.runtime.state.ChangelogTestUtils.createDummyIncrementalStateHandle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SharedStateRegistryTest {
    private static final String RESTORED_STATE_ID = "restored-state";

    /** Validate that all states can be correctly registered at the registry. */
    @Test
    public void testRegistryNormal() {

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        // register one state
        TestSharedState firstState = new TestSharedState("first");
        StreamStateHandle result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstState, 0L);
        assertTrue(firstState == result);
        assertFalse(firstState.isDiscarded());

        // register another state
        TestSharedState secondState = new TestSharedState("second");
        result =
                sharedStateRegistry.registerReference(
                        secondState.getRegistrationKey(), secondState, 0L);
        assertTrue(secondState == result);
        assertFalse(firstState.isDiscarded());
        assertFalse(secondState.isDiscarded());

        // attempt to register state under an existing key - before CP completion
        // new state should replace the old one
        TestSharedState firstStatePrime =
                new TestSharedState(firstState.getRegistrationKey().getKeyString());
        result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstStatePrime, 0L);
        assertTrue(firstStatePrime == result);
        assertFalse(firstStatePrime.isDiscarded());
        assertFalse(firstState == result);
        assertTrue(firstState.isDiscarded());

        // attempt to register state under an existing key - after CP completion
        // new state should be discarded
        sharedStateRegistry.checkpointCompleted(0L);
        TestSharedState firstStateDPrime =
                new TestSharedState(firstState.getRegistrationKey().getKeyString());
        result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstStateDPrime, 0L);
        assertFalse(firstStateDPrime == result);
        assertTrue(firstStateDPrime.isDiscarded());
        assertTrue(firstStatePrime == result);
        assertFalse(firstStatePrime.isDiscarded());

        // reference the first state again
        result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstState, 0L);
        assertTrue(firstStatePrime == result);
        assertFalse(firstStatePrime.isDiscarded());

        sharedStateRegistry.unregisterUnusedState(1L);
        assertTrue(secondState.isDiscarded());
        assertTrue(firstState.isDiscarded());
    }

    /** Validate that unregister a nonexistent checkpoint will not throw exception */
    @Test
    public void testUnregisterWithUnexistedKey() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        sharedStateRegistry.unregisterUnusedState(-1);
        sharedStateRegistry.unregisterUnusedState(Long.MAX_VALUE);
    }

    @Test
    public void testRegisterChangelogStateBackendHandles() throws InterruptedException {
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
        assertFalse(materializedState1.isDiscarded());
        // FLINK-26101, check whether the multi registered state not discarded.
        assertFalse(materializedState2.isDiscarded());
        assertTrue(nonMaterializedState1.isDiscarded());

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
        assertTrue(materializedState1.isDiscarded());
        // the 2nd non-materialized state would not be discarded as 3rd changelog state backend
        // handle still use it.
        assertFalse(nonMaterializedState2.isDiscarded());
    }

    @Test
    public void testUnregisterUnusedSavepointState() {
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

        assertEquals(
                "Only the initial checkpoint should be retained because its state is in use",
                singleton(1L),
                sharedStateRegistry.unregisterUnusedState(3));
        assertTrue(
                "The initial checkpoint state is unused so it could be discarded",
                sharedStateRegistry.unregisterUnusedState(4).isEmpty());
    }

    @Test
    public void testUnregisterNonInitialCheckpoint() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey("stateId"), new TestingStreamStateHandle(), 1L);
        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey("stateId"), new TestingStreamStateHandle(), 2L);
        assertTrue(
                "First (non-initial) checkpoint could be discarded",
                sharedStateRegistry.unregisterUnusedState(2).isEmpty());
    }

    @Test
    public void testUnregisterInitialCheckpoint() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl();
        TestingStreamStateHandle handle = new TestingStreamStateHandle();

        registerInitialCheckpoint(
                sharedStateRegistry,
                RESTORED_STATE_ID,
                CheckpointProperties.forCheckpoint(RETAIN_ON_CANCELLATION));

        sharedStateRegistry.registerReference(
                new SharedStateRegistryKey(RESTORED_STATE_ID), handle, 2L);

        assertTrue(
                "(retained) checkpoint - should NOT be considered in use even if its state is in use",
                sharedStateRegistry.unregisterUnusedState(2).isEmpty());
    }

    /** Emulate turning changelog on while recovering from a retained checkpoint. */
    @Test
    public void testUnregisterInitialCheckpointUsedInChangelog() {
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

        assertEquals(
                "(retained) checkpoint - should be considered in use as long as its state is in use by changelog",
                singleton(1L),
                sharedStateRegistry.unregisterUnusedState(3));
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
                        Collections.emptyMap(),
                        Collections.emptyMap(),
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
