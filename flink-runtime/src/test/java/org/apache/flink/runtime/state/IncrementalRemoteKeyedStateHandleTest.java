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

import org.apache.flink.runtime.checkpoint.metadata.CheckpointTestUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.TernaryBoolean;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.DiscardRecordedStateObject.verifyDiscard;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class IncrementalRemoteKeyedStateHandleTest {

    /**
     * This test checks, that for an unregistered {@link IncrementalRemoteKeyedStateHandle} all
     * state (including shared) is discarded.
     */
    @Test
    void testUnregisteredDiscarding() throws Exception {
        IncrementalRemoteKeyedStateHandle stateHandle = create(new Random(42));

        stateHandle.discardState();

        for (HandleAndLocalPath handleAndLocalPath : stateHandle.getPrivateState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.TRUE);
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.TRUE);
        }

        verify(stateHandle.getMetaDataStateHandle()).discardState();
    }

    /**
     * This test checks, that for a registered {@link IncrementalRemoteKeyedStateHandle} discards
     * respect all shared state and only discard it one all references are released.
     */
    @Test
    void testSharedStateDeRegistration() throws Exception {

        SharedStateRegistry registry = spy(new SharedStateRegistryImpl());

        // Create two state handles with overlapping shared state
        IncrementalRemoteKeyedStateHandle stateHandle1 = create(new Random(42));
        IncrementalRemoteKeyedStateHandle stateHandle2 = create(new Random(42));

        // Both handles should not be registered and not discarded by now.
        for (HandleAndLocalPath handleAndLocalPath : stateHandle1.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }
        for (HandleAndLocalPath handleAndLocalPath : stateHandle2.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }

        // Now we register both ...
        stateHandle1.registerSharedStates(registry, 0L);
        registry.checkpointCompleted(0L);
        stateHandle2.registerSharedStates(registry, 0L);

        for (HandleAndLocalPath handleAndLocalPath : stateHandle1.getSharedState()) {
            StreamStateHandle handle = handleAndLocalPath.getHandle();

            SharedStateRegistryKey registryKey =
                    SharedStateRegistryKey.forStreamStateHandle(handle);

            // stateHandle1 and stateHandle2 has same shared states, so same key register 2 times
            verify(registry, times(2)).registerReference(registryKey, handle, 0L);
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle2.getSharedState()) {
            StreamStateHandle handle = handleAndLocalPath.getHandle();

            SharedStateRegistryKey registryKey =
                    SharedStateRegistryKey.forStreamStateHandle(handle);

            // stateHandle1 and stateHandle2 has same shared states, so same key register 2 times
            verify(registry, times(2)).registerReference(registryKey, handle, 0L);
        }

        // We discard the first
        stateHandle1.discardState();

        // Should be unregistered, non-shared discarded, shared not discarded
        for (HandleAndLocalPath handleAndLocalPath : stateHandle1.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle2.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle1.getPrivateState()) {
            verify(handleAndLocalPath.getHandle(), times(1)).discardState();
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle2.getPrivateState()) {
            verify(handleAndLocalPath.getHandle(), times(0)).discardState();
        }

        verify(stateHandle1.getMetaDataStateHandle(), times(1)).discardState();
        verify(stateHandle2.getMetaDataStateHandle(), times(0)).discardState();

        // We discard the second
        stateHandle2.discardState();

        // Now everything should be unregistered and discarded
        registry.unregisterUnusedState(Long.MAX_VALUE);
        for (HandleAndLocalPath handleAndLocalPath : stateHandle1.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.TRUE);
        }

        for (HandleAndLocalPath handleAndLocalPath : stateHandle2.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.TRUE);
        }

        verify(stateHandle1.getMetaDataStateHandle(), times(1)).discardState();
        verify(stateHandle2.getMetaDataStateHandle(), times(1)).discardState();
    }

    /**
     * This tests that re-registration of shared state with another registry works as expected. This
     * simulates a recovery from a checkpoint, when the checkpoint coordinator creates a new shared
     * state registry and re-registers all live checkpoint states.
     */
    @Test
    void testSharedStateReRegistration() throws Exception {

        SharedStateRegistry stateRegistryA = spy(new SharedStateRegistryImpl());

        IncrementalRemoteKeyedStateHandle stateHandleX = create(new Random(1));
        IncrementalRemoteKeyedStateHandle stateHandleY = create(new Random(2));
        IncrementalRemoteKeyedStateHandle stateHandleZ = create(new Random(3));

        // Now we register first time ...
        stateHandleX.registerSharedStates(stateRegistryA, 0L);
        stateHandleY.registerSharedStates(stateRegistryA, 0L);
        stateHandleZ.registerSharedStates(stateRegistryA, 0L);

        // Second attempt should fail
        assertThatThrownBy(() -> stateHandleX.registerSharedStates(stateRegistryA, 0L))
                .withFailMessage("Should not be able to register twice with the same registry.")
                .isInstanceOf(IllegalStateException.class);

        // Everything should be discarded for this handle
        stateHandleZ.discardState();
        verify(stateHandleZ.getMetaDataStateHandle(), times(1)).discardState();

        // Close the first registry
        stateRegistryA.close();

        // Attempt to register to closed registry should trigger exception
        assertThatThrownBy(() -> create(new Random(4)).registerSharedStates(stateRegistryA, 0L))
                .withFailMessage("Should not be able to register new state to closed registry.")
                .isInstanceOf(IllegalStateException.class);

        // Private state should still get discarded
        stateHandleY.discardState();
        verify(stateHandleY.getMetaDataStateHandle(), times(1)).discardState();

        // This should still be unaffected
        verify(stateHandleX.getMetaDataStateHandle(), never()).discardState();

        // We re-register the handle with a new registry
        SharedStateRegistry sharedStateRegistryB = spy(new SharedStateRegistryImpl());
        stateHandleX.registerSharedStates(sharedStateRegistryB, 0L);
        stateHandleX.discardState();
        verify(stateHandleX.getMetaDataStateHandle(), times(1)).discardState();

        // Should be completely discarded because it is tracked through the new registry
        sharedStateRegistryB.unregisterUnusedState(1L);

        for (HandleAndLocalPath handleAndLocalPath : stateHandleX.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.TRUE);
        }
        for (HandleAndLocalPath handleAndLocalPath : stateHandleY.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }
        for (HandleAndLocalPath handleAndLocalPath : stateHandleZ.getSharedState()) {
            verifyDiscard(handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
        }
        sharedStateRegistryB.close();
    }

    @Test
    void testCheckpointedSize() {
        IncrementalRemoteKeyedStateHandle stateHandle1 = create(ThreadLocalRandom.current());
        assertThat(stateHandle1.getCheckpointedSize()).isEqualTo(stateHandle1.getStateSize());

        long checkpointedSize = 123L;
        IncrementalRemoteKeyedStateHandle stateHandle2 =
                create(ThreadLocalRandom.current(), checkpointedSize);
        assertThat(stateHandle2.getCheckpointedSize()).isEqualTo(checkpointedSize);
    }

    @Test
    void testNonEmptyIntersection() {
        IncrementalRemoteKeyedStateHandle handle = create(ThreadLocalRandom.current());

        KeyGroupRange expectedRange = new KeyGroupRange(0, 3);
        KeyedStateHandle newHandle = handle.getIntersection(expectedRange);
        assertThat(newHandle).isInstanceOf(IncrementalRemoteKeyedStateHandle.class);
        assertThat(newHandle.getStateHandleId()).isEqualTo(handle.getStateHandleId());
    }

    @Test
    void testConcurrentCheckpointSharedStateRegistration() throws Exception {
        String localPath = "1.sst";
        StreamStateHandle streamHandle1 = new ByteStreamStateHandle("file-1", new byte[] {'s'});
        StreamStateHandle streamHandle2 = new ByteStreamStateHandle("file-2", new byte[] {'s'});

        SharedStateRegistry registry = new SharedStateRegistryImpl();

        UUID backendID = UUID.randomUUID();

        IncrementalRemoteKeyedStateHandle handle1 =
                new IncrementalRemoteKeyedStateHandle(
                        backendID,
                        KeyGroupRange.of(0, 0),
                        1L,
                        placeSpies(
                                Collections.singletonList(
                                        HandleAndLocalPath.of(streamHandle1, localPath))),
                        Collections.emptyList(),
                        new ByteStreamStateHandle("", new byte[] {'s'}));

        handle1.registerSharedStates(registry, handle1.getCheckpointId());

        IncrementalRemoteKeyedStateHandle handle2 =
                new IncrementalRemoteKeyedStateHandle(
                        backendID,
                        KeyGroupRange.of(0, 0),
                        2L,
                        placeSpies(
                                Collections.singletonList(
                                        HandleAndLocalPath.of(streamHandle2, localPath))),
                        Collections.emptyList(),
                        new ByteStreamStateHandle("", new byte[] {'s'}));

        handle2.registerSharedStates(registry, handle2.getCheckpointId());

        registry.checkpointCompleted(1L);

        // checkpoint 2 failed
        handle2.discardState();

        for (HandleAndLocalPath handleAndLocalPath : handle1.getSharedState()) {
            verify(handleAndLocalPath.getHandle(), never()).discardState();
        }
        for (HandleAndLocalPath handleAndLocalPath : handle2.getSharedState()) {
            verify(handleAndLocalPath.getHandle(), never()).discardState();
        }
        registry.close();
    }

    private static IncrementalRemoteKeyedStateHandle create(Random rnd) {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.nameUUIDFromBytes("test".getBytes(StandardCharsets.UTF_8)),
                KeyGroupRange.of(0, 0),
                1L,
                // not place spies on shared state handle
                CheckpointTestUtils.createRandomHandleAndLocalPathList(rnd),
                placeSpies(CheckpointTestUtils.createRandomHandleAndLocalPathList(rnd)),
                spy(CheckpointTestUtils.createDummyStreamStateHandle(rnd, null)));
    }

    private static IncrementalRemoteKeyedStateHandle create(Random rnd, long checkpointedSize) {
        return new IncrementalRemoteKeyedStateHandle(
                UUID.nameUUIDFromBytes("test".getBytes()),
                KeyGroupRange.of(0, 0),
                1L,
                // not place spies on shared state handle
                CheckpointTestUtils.createRandomHandleAndLocalPathList(rnd),
                placeSpies(CheckpointTestUtils.createRandomHandleAndLocalPathList(rnd)),
                spy(CheckpointTestUtils.createDummyStreamStateHandle(rnd, null)),
                checkpointedSize);
    }

    private static List<HandleAndLocalPath> placeSpies(List<HandleAndLocalPath> list) {
        return list.stream()
                .map(e -> HandleAndLocalPath.of(spy(e.getHandle()), e.getLocalPath()))
                .collect(Collectors.toList());
    }
}
