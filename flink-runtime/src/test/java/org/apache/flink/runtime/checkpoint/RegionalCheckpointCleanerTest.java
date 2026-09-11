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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for regional checkpoint reference protection in the checkpoint cleaner. Verifies that
 * checkpoints transitively referenced via {@code refCheckpointId} are not subsumed.
 */
class RegionalCheckpointCleanerTest {

    private TestingStateHandleStore.Builder<CompletedCheckpoint> builder;
    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        builder = TestingStateHandleStore.newBuilder();
        executorService = Executors.newFixedThreadPool(2, new ExecutorThreadFactory("IO-Executor"));
    }

    @AfterEach
    void after() {
        executorService.shutdownNow();
    }

    @Test
    void testReferencedCheckpointNotCleaned() throws Exception {
        // numRetained = 1, ckp102 references ckp101
        // Effective retention set = {102, 101} — ckp101 must not be subsumed
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder.build();
        final CompletedCheckpointStore store = createStore(stateHandleStore, 1);

        CompletedCheckpoint ckp101 = createCheckpointWithRef(101L, null);
        CompletedCheckpoint ckp102 = createCheckpointWithRef(102L, 101L);

        store.addCheckpointAndSubsumeOldestOne(ckp101, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp102, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> retained = store.getAllCheckpoints();
        assertThat(retained)
                .extracting(CompletedCheckpoint::getCheckpointID)
                .containsExactly(101L, 102L);
    }

    @Test
    void testTransitiveReferenceProtection() throws Exception {
        // numRetained = 1
        // ckp102 refs ckp101, ckp101 refs ckp99
        // Effective retention set = {102, 101, 99}
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder.build();
        final CompletedCheckpointStore store = createStore(stateHandleStore, 1);

        CompletedCheckpoint ckp99 = createCheckpointWithRef(99L, null);
        CompletedCheckpoint ckp101 = createCheckpointWithRef(101L, 99L);
        CompletedCheckpoint ckp102 = createCheckpointWithRef(102L, 101L);

        store.addCheckpointAndSubsumeOldestOne(ckp99, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp101, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp102, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> retained = store.getAllCheckpoints();
        assertThat(retained)
                .extracting(CompletedCheckpoint::getCheckpointID)
                .containsExactly(99L, 101L, 102L);
    }

    @Test
    void testReferenceChainBrokenAfterGlobalCheckpoint() throws Exception {
        // numRetained = 1
        // ckp102 refs ckp101, ckp101 refs ckp99
        // Then ckp103 arrives with NO refs (global checkpoint)
        // After ckp103: effective set = {103} — all others can be subsumed
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder.build();
        final CompletedCheckpointStore store = createStore(stateHandleStore, 1);

        CompletedCheckpoint ckp99 = createCheckpointWithRef(99L, null);
        CompletedCheckpoint ckp101 = createCheckpointWithRef(101L, 99L);
        CompletedCheckpoint ckp102 = createCheckpointWithRef(102L, 101L);
        CompletedCheckpoint ckp103 = createCheckpointWithRef(103L, null);

        store.addCheckpointAndSubsumeOldestOne(ckp99, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp101, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp102, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp103, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> retained = store.getAllCheckpoints();
        assertThat(retained).extracting(CompletedCheckpoint::getCheckpointID).containsExactly(103L);
    }

    @Test
    void testCheckpointWithoutRefFieldIsTreatedAsGlobal() throws Exception {
        // All checkpoints have no ref — behavior should be identical to before (standard retention)
        final TestingStateHandleStore<CompletedCheckpoint> stateHandleStore = builder.build();
        final CompletedCheckpointStore store = createStore(stateHandleStore, 1);

        CompletedCheckpoint ckp1 = createCheckpointWithRef(1L, null);
        CompletedCheckpoint ckp2 = createCheckpointWithRef(2L, null);
        CompletedCheckpoint ckp3 = createCheckpointWithRef(3L, null);

        store.addCheckpointAndSubsumeOldestOne(ckp1, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp2, new CheckpointsCleaner(), () -> {});
        store.addCheckpointAndSubsumeOldestOne(ckp3, new CheckpointsCleaner(), () -> {});

        List<CompletedCheckpoint> retained = store.getAllCheckpoints();
        assertThat(retained).extracting(CompletedCheckpoint::getCheckpointID).containsExactly(3L);
    }

    @Test
    void testComputeReferencedCheckpointIdsDirectReference() {
        // ckp102 references ckp101; numRetain=1; only ckp102 is retained
        ArrayDeque<CompletedCheckpoint> deque = new ArrayDeque<>();
        deque.add(createCheckpointWithRef(101L, null));
        deque.add(createCheckpointWithRef(102L, 101L));

        Set<Long> protectedIds =
                DefaultCompletedCheckpointStore.computeReferencedCheckpointIds(deque, 1);
        assertThat(protectedIds).containsExactly(101L);
    }

    @Test
    void testComputeReferencedCheckpointIdsTransitive() {
        // ckp102 refs ckp101, ckp101 refs ckp99; numRetain=1
        ArrayDeque<CompletedCheckpoint> deque = new ArrayDeque<>();
        deque.add(createCheckpointWithRef(99L, null));
        deque.add(createCheckpointWithRef(101L, 99L));
        deque.add(createCheckpointWithRef(102L, 101L));

        Set<Long> protectedIds =
                DefaultCompletedCheckpointStore.computeReferencedCheckpointIds(deque, 1);
        assertThat(protectedIds).containsExactlyInAnyOrder(101L, 99L);
    }

    @Test
    void testComputeReferencedCheckpointIdsNoReferences() {
        // No refs — protected set should be empty
        ArrayDeque<CompletedCheckpoint> deque = new ArrayDeque<>();
        deque.add(createCheckpointWithRef(1L, null));
        deque.add(createCheckpointWithRef(2L, null));
        deque.add(createCheckpointWithRef(3L, null));

        Set<Long> protectedIds =
                DefaultCompletedCheckpointStore.computeReferencedCheckpointIds(deque, 1);
        assertThat(protectedIds).isEmpty();
    }

    @Test
    void testComputeReferencedCheckpointIdsRefToExternalCheckpoint() {
        // ckp102 references ckp50 which is NOT in the deque (already gone)
        // 50 should still be in protected set (even though we can't follow it further)
        ArrayDeque<CompletedCheckpoint> deque = new ArrayDeque<>();
        deque.add(createCheckpointWithRef(101L, null));
        deque.add(createCheckpointWithRef(102L, 50L));

        Set<Long> protectedIds =
                DefaultCompletedCheckpointStore.computeReferencedCheckpointIds(deque, 1);
        assertThat(protectedIds).containsExactly(50L);
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private CompletedCheckpoint createCheckpointWithRef(long checkpointId, Long refId) {
        OperatorID operatorID = new OperatorID();
        Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        OperatorState operatorState = new OperatorState(null, null, operatorID, 1, 128);

        OperatorSubtaskState.Builder subtaskBuilder = OperatorSubtaskState.builder();
        if (refId != null) {
            subtaskBuilder.setRefCheckpointId(refId);
        }
        operatorState.putState(0, subtaskBuilder.build());
        operatorStates.put(operatorID, operatorState);

        return new CompletedCheckpoint(
                new JobID(),
                checkpointId,
                0L,
                0L,
                operatorStates,
                Collections.emptyList(),
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation(),
                null);
    }

    private CompletedCheckpointStore createStore(
            TestingStateHandleStore<CompletedCheckpoint> stateHandleStore, int numRetain)
            throws Exception {
        final CheckpointStoreUtil checkpointStoreUtil =
                new CheckpointStoreUtil() {
                    @Override
                    public String checkpointIDToName(long checkpointId) {
                        return String.valueOf(checkpointId);
                    }

                    @Override
                    public long nameToCheckpointID(String name) {
                        return Long.parseLong(name);
                    }
                };
        return new DefaultCompletedCheckpointStore<>(
                numRetain,
                stateHandleStore,
                checkpointStoreUtil,
                emptyList(),
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        org.apache.flink.util.concurrent.Executors.directExecutor(),
                        emptyList(),
                        RecoveryClaimMode.DEFAULT),
                executorService);
    }
}
