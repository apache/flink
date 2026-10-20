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

package org.apache.flink.runtime.source.coordinator;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SourceCoordinator} regional checkpoint support. */
class SourceCoordinatorRegionalCheckpointTest extends SourceCoordinatorTestBase {

    @Test
    void testSupportsRegionCheckpoint() throws Exception {
        sourceReady();
        assertThat(sourceCoordinator.supportsRegionCheckpoint()).isTrue();
    }

    @Test
    void testCheckpointCoordinatorForRegionFallback() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        // Register readers for subtask 0 and 1
        registerReader(0);
        registerReader(1);

        // Assign splits: 2 to subtask 0, 1 to subtask 1
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(1);

        // Take checkpoint 100 - this records the above assignments under checkpoint 100
        final CompletableFuture<byte[]> checkpoint100Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(100L, checkpoint100Future);
        waitForCoordinatorToProcessActions();
        assertThat(checkpoint100Future).isDone();

        // Assign more splits after checkpoint 100
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(1);

        // Take checkpoint 101 to record the new assignments
        final CompletableFuture<byte[]> checkpoint101Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(101L, checkpoint101Future);
        waitForCoordinatorToProcessActions();
        assertThat(checkpoint101Future).isDone();

        // Now simulate region fallback: subtask 0 failed, falls back to checkpoint 100
        final Set<Integer> fallbackSubtaskIds = new HashSet<>(Collections.singletonList(0));
        final CompletableFuture<byte[]> regionFallbackFuture = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinatorForRegionFallback(
                101L, 100L, fallbackSubtaskIds, regionFallbackFuture);
        waitForCoordinatorToProcessActions();

        // The future should complete successfully with serialized state
        assertThat(regionFallbackFuture).isDone();
        assertThat(regionFallbackFuture).isNotCompletedExceptionally();
        final byte[] result = regionFallbackFuture.get();
        assertThat(result).isNotNull();
        assertThat(result.length).isGreaterThan(0);

        // The split assigned to subtask 0 after checkpoint 100 should be added back
        // to the enumerator. Originally 6 splits, 5 were assigned (3 before ckpt 100,
        // 2 after). After rollback, subtask 0's post-ckpt-100 split should return.
        // So unassigned = original 1 remaining + 1 rolled back = 2
        assertThat(getEnumerator().getUnassignedSplits()).hasSize(2);
    }

    @Test
    void testRegionFallbackWithNoAssignmentsAfterCheckpoint() throws Exception {
        sourceReady();
        addTestingSplitSet(4);

        registerReader(0);
        registerReader(1);

        // Assign splits before checkpoint
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(1);

        // Take checkpoint 100
        final CompletableFuture<byte[]> checkpoint100Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(100L, checkpoint100Future);
        waitForCoordinatorToProcessActions();
        assertThat(checkpoint100Future).isDone();

        // No new assignments after checkpoint 100
        // Take checkpoint 101
        final CompletableFuture<byte[]> checkpoint101Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(101L, checkpoint101Future);
        waitForCoordinatorToProcessActions();

        // Region fallback for subtask 0 to checkpoint 100
        final Set<Integer> fallbackSubtaskIds = new HashSet<>(Collections.singletonList(0));
        final CompletableFuture<byte[]> regionFallbackFuture = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinatorForRegionFallback(
                101L, 100L, fallbackSubtaskIds, regionFallbackFuture);
        waitForCoordinatorToProcessActions();

        // Should succeed even when there's nothing to roll back
        assertThat(regionFallbackFuture).isDone();
        assertThat(regionFallbackFuture).isNotCompletedExceptionally();

        // Unassigned splits should remain the same (2 remaining from original 4)
        assertThat(getEnumerator().getUnassignedSplits()).hasSize(2);
    }

    @Test
    void testRegionFallbackMultipleSubtasks() throws Exception {
        sourceReady();
        addTestingSplitSet(6);

        registerReader(0);
        registerReader(1);
        registerReader(2);

        // Assign 2 splits each to subtask 0, 1, 2
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(1);
        getEnumerator().executeAssignOneSplit(2);

        // Take checkpoint 100
        final CompletableFuture<byte[]> checkpoint100Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(100L, checkpoint100Future);
        waitForCoordinatorToProcessActions();

        // Assign more splits after checkpoint 100
        getEnumerator().executeAssignOneSplit(0);
        getEnumerator().executeAssignOneSplit(1);
        getEnumerator().executeAssignOneSplit(2);

        // Take checkpoint 101
        final CompletableFuture<byte[]> checkpoint101Future = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinator(101L, checkpoint101Future);
        waitForCoordinatorToProcessActions();

        // Region fallback: subtasks 0 and 2 fall back to checkpoint 100
        final Set<Integer> fallbackSubtaskIds = new HashSet<>();
        fallbackSubtaskIds.add(0);
        fallbackSubtaskIds.add(2);
        final CompletableFuture<byte[]> regionFallbackFuture = new CompletableFuture<>();
        sourceCoordinator.checkpointCoordinatorForRegionFallback(
                101L, 100L, fallbackSubtaskIds, regionFallbackFuture);
        waitForCoordinatorToProcessActions();

        assertThat(regionFallbackFuture).isDone();
        assertThat(regionFallbackFuture).isNotCompletedExceptionally();

        // 6 splits total, 6 assigned (3 before + 3 after ckpt 100).
        // 0 unassigned originally. After rollback, subtask 0 and 2's post-ckpt-100
        // splits (1 each) should be returned = 2 back in enumerator.
        assertThat(getEnumerator().getUnassignedSplits()).hasSize(2);
    }
}
