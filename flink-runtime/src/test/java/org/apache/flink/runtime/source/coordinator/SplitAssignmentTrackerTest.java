/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for @link {@link SplitAssignmentTracker}. */
class SplitAssignmentTrackerTest {

    @Test
    void testRecordIncrementalSplitAssignment() {
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
        tracker.recordSplitAssignment(getSplitsAssignment(3, 0));
        tracker.recordSplitAssignment(getSplitsAssignment(2, 6));

        verifyAssignment(Arrays.asList("0", "6"), tracker.uncheckpointedAssignments().get(0));
        verifyAssignment(
                Arrays.asList("1", "2", "7", "8"), tracker.uncheckpointedAssignments().get(1));
        verifyAssignment(Arrays.asList("3", "4", "5"), tracker.uncheckpointedAssignments().get(2));
    }

    @Test
    void testSnapshotStateAndRestoreState() throws Exception {
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
        tracker.recordSplitAssignment(getSplitsAssignment(3, 0));
        tracker.recordSplitAssignment(getSplitsAssignment(2, 6));

        byte[] snapshotState = tracker.snapshotState(new MockSourceSplitSerializer());

        SplitAssignmentTracker<MockSourceSplit> trackerToRestore = new SplitAssignmentTracker<>();
        assertThat(trackerToRestore.uncheckpointedAssignments()).isEmpty();
        trackerToRestore.restoreState(new MockSourceSplitSerializer(), snapshotState);

        verifyAssignment(
                Arrays.asList("0", "6"), trackerToRestore.uncheckpointedAssignments().get(0));
        verifyAssignment(
                Arrays.asList("1", "2", "7", "8"),
                trackerToRestore.uncheckpointedAssignments().get(1));
        verifyAssignment(
                Arrays.asList("3", "4", "5"), trackerToRestore.uncheckpointedAssignments().get(2));
    }

    @Test
    void testOnCheckpoint() throws Exception {
        final long checkpointId = 123L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
        tracker.recordSplitAssignment(getSplitsAssignment(3, 0));

        // Serialize
        tracker.onCheckpoint(checkpointId);

        // Verify the uncheckpointed assignments.
        assertThat(tracker.uncheckpointedAssignments()).isEmpty();

        // verify assignments put into the checkpoints.
        Map<Long, Map<Integer, LinkedHashSet<MockSourceSplit>>> assignmentsByCheckpoints =
                tracker.assignmentsByCheckpointId();
        assertThat(assignmentsByCheckpoints.size()).isOne();

        Map<Integer, LinkedHashSet<MockSourceSplit>> assignmentForCheckpoint =
                assignmentsByCheckpoints.get(checkpointId);
        assertThat(assignmentForCheckpoint).isNotNull();

        verifyAssignment(Arrays.asList("0"), assignmentForCheckpoint.get(0));
        verifyAssignment(Arrays.asList("1", "2"), assignmentForCheckpoint.get(1));
        verifyAssignment(Arrays.asList("3", "4", "5"), assignmentForCheckpoint.get(2));
    }

    @Test
    void testOnCheckpointComplete() throws Exception {
        final long checkpointId1 = 100L;
        final long checkpointId2 = 101L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign some splits to subtask 0 and 1.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));

        // Take the first snapshot.
        tracker.onCheckpoint(checkpointId1);
        verifyAssignment(
                Arrays.asList("0"), tracker.assignmentsByCheckpointId(checkpointId1).get(0));
        verifyAssignment(
                Arrays.asList("1", "2"), tracker.assignmentsByCheckpointId(checkpointId1).get(1));

        // Assign additional splits to subtask 0 and 1.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));

        // Take the second snapshot.
        tracker.onCheckpoint(checkpointId2);
        verifyAssignment(
                Arrays.asList("0"), tracker.assignmentsByCheckpointId(checkpointId1).get(0));
        verifyAssignment(
                Arrays.asList("1", "2"), tracker.assignmentsByCheckpointId(checkpointId1).get(1));
        verifyAssignment(
                Arrays.asList("3"), tracker.assignmentsByCheckpointId(checkpointId2).get(0));
        verifyAssignment(
                Arrays.asList("4", "5"), tracker.assignmentsByCheckpointId(checkpointId2).get(1));

        // Complete the first checkpoint.
        tracker.onCheckpointComplete(checkpointId1);
        assertThat(tracker.assignmentsByCheckpointId(checkpointId1)).isNull();
        verifyAssignment(
                Arrays.asList("3"), tracker.assignmentsByCheckpointId(checkpointId2).get(0));
        verifyAssignment(
                Arrays.asList("4", "5"), tracker.assignmentsByCheckpointId(checkpointId2).get(1));
    }

    @Test
    void testGetAndRemoveUncheckpointedAssignment() throws Exception {
        final long checkpointId1 = 100L;
        final long checkpointId2 = 101L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign some splits and take snapshot 1.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
        tracker.onCheckpoint(checkpointId1);

        // Assign some more splits and take snapshot 2.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
        tracker.onCheckpoint(checkpointId2);

        // Now assume subtask 0 has failed.
        List<MockSourceSplit> splitsToPutBack =
                tracker.getAndRemoveUncheckpointedAssignment(0, checkpointId1 - 1);
        verifyAssignment(Arrays.asList("0", "3"), splitsToPutBack);
    }

    @Test
    void testGetAndRemoveSplitsAfterSomeCheckpoint() throws Exception {
        final long checkpointId1 = 100L;
        final long checkpointId2 = 101L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign some splits and take snapshot 1.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
        tracker.onCheckpoint(checkpointId1);

        // Assign some more splits and take snapshot 2.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
        tracker.onCheckpoint(checkpointId2);

        // Now assume subtask 0 has failed.
        List<MockSourceSplit> splitsToPutBack =
                tracker.getAndRemoveUncheckpointedAssignment(0, checkpointId1);
        verifyAssignment(Collections.singletonList("3"), splitsToPutBack);
    }

    // -------------------- Tombstone Tests --------------------

    @Test
    void testMarkSplitRemovedWithRetention() throws Exception {
        // Create tracker with 1-hour retention
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(3600000L);

        MockSourceSplit split = new MockSourceSplit(0);
        tracker.markSplitRemoved("0", split, 1);

        // Verify tombstone was created
        assertThat(tracker.getRemovedSplitsTombstones()).hasSize(1);
        assertThat(tracker.getRemovedSplitsTombstones().containsKey("0")).isTrue();
        assertThat(tracker.getRemovedSplitsTombstones().get("0").getLastAssignedSubtaskId())
                .isEqualTo(1);
    }

    @Test
    void testMarkSplitRemovedWithoutRetention() {
        // Create tracker with no retention (default behavior)
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(0L);

        MockSourceSplit split = new MockSourceSplit(0);
        tracker.markSplitRemoved("0", split, 1);

        // Verify no tombstone was created
        assertThat(tracker.getRemovedSplitsTombstones()).isEmpty();
    }

    @Test
    void testTryResurrectSplitWithinRetention() throws Exception {
        // Create tracker with very long retention
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(3600000L);

        MockSourceSplit split = new MockSourceSplit(0);
        tracker.markSplitRemoved("0", split, 1);

        // Try to resurrect immediately (within retention window)
        SplitAssignmentTracker.RemovedSplitInfo<MockSourceSplit> resurrected =
                tracker.tryResurrectSplit("0");

        assertThat(resurrected).isNotNull();
        assertThat(resurrected.getSplit().splitId()).isEqualTo("0");
        assertThat(resurrected.getLastAssignedSubtaskId()).isEqualTo(1);

        // Tombstone should be removed after resurrection
        assertThat(tracker.getRemovedSplitsTombstones()).isEmpty();
    }

    @Test
    void testTryResurrectSplitExpired() throws Exception {
        // Create tracker with very short retention (1ms)
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(1L);

        MockSourceSplit split = new MockSourceSplit(0);
        tracker.markSplitRemoved("0", split, 1);

        // Wait for retention to expire
        Thread.sleep(10);

        // Try to resurrect after expiration
        SplitAssignmentTracker.RemovedSplitInfo<MockSourceSplit> resurrected =
                tracker.tryResurrectSplit("0");

        // Should return null for expired tombstone
        assertThat(resurrected).isNull();

        // Tombstone should be cleaned up
        assertThat(tracker.getRemovedSplitsTombstones()).isEmpty();
    }

    @Test
    void testTryResurrectNonExistentSplit() {
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(3600000L);

        // Try to resurrect a split that was never removed
        SplitAssignmentTracker.RemovedSplitInfo<MockSourceSplit> resurrected =
                tracker.tryResurrectSplit("nonexistent");

        assertThat(resurrected).isNull();
    }

    @Test
    void testCleanupExpiredTombstones() throws Exception {
        // Create tracker with short retention
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(50L);

        // Add multiple tombstones
        tracker.markSplitRemoved("0", new MockSourceSplit(0), 1);
        Thread.sleep(30); // Wait a bit
        tracker.markSplitRemoved("1", new MockSourceSplit(1), 1);

        assertThat(tracker.getRemovedSplitsTombstones()).hasSize(2);

        // Wait for first tombstone to expire
        Thread.sleep(30);

        // Trigger cleanup via checkpoint complete
        tracker.onCheckpointComplete(1L);

        // First tombstone should be cleaned, second should remain
        assertThat(tracker.getRemovedSplitsTombstones()).hasSize(1);
        assertThat(tracker.getRemovedSplitsTombstones().containsKey("1")).isTrue();
    }

    @Test
    void testSnapshotAndRestoreWithTombstones() throws Exception {
        // Create tracker with retention
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>(3600000L);

        // Record some assignments
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));

        // Mark a split as removed
        MockSourceSplit removedSplit = new MockSourceSplit(42);
        tracker.markSplitRemoved("42", removedSplit, 1);

        // Take snapshot
        byte[] snapshotState = tracker.snapshotState(new MockSourceSplitSerializer());

        // Restore to new tracker
        SplitAssignmentTracker<MockSourceSplit> restoredTracker =
                new SplitAssignmentTracker<>(3600000L);
        restoredTracker.restoreState(new MockSourceSplitSerializer(), snapshotState);

        // Verify assignments were restored
        assertThat(restoredTracker.uncheckpointedAssignments()).hasSize(2);

        // Verify tombstone was restored
        assertThat(restoredTracker.getRemovedSplitsTombstones()).hasSize(1);
        assertThat(restoredTracker.getRemovedSplitsTombstones().containsKey("42")).isTrue();

        // Verify resurrection works after restore
        SplitAssignmentTracker.RemovedSplitInfo<MockSourceSplit> resurrected =
                restoredTracker.tryResurrectSplit("42");
        assertThat(resurrected).isNotNull();
        assertThat(resurrected.getSplit().splitId()).isEqualTo("42");
    }

    @Test
    void testBackwardCompatibilityWithOldCheckpoints() throws Exception {
        // Create old-style tracker (no retention)
        SplitAssignmentTracker<MockSourceSplit> oldTracker = new SplitAssignmentTracker<>();
        oldTracker.recordSplitAssignment(getSplitsAssignment(2, 0));

        // Manually serialize just assignments (old format)
        byte[] oldFormatSnapshot =
                SourceCoordinatorSerdeUtils.serializeAssignments(
                        oldTracker.uncheckpointedAssignments(), new MockSourceSplitSerializer());

        // Restore with new tracker that supports tombstones
        SplitAssignmentTracker<MockSourceSplit> newTracker = new SplitAssignmentTracker<>(3600000L);
        newTracker.restoreState(new MockSourceSplitSerializer(), oldFormatSnapshot);

        // Should handle gracefully - assignments restored, no tombstones
        assertThat(newTracker.uncheckpointedAssignments()).hasSize(2);
        assertThat(newTracker.getRemovedSplitsTombstones()).isEmpty();
    }

    // ---------------------
}
