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
    void testSnapshotStateAndRestoreStateWithCheckpointHistory() throws Exception {
        final long checkpointId1 = 100L;
        final long checkpointId2 = 101L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Checkpointed history: ckp100 and ckp101.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
        tracker.onCheckpoint(checkpointId1);
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
        tracker.onCheckpoint(checkpointId2);

        // Plus some uncheckpointed assignments on top.
        tracker.recordSplitAssignment(getSplitsAssignment(1, 6));

        byte[] snapshotState = tracker.snapshotState(new MockSourceSplitSerializer());

        SplitAssignmentTracker<MockSourceSplit> trackerToRestore = new SplitAssignmentTracker<>();
        trackerToRestore.restoreState(new MockSourceSplitSerializer(), snapshotState);

        // The per-checkpoint history must survive the round-trip (the core of regional checkpoint
        // precise rollback support).
        verifyAssignment(
                Arrays.asList("0"),
                trackerToRestore.assignmentsByCheckpointId(checkpointId1).get(0));
        verifyAssignment(
                Arrays.asList("1", "2"),
                trackerToRestore.assignmentsByCheckpointId(checkpointId1).get(1));
        verifyAssignment(
                Arrays.asList("3"),
                trackerToRestore.assignmentsByCheckpointId(checkpointId2).get(0));
        verifyAssignment(
                Arrays.asList("4", "5"),
                trackerToRestore.assignmentsByCheckpointId(checkpointId2).get(1));

        // The uncheckpointed assignments must survive too.
        verifyAssignment(Arrays.asList("6"), trackerToRestore.uncheckpointedAssignments().get(0));

        // And precise rollback must work after restore: rolling subtask 0 back past ckp100 should
        // recover the splits assigned in ckp101 and the uncheckpointed ones.
        List<MockSourceSplit> splitsToPutBack =
                trackerToRestore.getAndRemoveUncheckpointedAssignment(0, checkpointId1);
        verifyAssignment(Arrays.asList("3", "6"), splitsToPutBack);
    }

    @Test
    void testRestoreFromLegacyFormatLeavesHistoryEmpty() throws Exception {
        // The legacy snapshot format only contained the uncheckpointed assignments, produced
        // directly by SourceCoordinatorSerdeUtils.serializeAssignments(...).
        SplitAssignmentTracker<MockSourceSplit> source = new SplitAssignmentTracker<>();
        source.recordSplitAssignment(getSplitsAssignment(2, 0));
        byte[] legacy =
                SourceCoordinatorSerdeUtils.serializeAssignments(
                        source.uncheckpointedAssignments(), new MockSourceSplitSerializer());

        SplitAssignmentTracker<MockSourceSplit> trackerToRestore = new SplitAssignmentTracker<>();
        trackerToRestore.restoreState(new MockSourceSplitSerializer(), legacy);

        verifyAssignment(Arrays.asList("0"), trackerToRestore.uncheckpointedAssignments().get(0));
        verifyAssignment(
                Arrays.asList("1", "2"), trackerToRestore.uncheckpointedAssignments().get(1));
        assertThat(trackerToRestore.assignmentsByCheckpointId()).isEmpty();
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

    // ---------------------
}
