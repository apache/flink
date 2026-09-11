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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.getSplitsAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Regional Checkpoint support in {@link SplitAssignmentTracker}. */
class SplitAssignmentTrackerRegionalCheckpointTest {

    @Test
    void testGetAssignmentsAfterCheckpoint() throws Exception {
        final long checkpointId1 = 99L;
        final long checkpointId2 = 100L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign splits and checkpoint at 99.
        // subtask 0: [0], subtask 1: [1, 2]
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
        tracker.onCheckpoint(checkpointId1);

        // Assign more splits and checkpoint at 100.
        // subtask 0: [3], subtask 1: [4, 5]
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
        tracker.onCheckpoint(checkpointId2);

        // Assign uncheckpointed splits.
        // subtask 0: [6], subtask 1: [7, 8]
        tracker.recordSplitAssignment(getSplitsAssignment(2, 6));

        // Get assignments after checkpoint 99 for subtask 0.
        Set<Integer> subtaskIds = new HashSet<>(Arrays.asList(0));
        Map<Integer, List<MockSourceSplit>> result =
                tracker.getAssignmentsAfterCheckpoint(checkpointId1, subtaskIds);

        assertThat(result).containsKey(0);
        verifyAssignment(Arrays.asList("3", "6"), result.get(0));

        // Get assignments after checkpoint 99 for subtask 1.
        subtaskIds = new HashSet<>(Arrays.asList(1));
        result = tracker.getAssignmentsAfterCheckpoint(checkpointId1, subtaskIds);

        assertThat(result).containsKey(1);
        verifyAssignment(Arrays.asList("4", "5", "7", "8"), result.get(1));

        // Get assignments after checkpoint 100 for subtask 0 — only uncheckpointed.
        subtaskIds = new HashSet<>(Arrays.asList(0));
        result = tracker.getAssignmentsAfterCheckpoint(checkpointId2, subtaskIds);

        assertThat(result).containsKey(0);
        verifyAssignment(Arrays.asList("6"), result.get(0));
    }

    @Test
    void testRemoveAssignmentsAfterCheckpoint() throws Exception {
        final long checkpointId1 = 99L;
        final long checkpointId2 = 100L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign splits and checkpoint at 99.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 0));
        tracker.onCheckpoint(checkpointId1);

        // Assign more splits and checkpoint at 100.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 3));
        tracker.onCheckpoint(checkpointId2);

        // Assign uncheckpointed splits.
        tracker.recordSplitAssignment(getSplitsAssignment(2, 6));

        // Remove assignments after checkpoint 99 for subtask 0.
        Set<Integer> subtaskIds = new HashSet<>(Arrays.asList(0));
        Map<Integer, List<MockSourceSplit>> removed =
                tracker.removeAssignmentsAfterCheckpoint(checkpointId1, subtaskIds);

        assertThat(removed).containsKey(0);
        verifyAssignment(Arrays.asList("3", "6"), removed.get(0));

        // Verify the splits have been removed — getting again should return empty.
        Map<Integer, List<MockSourceSplit>> afterRemoval =
                tracker.getAssignmentsAfterCheckpoint(checkpointId1, subtaskIds);
        assertThat(afterRemoval).doesNotContainKey(0);

        // Verify subtask 1 assignments are still intact.
        Set<Integer> subtask1 = new HashSet<>(Arrays.asList(1));
        Map<Integer, List<MockSourceSplit>> subtask1Result =
                tracker.getAssignmentsAfterCheckpoint(checkpointId1, subtask1);
        assertThat(subtask1Result).containsKey(1);
        verifyAssignment(Arrays.asList("4", "5", "7", "8"), subtask1Result.get(1));
    }

    @Test
    void testMultipleSubtasksRemoval() throws Exception {
        final long checkpointId1 = 99L;
        final long checkpointId2 = 100L;
        SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();

        // Assign splits and checkpoint at 99.
        // subtask 0: [0], subtask 1: [1, 2], subtask 2: [3, 4, 5]
        tracker.recordSplitAssignment(getSplitsAssignment(3, 0));
        tracker.onCheckpoint(checkpointId1);

        // Assign more splits and checkpoint at 100.
        // subtask 0: [6], subtask 1: [7, 8], subtask 2: [9, 10, 11]
        tracker.recordSplitAssignment(getSplitsAssignment(3, 6));
        tracker.onCheckpoint(checkpointId2);

        // Remove assignments after checkpoint 99 for subtasks 0 and 2 (a region).
        Set<Integer> subtaskIds = new HashSet<>(Arrays.asList(0, 2));
        Map<Integer, List<MockSourceSplit>> removed =
                tracker.removeAssignmentsAfterCheckpoint(checkpointId1, subtaskIds);

        assertThat(removed).hasSize(2);
        assertThat(removed).containsKey(0);
        assertThat(removed).containsKey(2);
        verifyAssignment(Arrays.asList("6"), removed.get(0));
        verifyAssignment(Arrays.asList("9", "10", "11"), removed.get(2));

        // Verify subtask 1 is unaffected.
        Set<Integer> subtask1 = new HashSet<>(Arrays.asList(1));
        Map<Integer, List<MockSourceSplit>> subtask1Result =
                tracker.getAssignmentsAfterCheckpoint(checkpointId1, subtask1);
        assertThat(subtask1Result).containsKey(1);
        verifyAssignment(Arrays.asList("7", "8"), subtask1Result.get(1));
    }
}
