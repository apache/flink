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

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CheckpointDurationTracker}. */
class CheckpointDurationTrackerTest {

    @Test
    void testCompleteWithoutMatchingStartReturnsEmpty() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(10);

        OptionalLong duration = tracker.onCheckpointComplete(1L, 1000L);

        assertThat(duration).isEmpty();
        assertThat(tracker.getRecentDurations()).isEmpty();
    }

    @Test
    void testStartThenCompleteRecordsDuration() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(10);

        tracker.onCheckpointStart(1L, 1000L);
        OptionalLong duration = tracker.onCheckpointComplete(1L, 1300L);

        assertThat(duration).hasValue(300L);
        assertThat(tracker.getRecentDurations()).containsExactly(300L);
    }

    @Test
    void testMultipleCheckpointsAreRecordedInOrder() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(10);

        tracker.onCheckpointStart(1L, 0L);
        tracker.onCheckpointComplete(1L, 100L);
        tracker.onCheckpointStart(2L, 200L);
        tracker.onCheckpointComplete(2L, 500L);

        assertThat(tracker.getRecentDurations()).containsExactly(100L, 300L);
    }

    @Test
    void testCompletingACheckpointPurgesOlderStillPendingStarts() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(10);

        // Checkpoint 1 never completes (e.g. it was superseded/aborted), but checkpoint 2
        // completing should purge its stale start timestamp so it can never be double-counted.
        tracker.onCheckpointStart(1L, 0L);
        tracker.onCheckpointStart(2L, 50L);
        tracker.onCheckpointComplete(2L, 150L);

        assertThat(tracker.getRecentDurations()).containsExactly(100L);

        // If checkpoint 1's stale start were still present, this would wrongly produce a duration.
        OptionalLong staleCompletion = tracker.onCheckpointComplete(1L, 9999L);
        assertThat(staleCompletion).isEmpty();
        assertThat(tracker.getRecentDurations()).containsExactly(100L);
    }

    @Test
    void testRollingWindowEvictsOldestDurations() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(2);

        for (long id = 1; id <= 3; id++) {
            tracker.onCheckpointStart(id, 0L);
            tracker.onCheckpointComplete(id, id * 100L);
        }

        // Only the 2 most recent durations should be retained.
        assertThat(tracker.getRecentDurations()).containsExactly(200L, 300L);
    }

    @Test
    void testDurationNeverNegative() {
        CheckpointDurationTracker tracker = new CheckpointDurationTracker(10);

        // A non-monotonic clock (e.g. NTP adjustment) should not produce a negative duration.
        tracker.onCheckpointStart(1L, 1000L);
        OptionalLong duration = tracker.onCheckpointComplete(1L, 500L);

        assertThat(duration).hasValue(0L);
    }
}
