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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test checkpoint statistics counters. */
class CheckpointStatsCountsTest {

    /** Tests that counts are reported correctly. */
    @Test
    void testCounts() {
        CheckpointStatsCounts counts = new CheckpointStatsCounts();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isZero();
        assertThat(counts.getTotalNumberOfCheckpoints()).isZero();
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isZero();
        assertThat(counts.getNumberOfFailedCheckpoints()).isZero();

        counts.incrementRestoredCheckpoints();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isZero();
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isZero();
        assertThat(counts.getNumberOfFailedCheckpoints()).isZero();

        // 1st checkpoint
        counts.incrementInProgressCheckpoints();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isOne();
        assertThat(counts.getNumberOfInProgressCheckpoints()).isOne();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isZero();
        assertThat(counts.getNumberOfFailedCheckpoints()).isZero();

        counts.incrementCompletedCheckpoints();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isOne();
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isOne();
        assertThat(counts.getNumberOfFailedCheckpoints()).isZero();

        // 2nd checkpoint
        counts.incrementInProgressCheckpoints();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(2);
        assertThat(counts.getNumberOfInProgressCheckpoints()).isOne();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isOne();
        assertThat(counts.getNumberOfFailedCheckpoints()).isZero();

        counts.incrementFailedCheckpoints();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(2);
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isOne();
        assertThat(counts.getNumberOfFailedCheckpoints()).isOne();

        counts.incrementFailedCheckpointsWithoutInProgress();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isOne();
        assertThat(counts.getTotalNumberOfCheckpoints()).isEqualTo(3);
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isOne();
        assertThat(counts.getNumberOfFailedCheckpoints()).isEqualTo(2);
    }

    /**
     * Tests that increment the completed or failed number of checkpoints without incrementing the
     * in progress checkpoints before throws an Exception.
     */
    @Test
    void testCompleteOrFailWithoutInProgressCheckpoint() {
        CheckpointStatsCounts counts = new CheckpointStatsCounts();
        counts.incrementCompletedCheckpoints();
        assertThat(counts.getNumberOfInProgressCheckpoints())
                .as("Number of checkpoints in progress should never be negative")
                .isGreaterThanOrEqualTo(0);

        counts.incrementFailedCheckpoints();
        assertThat(counts.getNumberOfInProgressCheckpoints())
                .as("Number of checkpoints in progress should never be negative")
                .isGreaterThanOrEqualTo(0);
    }

    /** Tests that taking snapshots of the state are independent of the parent. */
    @Test
    void testCreateSnapshot() {
        CheckpointStatsCounts counts = new CheckpointStatsCounts();
        counts.incrementRestoredCheckpoints();
        counts.incrementRestoredCheckpoints();
        counts.incrementRestoredCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementFailedCheckpoints();

        long restored = counts.getNumberOfRestoredCheckpoints();
        long total = counts.getTotalNumberOfCheckpoints();
        long inProgress = counts.getNumberOfInProgressCheckpoints();
        long completed = counts.getNumberOfCompletedCheckpoints();
        long failed = counts.getNumberOfFailedCheckpoints();

        CheckpointStatsCounts snapshot = counts.createSnapshot();
        assertThat(snapshot.getNumberOfRestoredCheckpoints()).isEqualTo(restored);
        assertThat(snapshot.getTotalNumberOfCheckpoints()).isEqualTo(total);
        assertThat(snapshot.getNumberOfInProgressCheckpoints()).isEqualTo(inProgress);
        assertThat(snapshot.getNumberOfCompletedCheckpoints()).isEqualTo(completed);
        assertThat(snapshot.getNumberOfFailedCheckpoints()).isEqualTo(failed);

        // Update the original
        counts.incrementRestoredCheckpoints();
        counts.incrementRestoredCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        counts.incrementInProgressCheckpoints();
        counts.incrementFailedCheckpoints();

        assertThat(snapshot.getNumberOfRestoredCheckpoints()).isEqualTo(restored);
        assertThat(snapshot.getTotalNumberOfCheckpoints()).isEqualTo(total);
        assertThat(snapshot.getNumberOfInProgressCheckpoints()).isEqualTo(inProgress);
        assertThat(snapshot.getNumberOfCompletedCheckpoints()).isEqualTo(completed);
        assertThat(snapshot.getNumberOfFailedCheckpoints()).isEqualTo(failed);
    }
}
