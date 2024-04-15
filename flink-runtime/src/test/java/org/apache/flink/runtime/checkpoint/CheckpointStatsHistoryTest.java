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

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CheckpointStatsHistoryTest {

    /** Tests a checkpoint history with allowed size 0. */
    @Test
    void testZeroMaxSizeHistory() {
        CheckpointStatsHistory history = new CheckpointStatsHistory(0);

        history.addInProgressCheckpoint(createPendingCheckpointStats(0));
        assertThat(history.replacePendingCheckpointById(createCompletedCheckpointStats(0)))
                .isFalse();

        CheckpointStatsHistory snapshot = history.createSnapshot();

        int counter = 0;
        for (AbstractCheckpointStats ignored : snapshot.getCheckpoints()) {
            counter++;
        }

        assertThat(counter).isZero();
        assertThat(snapshot.getCheckpointById(0)).isNotNull();
    }

    /** Tests a checkpoint history with allowed size 1. */
    @Test
    void testSizeOneHistory() {
        CheckpointStatsHistory history = new CheckpointStatsHistory(1);

        history.addInProgressCheckpoint(createPendingCheckpointStats(0));
        history.addInProgressCheckpoint(createPendingCheckpointStats(1));

        assertThat(history.replacePendingCheckpointById(createCompletedCheckpointStats(0)))
                .isFalse();
        assertThat(history.replacePendingCheckpointById(createCompletedCheckpointStats(1)))
                .isTrue();

        CheckpointStatsHistory snapshot = history.createSnapshot();

        for (AbstractCheckpointStats stats : snapshot.getCheckpoints()) {
            assertThat(stats.getCheckpointId()).isOne();
            assertThat(stats.getStatus().isCompleted()).isTrue();
        }
    }

    /** Tests the checkpoint history with multiple checkpoints. */
    @Test
    void testCheckpointHistory() throws Exception {
        CheckpointStatsHistory history = new CheckpointStatsHistory(3);

        history.addInProgressCheckpoint(createPendingCheckpointStats(0));

        CheckpointStatsHistory snapshot = history.createSnapshot();
        for (AbstractCheckpointStats stats : snapshot.getCheckpoints()) {
            assertThat(stats.getCheckpointId()).isZero();
            assertThat(stats.getStatus().isInProgress()).isTrue();
        }

        history.addInProgressCheckpoint(createPendingCheckpointStats(1));
        history.addInProgressCheckpoint(createPendingCheckpointStats(2));
        history.addInProgressCheckpoint(createPendingCheckpointStats(3));

        snapshot = history.createSnapshot();

        // Check in progress stats.
        Iterator<AbstractCheckpointStats> it = snapshot.getCheckpoints().iterator();
        for (int i = 3; i > 0; i--) {
            assertThat(it).hasNext();
            AbstractCheckpointStats stats = it.next();
            assertThat(stats.getCheckpointId()).isEqualTo(i);
            assertThat(stats.getStatus().isInProgress()).isTrue();
        }
        assertThat(it).isExhausted();

        // Update checkpoints
        history.replacePendingCheckpointById(createFailedCheckpointStats(1));
        history.replacePendingCheckpointById(createCompletedCheckpointStats(3));
        history.replacePendingCheckpointById(createFailedCheckpointStats(2));

        snapshot = history.createSnapshot();
        it = snapshot.getCheckpoints().iterator();

        assertThat(it).hasNext();
        AbstractCheckpointStats stats = it.next();
        assertThat(stats.getCheckpointId()).isEqualTo(3);
        assertThat(snapshot.getCheckpointById(3)).isNotNull();
        assertThat(stats.getStatus().isCompleted()).isTrue();
        assertThat(snapshot.getCheckpointById(3).getStatus().isCompleted()).isTrue();

        assertThat(it).hasNext();
        stats = it.next();
        assertThat(stats.getCheckpointId()).isEqualTo(2);
        assertThat(snapshot.getCheckpointById(2)).isNotNull();
        assertThat(stats.getStatus().isFailed()).isTrue();
        assertThat(snapshot.getCheckpointById(2).getStatus().isFailed()).isTrue();

        assertThat(it).hasNext();
        stats = it.next();
        assertThat(stats.getCheckpointId()).isOne();
        assertThat(snapshot.getCheckpointById(1)).isNotNull();
        assertThat(stats.getStatus().isFailed()).isTrue();
        assertThat(snapshot.getCheckpointById(1).getStatus().isFailed()).isTrue();

        assertThat(it).isExhausted();
    }

    /** Tests that a snapshot cannot be modified or copied. */
    @Test
    void testModifySnapshot() throws Exception {
        CheckpointStatsHistory history = new CheckpointStatsHistory(3);

        history.addInProgressCheckpoint(createPendingCheckpointStats(0));
        history.addInProgressCheckpoint(createPendingCheckpointStats(1));
        history.addInProgressCheckpoint(createPendingCheckpointStats(2));

        CheckpointStatsHistory snapshot = history.createSnapshot();

        try {
            snapshot.addInProgressCheckpoint(createPendingCheckpointStats(4));
            fail("Did not throw expected Exception");
        } catch (UnsupportedOperationException ignored) {
        }

        try {
            snapshot.replacePendingCheckpointById(createCompletedCheckpointStats(2));
            fail("Did not throw expected Exception");
        } catch (UnsupportedOperationException ignored) {
        }

        try {
            snapshot.createSnapshot();
            fail("Did not throw expected Exception");
        } catch (UnsupportedOperationException ignored) {
        }
    }

    // ------------------------------------------------------------------------

    private PendingCheckpointStats createPendingCheckpointStats(long checkpointId) {
        PendingCheckpointStats pending = mock(PendingCheckpointStats.class);
        when(pending.getStatus()).thenReturn(CheckpointStatsStatus.IN_PROGRESS);
        when(pending.getCheckpointId()).thenReturn(checkpointId);
        when(pending.getProperties())
                .thenReturn(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        return pending;
    }

    private CompletedCheckpointStats createCompletedCheckpointStats(long checkpointId) {
        CompletedCheckpointStats completed = mock(CompletedCheckpointStats.class);
        when(completed.getStatus()).thenReturn(CheckpointStatsStatus.COMPLETED);
        when(completed.getProperties())
                .thenReturn(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        when(completed.getCheckpointId()).thenReturn(checkpointId);
        when(completed.getProperties())
                .thenReturn(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        return completed;
    }

    private FailedCheckpointStats createFailedCheckpointStats(long checkpointId) {
        FailedCheckpointStats failed = mock(FailedCheckpointStats.class);
        when(failed.getStatus()).thenReturn(CheckpointStatsStatus.FAILED);
        when(failed.getProperties())
                .thenReturn(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        when(failed.getCheckpointId()).thenReturn(checkpointId);
        return failed;
    }
}
