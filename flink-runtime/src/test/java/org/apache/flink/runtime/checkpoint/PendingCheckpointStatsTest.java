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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PendingCheckpointStatsTest {

    /** Tests reporting of subtask stats. */
    @Test
    void testReportSubtaskStats() {
        long checkpointId = Integer.MAX_VALUE + 1222L;
        long triggerTimestamp = Integer.MAX_VALUE - 1239L;
        CheckpointProperties props =
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION);
        TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
        TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);
        int totalSubtaskCount = task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks();

        HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        taskStats.put(task1.getJobVertexId(), task1);
        taskStats.put(task2.getJobVertexId(), task2);

        PendingCheckpointStats pending =
                new PendingCheckpointStats(
                        checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);

        // Check initial state
        assertThat(pending.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(pending.getTriggerTimestamp()).isEqualTo(triggerTimestamp);
        assertThat(pending.getProperties()).isEqualTo(props);
        assertThat(pending.getStatus()).isEqualTo(CheckpointStatsStatus.IN_PROGRESS);
        assertThat(pending.getNumberOfAcknowledgedSubtasks()).isZero();
        assertThat(pending.getStateSize()).isZero();
        assertThat(pending.getNumberOfSubtasks()).isEqualTo(totalSubtaskCount);
        assertThat(pending.getLatestAcknowledgedSubtaskStats()).isNull();
        assertThat(pending.getLatestAckTimestamp()).isEqualTo(-1);
        assertThat(pending.getEndToEndDuration()).isEqualTo(-1);
        assertThat(pending.getTaskStateStats(task1.getJobVertexId())).isEqualTo(task1);
        assertThat(pending.getTaskStateStats(task2.getJobVertexId())).isEqualTo(task2);
        assertThat(pending.getTaskStateStats(new JobVertexID())).isNull();

        // Report subtasks and check getters
        assertThat(pending.reportSubtaskStats(new JobVertexID(), createSubtaskStats(0, false)))
                .isFalse();

        long stateSize = 0;

        // Report 1st task
        for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
            SubtaskStateStats subtask = createSubtaskStats(i, false);
            stateSize += subtask.getStateSize();

            pending.reportSubtaskStats(task1.getJobVertexId(), subtask);

            assertThat(pending.isUnalignedCheckpoint()).isFalse();
            assertThat(pending.getLatestAcknowledgedSubtaskStats()).isEqualTo(subtask);
            assertThat(pending.getLatestAckTimestamp()).isEqualTo(subtask.getAckTimestamp());
            assertThat(pending.getEndToEndDuration())
                    .isEqualTo(subtask.getAckTimestamp() - triggerTimestamp);
            assertThat(pending.getStateSize()).isEqualTo(stateSize);
        }

        // Don't allow overwrite
        assertThat(pending.reportSubtaskStats(task1.getJobVertexId(), task1.getSubtaskStats()[0]))
                .isFalse();

        // Report 2nd task
        for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
            SubtaskStateStats subtask = createSubtaskStats(i, true);
            stateSize += subtask.getStateSize();

            pending.reportSubtaskStats(task2.getJobVertexId(), subtask);

            assertThat(pending.isUnalignedCheckpoint()).isTrue();
            assertThat(pending.getLatestAcknowledgedSubtaskStats()).isEqualTo(subtask);
            assertThat(pending.getLatestAckTimestamp()).isEqualTo(subtask.getAckTimestamp());
            assertThat(pending.getEndToEndDuration())
                    .isEqualTo(subtask.getAckTimestamp() - triggerTimestamp);
            assertThat(pending.getStateSize()).isEqualTo(stateSize);
        }

        assertThat(task1.getNumberOfAcknowledgedSubtasks()).isEqualTo(task1.getNumberOfSubtasks());
        assertThat(task2.getNumberOfAcknowledgedSubtasks()).isEqualTo(task2.getNumberOfSubtasks());
    }

    /** Test reporting of a completed checkpoint. */
    @Test
    void testReportCompletedCheckpoint() {
        TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
        TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

        HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        taskStats.put(task1.getJobVertexId(), task1);
        taskStats.put(task2.getJobVertexId(), task2);

        CheckpointStatsTracker callback = mock(CheckpointStatsTracker.class);

        PendingCheckpointStats pending =
                new PendingCheckpointStats(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks(),
                        taskStats);

        // Report subtasks
        for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
            pending.reportSubtaskStats(task1.getJobVertexId(), createSubtaskStats(i, false));
            assertThat(pending.isUnalignedCheckpoint()).isFalse();
        }

        for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
            pending.reportSubtaskStats(task2.getJobVertexId(), createSubtaskStats(i, true));
            assertThat(pending.isUnalignedCheckpoint()).isTrue();
        }

        // Report completed
        String externalPath = "asdjkasdjkasd";

        callback.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(externalPath));

        ArgumentCaptor<CompletedCheckpointStats> args =
                ArgumentCaptor.forClass(CompletedCheckpointStats.class);
        verify(callback).reportCompletedCheckpoint(args.capture());

        CompletedCheckpointStats completed = args.getValue();

        assertThat(completed).isNotNull();
        assertThat(completed.getStatus()).isEqualTo(CheckpointStatsStatus.COMPLETED);
        assertThat(completed.isDiscarded()).isFalse();
        completed.discard();
        assertThat(completed.isDiscarded()).isTrue();
        assertThat(completed.getExternalPath()).isEqualTo(externalPath);

        assertThat(completed.getCheckpointId()).isEqualTo(pending.getCheckpointId());
        assertThat(completed.getNumberOfAcknowledgedSubtasks())
                .isEqualTo(pending.getNumberOfAcknowledgedSubtasks());
        assertThat(completed.getLatestAcknowledgedSubtaskStats())
                .isEqualTo(pending.getLatestAcknowledgedSubtaskStats());
        assertThat(completed.getLatestAckTimestamp()).isEqualTo(pending.getLatestAckTimestamp());
        assertThat(completed.getEndToEndDuration()).isEqualTo(pending.getEndToEndDuration());
        assertThat(completed.getStateSize()).isEqualTo(pending.getStateSize());
        assertThat(completed.isUnalignedCheckpoint()).isTrue();
        assertThat(completed.getTaskStateStats(task1.getJobVertexId())).isEqualTo(task1);
        assertThat(completed.getTaskStateStats(task2.getJobVertexId())).isEqualTo(task2);
    }

    /** Test reporting of a failed checkpoint. */
    @Test
    void testReportFailedCheckpoint() {
        TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
        TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

        HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        taskStats.put(task1.getJobVertexId(), task1);
        taskStats.put(task2.getJobVertexId(), task2);

        CheckpointStatsTracker callback = mock(CheckpointStatsTracker.class);

        long triggerTimestamp = 123123;
        PendingCheckpointStats pending =
                new PendingCheckpointStats(
                        0,
                        triggerTimestamp,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        task1.getNumberOfSubtasks() + task2.getNumberOfSubtasks(),
                        taskStats);

        // Report subtasks
        for (int i = 0; i < task1.getNumberOfSubtasks(); i++) {
            pending.reportSubtaskStats(task1.getJobVertexId(), createSubtaskStats(i, false));
            assertThat(pending.isUnalignedCheckpoint()).isFalse();
        }

        for (int i = 0; i < task2.getNumberOfSubtasks(); i++) {
            pending.reportSubtaskStats(task2.getJobVertexId(), createSubtaskStats(i, true));
            assertThat(pending.isUnalignedCheckpoint()).isTrue();
        }

        // Report failed
        Exception cause = new Exception("test exception");
        long failureTimestamp = 112211137;
        callback.reportFailedCheckpoint(pending.toFailedCheckpoint(failureTimestamp, cause));

        ArgumentCaptor<FailedCheckpointStats> args =
                ArgumentCaptor.forClass(FailedCheckpointStats.class);
        verify(callback).reportFailedCheckpoint(args.capture());

        FailedCheckpointStats failed = args.getValue();

        assertThat(failed).isNotNull();
        assertThat(failed.getStatus()).isEqualTo(CheckpointStatsStatus.FAILED);
        assertThat(failed.getFailureTimestamp()).isEqualTo(failureTimestamp);
        assertThat(failed.getFailureMessage()).isEqualTo(cause.getMessage());

        assertThat(failed.getCheckpointId()).isEqualTo(pending.getCheckpointId());
        assertThat(failed.getNumberOfAcknowledgedSubtasks())
                .isEqualTo(pending.getNumberOfAcknowledgedSubtasks());
        assertThat(failed.getLatestAcknowledgedSubtaskStats())
                .isEqualTo(pending.getLatestAcknowledgedSubtaskStats());
        assertThat(failed.getLatestAckTimestamp()).isEqualTo(pending.getLatestAckTimestamp());
        assertThat(failed.getEndToEndDuration()).isEqualTo(failureTimestamp - triggerTimestamp);
        assertThat(failed.getStateSize()).isEqualTo(pending.getStateSize());
        assertThat(failed.isUnalignedCheckpoint()).isTrue();
        assertThat(failed.getTaskStateStats(task1.getJobVertexId())).isEqualTo(task1);
        assertThat(failed.getTaskStateStats(task2.getJobVertexId())).isEqualTo(task2);
    }

    @Test
    void testIsJavaSerializable() throws Exception {
        TaskStateStats task1 = new TaskStateStats(new JobVertexID(), 3);
        TaskStateStats task2 = new TaskStateStats(new JobVertexID(), 4);

        HashMap<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        taskStats.put(task1.getJobVertexId(), task1);
        taskStats.put(task2.getJobVertexId(), task2);

        PendingCheckpointStats pending =
                new PendingCheckpointStats(
                        123123123L,
                        10123L,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        1337,
                        taskStats);

        PendingCheckpointStats copy = CommonTestUtils.createCopySerializable(pending);

        assertThat(copy.getCheckpointId()).isEqualTo(pending.getCheckpointId());
        assertThat(copy.getTriggerTimestamp()).isEqualTo(pending.getTriggerTimestamp());
        assertThat(copy.getProperties()).isEqualTo(pending.getProperties());
        assertThat(copy.getNumberOfSubtasks()).isEqualTo(pending.getNumberOfSubtasks());
        assertThat(copy.getNumberOfAcknowledgedSubtasks())
                .isEqualTo(pending.getNumberOfAcknowledgedSubtasks());
        assertThat(copy.getEndToEndDuration()).isEqualTo(pending.getEndToEndDuration());
        assertThat(copy.getStateSize()).isEqualTo(pending.getStateSize());
        assertThat(copy.getLatestAcknowledgedSubtaskStats())
                .isEqualTo(pending.getLatestAcknowledgedSubtaskStats());
        assertThat(copy.getStatus()).isEqualTo(pending.getStatus());
    }

    // ------------------------------------------------------------------------

    private SubtaskStateStats createSubtaskStats(int index, boolean unalignedCheckpoint) {
        return new SubtaskStateStats(
                index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                Integer.MAX_VALUE + (long) index,
                unalignedCheckpoint,
                true);
    }
}
