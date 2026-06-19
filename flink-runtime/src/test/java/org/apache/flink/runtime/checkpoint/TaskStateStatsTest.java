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

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class TaskStateStatsTest {

    private final ThreadLocalRandom rand = ThreadLocalRandom.current();

    /** Tests that subtask stats are correctly collected. */
    @Test
    void testHandInSubtasks() throws Exception {
        test(false);
    }

    @Test
    void testIsJavaSerializable() throws Exception {
        test(true);
    }

    private void test(boolean serialize) throws Exception {
        JobVertexID jobVertexId = new JobVertexID();
        SubtaskStateStats[] subtasks = new SubtaskStateStats[7];

        TaskStateStats taskStats = new TaskStateStats(jobVertexId, subtasks.length);

        assertThat(taskStats.getJobVertexId()).isEqualTo(jobVertexId);
        assertThat(taskStats.getNumberOfSubtasks()).isEqualTo(subtasks.length);
        assertThat(taskStats.getNumberOfAcknowledgedSubtasks()).isZero();
        assertThat(taskStats.getLatestAcknowledgedSubtaskStats()).isNull();
        assertThat(taskStats.getLatestAckTimestamp()).isEqualTo(-1);
        assertThat(taskStats.getSubtaskStats()).isEqualTo(subtasks);

        long stateSize = 0;
        long processedData = 0;
        long persistedData = 0;

        // Hand in some subtasks
        for (int i = 0; i < subtasks.length; i++) {
            subtasks[i] =
                    new SubtaskStateStats(
                            i,
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            rand.nextInt(128),
                            false,
                            true);

            stateSize += subtasks[i].getStateSize();
            processedData += subtasks[i].getProcessedData();
            persistedData += subtasks[i].getPersistedData();

            assertThat(taskStats.reportSubtaskStats(subtasks[i])).isTrue();
            assertThat(taskStats.getNumberOfAcknowledgedSubtasks()).isEqualTo(i + 1);
            assertThat(taskStats.getLatestAcknowledgedSubtaskStats()).isEqualTo(subtasks[i]);
            assertThat(taskStats.getLatestAckTimestamp()).isEqualTo(subtasks[i].getAckTimestamp());
            int duration = rand.nextInt(128);
            assertThat(taskStats.getEndToEndDuration(subtasks[i].getAckTimestamp() - duration))
                    .isEqualTo(duration);
            assertThat(taskStats.getStateSize()).isEqualTo(stateSize);
            assertThat(taskStats.getProcessedDataStats()).isEqualTo(processedData);
            assertThat(taskStats.getPersistedDataStats()).isEqualTo(persistedData);
        }

        assertThat(
                        taskStats.reportSubtaskStats(
                                new SubtaskStateStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, true)))
                .isFalse();

        taskStats = serialize ? CommonTestUtils.createCopySerializable(taskStats) : taskStats;

        assertThat(taskStats.getStateSize()).isEqualTo(stateSize);

        // Test that all subtasks are taken into the account for the summary.
        // The correctness of the actual results is checked in the test of the
        // MinMaxAvgStats.
        TaskStateStats.TaskStateStatsSummary summary = taskStats.getSummaryStats();
        assertThat(summary.getStateSizeStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getAckTimestampStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getSyncCheckpointDurationStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getAsyncCheckpointDurationStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getAlignmentDurationStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getProcessedDataStats().getCount()).isEqualTo(subtasks.length);
        assertThat(summary.getPersistedDataStats().getCount()).isEqualTo(subtasks.length);
    }
}
