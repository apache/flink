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

import java.io.NotSerializableException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class FailedCheckpointStatsTest {

    /**
     * Tests that the end to end duration of a failed checkpoint is the duration until the failure.
     */
    @Test
    void testEndToEndDuration() {
        long duration = 123912931293L;
        long triggerTimestamp = 10123;
        long failureTimestamp = triggerTimestamp + duration;

        Map<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        JobVertexID jobVertexId = new JobVertexID();
        taskStats.put(jobVertexId, new TaskStateStats(jobVertexId, 1));

        FailedCheckpointStats failed =
                new FailedCheckpointStats(
                        0,
                        triggerTimestamp,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        1,
                        taskStats,
                        0,
                        0,
                        0,
                        0,
                        0,
                        false,
                        failureTimestamp,
                        null,
                        null);

        assertThat(failed.getEndToEndDuration()).isEqualTo(duration);
    }

    @Test
    void testIsJavaSerializable() throws Exception {
        long duration = 123912931293L;
        long triggerTimestamp = 10123;
        long failureTimestamp = triggerTimestamp + duration;

        Map<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        JobVertexID jobVertexId = new JobVertexID();
        taskStats.put(jobVertexId, new TaskStateStats(jobVertexId, 1));

        FailedCheckpointStats failed =
                new FailedCheckpointStats(
                        123123123L,
                        triggerTimestamp,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        1337,
                        taskStats,
                        3,
                        190890123,
                        190890123,
                        4242,
                        4444,
                        true,
                        failureTimestamp,
                        null,
                        new NotSerializableException("message"));

        FailedCheckpointStats copy = CommonTestUtils.createCopySerializable(failed);

        assertThat(copy.getCheckpointId()).isEqualTo(failed.getCheckpointId());
        assertThat(copy.getTriggerTimestamp()).isEqualTo(failed.getTriggerTimestamp());
        assertThat(copy.getProperties()).isEqualTo(failed.getProperties());
        assertThat(copy.getNumberOfSubtasks()).isEqualTo(failed.getNumberOfSubtasks());
        assertThat(copy.getNumberOfAcknowledgedSubtasks())
                .isEqualTo(failed.getNumberOfAcknowledgedSubtasks());
        assertThat(copy.getEndToEndDuration()).isEqualTo(failed.getEndToEndDuration());
        assertThat(copy.getStateSize()).isEqualTo(failed.getStateSize());
        assertThat(copy.getProcessedData()).isEqualTo(failed.getProcessedData());
        assertThat(copy.getPersistedData()).isEqualTo(failed.getPersistedData());
        assertThat(copy.isUnalignedCheckpoint()).isEqualTo(failed.isUnalignedCheckpoint());
        assertThat(copy.getLatestAcknowledgedSubtaskStats())
                .isEqualTo(failed.getLatestAcknowledgedSubtaskStats());
        assertThat(copy.getStatus()).isEqualTo(failed.getStatus());
        assertThat(copy.getFailureMessage()).isEqualTo(failed.getFailureMessage());
    }
}
