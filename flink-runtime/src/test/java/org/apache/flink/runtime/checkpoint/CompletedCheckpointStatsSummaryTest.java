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

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompletedCheckpointStatsSummaryTest {

    /** Tests simple updates of the completed checkpoint stats. */
    @Test
    void testSimpleUpdates() {
        long triggerTimestamp = 123123L;
        long ackTimestamp = 123123 + 1212312399L;
        long stateSize = Integer.MAX_VALUE + 17787L;
        long processedData = Integer.MAX_VALUE + 123123L;
        long persistedData = Integer.MAX_VALUE + 42L;
        boolean unalignedCheckpoint = true;

        CompletedCheckpointStatsSummary summary = new CompletedCheckpointStatsSummary();
        assertThat(summary.getStateSizeStats().getCount()).isZero();
        assertThat(summary.getEndToEndDurationStats().getCount()).isZero();
        assertThat(summary.getProcessedDataStats().getCount()).isZero();
        assertThat(summary.getPersistedDataStats().getCount()).isZero();

        int numCheckpoints = 10;

        for (int i = 0; i < numCheckpoints; i++) {
            CompletedCheckpointStats completed =
                    createCompletedCheckpoint(
                            i,
                            triggerTimestamp,
                            ackTimestamp + i,
                            stateSize + i,
                            processedData + i,
                            persistedData + i,
                            unalignedCheckpoint);

            summary.updateSummary(completed);

            assertThat(summary.getStateSizeStats().getCount()).isEqualTo(i + 1);
            assertThat(summary.getEndToEndDurationStats().getCount()).isEqualTo(i + 1);
            assertThat(summary.getProcessedDataStats().getCount()).isEqualTo(i + 1);
            assertThat(summary.getPersistedDataStats().getCount()).isEqualTo(i + 1);
        }

        StatsSummary stateSizeStats = summary.getStateSizeStats();
        assertThat(stateSizeStats.getMinimum()).isEqualTo(stateSize);
        assertThat(stateSizeStats.getMaximum()).isEqualTo(stateSize + numCheckpoints - 1);

        StatsSummary durationStats = summary.getEndToEndDurationStats();
        assertThat(durationStats.getMinimum()).isEqualTo(ackTimestamp - triggerTimestamp);
        assertThat(durationStats.getMaximum())
                .isEqualTo(ackTimestamp - triggerTimestamp + numCheckpoints - 1);

        StatsSummary processedDataStats = summary.getProcessedDataStats();
        assertThat(processedDataStats.getMinimum()).isEqualTo(processedData);
        assertThat(processedDataStats.getMaximum()).isEqualTo(processedData + numCheckpoints - 1);

        StatsSummary persistedDataStats = summary.getPersistedDataStats();
        assertThat(persistedDataStats.getMinimum()).isEqualTo(persistedData);
        assertThat(persistedDataStats.getMaximum()).isEqualTo(persistedData + numCheckpoints - 1);
    }

    private CompletedCheckpointStats createCompletedCheckpoint(
            long checkpointId,
            long triggerTimestamp,
            long ackTimestamp,
            long stateSize,
            long processedData,
            long persistedData,
            boolean unalignedCheckpoint) {

        SubtaskStateStats latest = mock(SubtaskStateStats.class);
        when(latest.getAckTimestamp()).thenReturn(ackTimestamp);

        Map<JobVertexID, TaskStateStats> taskStats = new HashMap<>();
        JobVertexID jobVertexId = new JobVertexID();
        taskStats.put(jobVertexId, new TaskStateStats(jobVertexId, 1));

        return new CompletedCheckpointStats(
                checkpointId,
                triggerTimestamp,
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                1,
                taskStats,
                1,
                stateSize,
                processedData,
                persistedData,
                unalignedCheckpoint,
                latest,
                null);
    }

    /** Simply test that quantiles can be computed and fields are not permuted. */
    @Test
    void testQuantiles() {
        int stateSize = 100;
        int processedData = 200;
        int persistedData = 300;
        boolean unalignedCheckpoint = true;
        long triggerTimestamp = 1234;
        long lastAck = triggerTimestamp + 123;

        CompletedCheckpointStatsSummary summary = new CompletedCheckpointStatsSummary();
        summary.updateSummary(
                new CompletedCheckpointStats(
                        1L,
                        triggerTimestamp,
                        CheckpointProperties.forSavepoint(false, SavepointFormatType.CANONICAL),
                        1,
                        singletonMap(new JobVertexID(), new TaskStateStats(new JobVertexID(), 1)),
                        1,
                        stateSize,
                        processedData,
                        persistedData,
                        unalignedCheckpoint,
                        new SubtaskStateStats(0, lastAck),
                        ""));
        CompletedCheckpointStatsSummarySnapshot snapshot = summary.createSnapshot();
        assertThat(snapshot.getStateSizeStats().getQuantile(1)).isCloseTo(stateSize, offset(0d));
        assertThat(snapshot.getProcessedDataStats().getQuantile(1))
                .isCloseTo(processedData, offset(0d));
        assertThat(snapshot.getPersistedDataStats().getQuantile(1))
                .isCloseTo(persistedData, offset(0d));
        assertThat(snapshot.getEndToEndDurationStats().getQuantile(1))
                .isCloseTo(lastAck - triggerTimestamp, offset(0d));
    }
}
