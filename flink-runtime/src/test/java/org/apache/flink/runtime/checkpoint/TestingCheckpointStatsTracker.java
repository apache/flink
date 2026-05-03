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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Testing implementation of {@link CheckpointStatsTracker}. */
public class TestingCheckpointStatsTracker implements CheckpointStatsTracker {
    public final AtomicInteger numFailedCheckpoints = new AtomicInteger();
    public final AtomicInteger numPendingCheckpoints = new AtomicInteger();
    private static final PendingCheckpointStats PENDING_CHECKPOINT_STATS =
            new PendingCheckpointStats(
                    0L,
                    0L,
                    CheckpointProperties.forCheckpoint(
                            CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                    Collections.singletonMap(new JobVertexID(), 1));

    @Override
    public void reportFailedCheckpointsWithoutInProgress() {
        numFailedCheckpoints.incrementAndGet();
    }

    @Override
    public CheckpointStatsSnapshot createSnapshot() {
        return new CheckpointStatsSnapshot(
                new CheckpointStatsCounts(),
                CompletedCheckpointStatsSummarySnapshot.empty(),
                new CheckpointStatsHistory(10),
                null);
    }

    @Override
    public void reportFailedCheckpoint(FailedCheckpointStats failed) {
        numFailedCheckpoints.incrementAndGet();
    }

    @Override
    public void reportRestoredCheckpoint(
            long checkpointID,
            CheckpointProperties properties,
            String externalPath,
            long stateSize) {}

    @Override
    public void reportCompletedCheckpoint(CompletedCheckpointStats completed) {}

    @Override
    public PendingCheckpointStats getPendingCheckpointStats(long checkpointId) {
        return PENDING_CHECKPOINT_STATS;
    }

    @Override
    public void reportIncompleteStats(
            long checkpointId, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {}

    @Override
    public void reportInitializationStarted(
            Set<ExecutionAttemptID> toInitialize, long initializationStartTs) {}

    @Override
    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics) {}

    @Override
    public PendingCheckpointStats reportPendingCheckpoint(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            Map<JobVertexID, Integer> vertexToDop) {
        numPendingCheckpoints.incrementAndGet();
        return PENDING_CHECKPOINT_STATS;
    }
}
