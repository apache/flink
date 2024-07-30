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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Set;

public enum NoOpCheckpointStatsTracker implements CheckpointStatsTracker {
    INSTANCE;

    @Override
    public void reportRestoredCheckpoint(
            long checkpointID,
            CheckpointProperties properties,
            String externalPath,
            long stateSize) {}

    @Override
    public void reportCompletedCheckpoint(CompletedCheckpointStats completed) {}

    @Nullable
    @Override
    public PendingCheckpointStats getPendingCheckpointStats(long checkpointId) {
        return null;
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

    @Nullable
    @Override
    public PendingCheckpointStats reportPendingCheckpoint(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            Map<JobVertexID, Integer> vertexToDop) {
        return null;
    }

    @Override
    public void reportFailedCheckpoint(FailedCheckpointStats failed) {}

    @Override
    public void reportFailedCheckpointsWithoutInProgress() {}

    @Override
    public CheckpointStatsSnapshot createSnapshot() {
        return CheckpointStatsSnapshot.empty();
    }
}
