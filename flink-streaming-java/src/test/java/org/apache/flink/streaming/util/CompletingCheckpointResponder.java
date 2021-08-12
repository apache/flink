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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CheckpointResponder} that can be used in {@link StreamTaskMailboxTestHarnessBuilder} to
 * send {@link StreamTask#notifyCheckpointCompleteAsync(long)} and {@link
 * StreamTask#notifyCheckpointAbortAsync(long, long)}.
 *
 * <p>Because of cyclic dependency you must set the handlers via {@link #setHandlers(Consumer,
 * BiConsumer)} after instantiating the test harness.
 */
public class CompletingCheckpointResponder implements CheckpointResponder {
    private Consumer<Long> completeCheckpoint;
    private BiConsumer<Long, Long> abortCheckpoint;
    private final Set<Long> checkpointsToComplete = new HashSet<>();
    private long lastCompletedCheckpoint = -1;

    public void setHandlers(
            Consumer<Long> completeCheckpoint, BiConsumer<Long, Long> abortCheckpoint) {
        this.completeCheckpoint = completeCheckpoint;
        this.abortCheckpoint = abortCheckpoint;
    }

    public void completeCheckpoints(Collection<Long> checkpointsToComplete) {
        this.checkpointsToComplete.addAll(checkpointsToComplete);
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState) {
        checkState(completeCheckpoint != null);
        if (!checkpointsToComplete.isEmpty()) {
            if (checkpointsToComplete.contains(checkpointId)) {
                completeCheckpoint(checkpointId);
            }
        } else {
            completeCheckpoint(checkpointId);
        }
    }

    private void completeCheckpoint(long checkpointId) {
        lastCompletedCheckpoint = checkpointId;
        completeCheckpoint.accept(checkpointId);
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {}

    @Override
    public void declineCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointException checkpointException) {
        checkState(abortCheckpoint != null);
        abortCheckpoint.accept(checkpointId, lastCompletedCheckpoint);
    }
}
