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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The invokable handler used for triggering checkpoint and validation. */
public class ValidatingCheckpointHandler extends AbstractInvokable {

    private CheckpointFailureReason failureReason;
    private long lastCanceledCheckpointId = -1L;
    private long abortedCheckpointCounter = 0;
    final List<Long> abortedCheckpoints = new ArrayList<>();

    long nextExpectedCheckpointId;
    long triggeredCheckpointCounter = 0;
    CompletableFuture<Long> lastAlignmentDurationNanos;
    CompletableFuture<Long> lastBytesProcessedDuringAlignment;
    final List<Long> triggeredCheckpoints = new ArrayList<>();
    private final List<CheckpointOptions> triggeredCheckpointOptions = new ArrayList<>();

    ValidatingCheckpointHandler() {
        this(-1);
    }

    ValidatingCheckpointHandler(long nextExpectedCheckpointId) {
        super(new DummyEnvironment("test", 1, 0));
        this.nextExpectedCheckpointId = nextExpectedCheckpointId;
    }

    void setNextExpectedCheckpointId(long nextExpectedCheckpointId) {
        this.nextExpectedCheckpointId = nextExpectedCheckpointId;
    }

    CheckpointFailureReason getCheckpointFailureReason() {
        return failureReason;
    }

    long getLastCanceledCheckpointId() {
        return lastCanceledCheckpointId;
    }

    long getTriggeredCheckpointCounter() {
        return triggeredCheckpointCounter;
    }

    long getAbortedCheckpointCounter() {
        return abortedCheckpointCounter;
    }

    long getNextExpectedCheckpointId() {
        return nextExpectedCheckpointId;
    }

    CompletableFuture<Long> getLastAlignmentDurationNanos() {
        return lastAlignmentDurationNanos;
    }

    CompletableFuture<Long> getLastBytesProcessedDuringAlignment() {
        return lastBytesProcessedDuringAlignment;
    }

    List<CheckpointOptions> getTriggeredCheckpointOptions() {
        return triggeredCheckpointOptions;
    }

    @Override
    public void invoke() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics) {
        if (nextExpectedCheckpointId != -1L) {
            assertEquals(nextExpectedCheckpointId, checkpointMetaData.getCheckpointId());
        }
        assertTrue(checkpointMetaData.getTimestamp() > 0);

        nextExpectedCheckpointId = checkpointMetaData.getCheckpointId() + 1;
        triggeredCheckpointCounter++;

        lastAlignmentDurationNanos = checkpointMetrics.getAlignmentDurationNanos();
        lastBytesProcessedDuringAlignment = checkpointMetrics.getBytesProcessedDuringAlignment();

        if (!checkpointOptions.isUnalignedCheckpoint()) {
            Preconditions.checkCompletedNormally(lastAlignmentDurationNanos);
            Preconditions.checkCompletedNormally(lastBytesProcessedDuringAlignment);
        }

        triggeredCheckpoints.add(checkpointMetaData.getCheckpointId());
        triggeredCheckpointOptions.add(checkpointOptions);
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
        lastCanceledCheckpointId = checkpointId;
        failureReason = cause.getCheckpointFailureReason();
        abortedCheckpointCounter++;
        abortedCheckpoints.add(checkpointId);
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        throw new UnsupportedOperationException("should never be called");
    }
}
