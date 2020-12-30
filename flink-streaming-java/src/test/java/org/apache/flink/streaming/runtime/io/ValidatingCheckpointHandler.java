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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The invokable handler used for triggering checkpoint and validation. */
public class ValidatingCheckpointHandler extends AbstractInvokable {

    protected CheckpointFailureReason failureReason;
    protected long lastCanceledCheckpointId = -1L;
    protected long nextExpectedCheckpointId;
    protected long triggeredCheckpointCounter = 0;
    protected long abortedCheckpointCounter = 0;
    protected CompletableFuture<Long> lastAlignmentDurationNanos;
    protected CompletableFuture<Long> lastBytesProcessedDuringAlignment;
    protected final List<Long> triggeredCheckpoints = new ArrayList<>();
    protected final List<CheckpointOptions> triggeredCheckpointOptions = new ArrayList<>();

    public ValidatingCheckpointHandler() {
        this(-1);
    }

    public ValidatingCheckpointHandler(long nextExpectedCheckpointId) {
        super(new DummyEnvironment("test", 1, 0));
        this.nextExpectedCheckpointId = nextExpectedCheckpointId;
    }

    public void setNextExpectedCheckpointId(long nextExpectedCheckpointId) {
        this.nextExpectedCheckpointId = nextExpectedCheckpointId;
    }

    public CheckpointFailureReason getCheckpointFailureReason() {
        return failureReason;
    }

    public long getLastCanceledCheckpointId() {
        return lastCanceledCheckpointId;
    }

    public long getTriggeredCheckpointCounter() {
        return triggeredCheckpointCounter;
    }

    public long getAbortedCheckpointCounter() {
        return abortedCheckpointCounter;
    }

    public long getNextExpectedCheckpointId() {
        return nextExpectedCheckpointId;
    }

    public CompletableFuture<Long> getLastAlignmentDurationNanos() {
        return lastAlignmentDurationNanos;
    }

    public CompletableFuture<Long> getLastBytesProcessedDuringAlignment() {
        return lastBytesProcessedDuringAlignment;
    }

    public List<CheckpointOptions> getTriggeredCheckpointOptions() {
        return triggeredCheckpointOptions;
    }

    @Override
    public void invoke() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            boolean advanceToEndOfEventTime) {
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

        triggeredCheckpoints.add(checkpointMetaData.getCheckpointId());
        triggeredCheckpointOptions.add(checkpointOptions);
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) {
        lastCanceledCheckpointId = checkpointId;
        failureReason = ((CheckpointException) cause).getCheckpointFailureReason();
        abortedCheckpointCounter++;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        throw new UnsupportedOperationException("should never be called");
    }
}
