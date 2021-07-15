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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.clock.Clock;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input
 * channels. Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public abstract class CheckpointBarrierHandler implements Closeable {
    private static final long OUTSIDE_OF_ALIGNMENT = Long.MIN_VALUE;

    /** The listener to be notified on complete checkpoints. */
    private final AbstractInvokable toNotifyOnCheckpoint;

    private final Clock clock;

    /** The time (in nanoseconds) that the latest alignment took. */
    private CompletableFuture<Long> latestAlignmentDurationNanos = new CompletableFuture<>();

    /**
     * The time (in nanoseconds) between creation of the checkpoint's first checkpoint barrier and
     * receiving it by this task.
     */
    private long latestCheckpointStartDelayNanos;

    /** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
    private long startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;

    /** ID of checkpoint for which alignment was started last. */
    private long startAlignmentCheckpointId = -1;

    /**
     * Cumulative counter of bytes processed during alignment. Once we complete alignment, we will
     * put this value into the {@link #latestBytesProcessedDuringAlignment}.
     */
    private long bytesProcessedDuringAlignment;

    private CompletableFuture<Long> latestBytesProcessedDuringAlignment = new CompletableFuture<>();

    public CheckpointBarrierHandler(AbstractInvokable toNotifyOnCheckpoint, Clock clock) {
        this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
        this.clock = checkNotNull(clock);
    }

    @Override
    public void close() throws IOException {}

    public abstract void processBarrier(
            CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws IOException;

    public abstract void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException;

    public abstract void processCancellationBarrier(CancelCheckpointMarker cancelBarrier)
            throws IOException;

    public abstract void processEndOfPartition() throws IOException;

    public abstract long getLatestCheckpointId();

    public long getAlignmentDurationNanos() {
        if (isDuringAlignment()) {
            return clock.relativeTimeNanos() - startOfAlignmentTimestamp;
        } else {
            return FutureUtils.getOrDefault(latestAlignmentDurationNanos, 0L);
        }
    }

    public long getCheckpointStartDelayNanos() {
        return latestCheckpointStartDelayNanos;
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        return CompletableFuture.completedFuture(null);
    }

    protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException {
        CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointBarrier.getId(),
                        checkpointBarrier.getTimestamp(),
                        System.currentTimeMillis());

        CheckpointMetricsBuilder checkpointMetrics;
        if (checkpointBarrier.getId() == startAlignmentCheckpointId) {
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(latestAlignmentDurationNanos)
                            .setBytesProcessedDuringAlignment(latestBytesProcessedDuringAlignment)
                            .setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);
        } else {
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(0);
        }

        toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
                checkpointMetaData, checkpointBarrier.getCheckpointOptions(), checkpointMetrics);
    }

    protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
        notifyAbort(
                checkpointId,
                new CheckpointException(
                        CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
    }

    protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
        toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
    }

    protected void markAlignmentStartAndEnd(long checkpointId, long checkpointCreationTimestamp) {
        markAlignmentStart(checkpointId, checkpointCreationTimestamp);
        markAlignmentEnd(0);
    }

    protected void markAlignmentStart(long checkpointId, long checkpointCreationTimestamp) {
        latestCheckpointStartDelayNanos =
                1_000_000 * Math.max(0, clock.absoluteTimeMillis() - checkpointCreationTimestamp);

        resetAlignment();
        startOfAlignmentTimestamp = clock.relativeTimeNanos();
        startAlignmentCheckpointId = checkpointId;
    }

    protected void markAlignmentEnd() {
        markAlignmentEnd(clock.relativeTimeNanos() - startOfAlignmentTimestamp);
    }

    protected void markAlignmentEnd(long alignmentDuration) {
        checkState(
                alignmentDuration >= 0,
                "Alignment time is less than zero({}). Is the time monotonic?",
                alignmentDuration);

        latestAlignmentDurationNanos.complete(alignmentDuration);
        latestBytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment);

        startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
        bytesProcessedDuringAlignment = 0;
    }

    protected void resetAlignment() {
        markAlignmentEnd(0);
        latestAlignmentDurationNanos = new CompletableFuture<>();
        latestBytesProcessedDuringAlignment = new CompletableFuture<>();
    }

    protected abstract boolean isCheckpointPending();

    public void addProcessedBytes(int bytes) {
        if (isDuringAlignment()) {
            bytesProcessedDuringAlignment += bytes;
        }
    }

    private boolean isDuringAlignment() {
        return startOfAlignmentTimestamp > OUTSIDE_OF_ALIGNMENT;
    }

    protected final Clock getClock() {
        return clock;
    }

    /** A handle to a delayed action which can be cancelled. */
    interface Cancellable {
        void cancel();
    }
}
