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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskExternallyInducedSourceInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A subclass of {@link StreamTask} for executing the {@link SourceOperator}. */
@Internal
public class SourceOperatorStreamTask<T> extends StreamTask<T, SourceOperator<T, ?>> {

    private AsyncDataOutputToOutput<T> output;
    /**
     * Contains information about all checkpoints where RPC from checkpoint coordinator arrives
     * before the source reader triggers it. (Common case)
     */
    private SortedMap<Long, UntriggeredCheckpoint> untriggeredCheckpoints = new TreeMap<>();
    /**
     * Contains the checkpoints that are triggered by the source but the RPC from checkpoint
     * coordinator has yet to arrive. This may happen if the barrier is inserted as an event into
     * the data plane by the source coordinator and the (distributed) source reader reads that event
     * before receiving Flink's checkpoint RPC. (Rare case)
     */
    private SortedSet<Long> triggeredCheckpoints = new TreeSet<>();
    /**
     * Blocks input until the RPC call has been received that corresponds to the triggered
     * checkpoint. This future must only be accessed and completed in the mailbox thread.
     */
    private CompletableFuture<Void> waitForRPC = FutureUtils.completedVoidFuture();
    /** Only set for externally induced sources. See also {@link #isExternallyInducedSource()}. */
    private StreamTaskExternallyInducedSourceInput<T> externallyInducedSourceInput;

    public SourceOperatorStreamTask(Environment env) throws Exception {
        super(env);
    }

    @Override
    public void init() throws Exception {
        final SourceOperator<T, ?> sourceOperator = this.mainOperator;
        // reader initialization, which cannot happen in the constructor due to the
        // lazy metric group initialization. We do this here now, rather than
        // later (in open()) so that we can access the reader when setting up the
        // input processors
        sourceOperator.initReader();

        final SourceReader<T, ?> sourceReader = sourceOperator.getSourceReader();
        final StreamTaskInput<T> input;

        // TODO: should the input be constructed inside the `OperatorChain` class?
        if (operatorChain.isTaskDeployedAsFinished()) {
            input = new StreamTaskFinishedOnRestoreSourceInput<>(sourceOperator, 0, 0);
        } else if (sourceReader instanceof ExternallyInducedSourceReader) {
            externallyInducedSourceInput =
                    new StreamTaskExternallyInducedSourceInput<>(
                            sourceOperator,
                            this::triggerCheckpointForExternallyInducedSource,
                            0,
                            0);

            input = externallyInducedSourceInput;
        } else {
            input = new StreamTaskSourceInput<>(sourceOperator, 0, 0);
        }

        // The SourceOperatorStreamTask doesn't have any inputs, so there is no need for
        // a WatermarkGauge on the input.
        output =
                new AsyncDataOutputToOutput<T>(
                        operatorChain.getMainOperatorOutput(),
                        sourceOperator.getSourceMetricGroup(),
                        null);

        inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);

        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
    }

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!isExternallyInducedSource()) {
            return triggerCheckpointNowAsync(checkpointMetaData, checkpointOptions);
        }
        CompletableFuture<Boolean> triggerFuture = new CompletableFuture<>();
        // immediately move RPC to mailbox so we don't need to synchronize fields
        mainMailboxExecutor.execute(
                () ->
                        triggerCheckpointOnExternallyInducedSource(
                                checkpointMetaData, checkpointOptions, triggerFuture),
                "SourceOperatorStreamTask#triggerCheckpointAsync(%s, %s)",
                checkpointMetaData,
                checkpointOptions);
        return triggerFuture;
    }

    private boolean isExternallyInducedSource() {
        return externallyInducedSourceInput != null;
    }

    private void triggerCheckpointOnExternallyInducedSource(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CompletableFuture<Boolean> triggerFuture) {
        assert (mailboxProcessor.isMailboxThread());
        if (!triggeredCheckpoints.remove(checkpointMetaData.getCheckpointId())) {
            // common case: RPC is received before source reader triggers checkpoint
            // store metadata and options for later
            untriggeredCheckpoints.put(
                    checkpointMetaData.getCheckpointId(),
                    new UntriggeredCheckpoint(checkpointMetaData, checkpointOptions));
            triggerFuture.complete(isRunning());
        } else {
            // trigger already received (rare case)
            FutureUtils.forward(
                    triggerCheckpointNowAsync(checkpointMetaData, checkpointOptions),
                    triggerFuture);

            cleanupOldCheckpoints(checkpointMetaData.getCheckpointId());
        }
    }

    private CompletableFuture<Boolean> triggerCheckpointNowAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (isSynchronous(checkpointOptions.getCheckpointType())) {
            return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
        } else {
            return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
        }
    }

    private boolean isSynchronous(SnapshotType checkpointType) {
        return checkpointType.isSavepoint() && ((SavepointType) checkpointType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Void> operatorFinished = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                    FutureUtils.forward(
                            mainOperator.stop(
                                    ((SavepointType) checkpointOptions.getCheckpointType())
                                                    .shouldDrain()
                                            ? StopMode.DRAIN
                                            : StopMode.NO_DRAIN),
                            operatorFinished);
                },
                "stop Flip-27 source for stop-with-savepoint");

        return operatorFinished.thenCompose(
                (ignore) -> super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions));
    }

    @Override
    protected void advanceToEndOfEventTime() {
        output.emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        cleanupCheckpoint(checkpointId);
        super.declineCheckpoint(checkpointId);
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        mainMailboxExecutor.execute(
                () -> cleanupCheckpoint(checkpointId), "Cleanup checkpoint %d", checkpointId);
        return super.notifyCheckpointAbortAsync(checkpointId, latestCompletedCheckpointId);
    }

    @Override
    public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
        mainMailboxExecutor.execute(
                () -> cleanupCheckpoint(checkpointId), "Cleanup checkpoint %d", checkpointId);
        return super.notifyCheckpointSubsumedAsync(checkpointId);
    }

    // --------------------------

    private void triggerCheckpointForExternallyInducedSource(long checkpointId) {
        UntriggeredCheckpoint untriggeredCheckpoint = untriggeredCheckpoints.remove(checkpointId);
        if (untriggeredCheckpoint != null) {
            // common case: RPC before external sources induces it
            triggerCheckpointNowAsync(
                    untriggeredCheckpoint.getMetadata(),
                    untriggeredCheckpoint.getCheckpointOptions());
            cleanupOldCheckpoints(checkpointId);
        } else {
            // rare case: external source induced first
            triggeredCheckpoints.add(checkpointId);
            if (waitForRPC.isDone()) {
                waitForRPC = new CompletableFuture<>();
                externallyInducedSourceInput.blockUntil(waitForRPC);
            }
        }
    }

    /**
     * Cleanup any orphaned checkpoint before the given currently triggered checkpoint. These
     * checkpoint may occur when the checkpoint is cancelled but the RPC is lost. Note, to be safe,
     * checkpoint X is only removed when both RPC and trigger for a checkpoint Y>X is received.
     */
    private void cleanupOldCheckpoints(long checkpointId) {
        assert (mailboxProcessor.isMailboxThread());
        triggeredCheckpoints.headSet(checkpointId).clear();
        untriggeredCheckpoints.headMap(checkpointId).clear();

        maybeResumeProcessing();
    }

    /** Resumes processing if it was blocked before or else is a no-op. */
    private void maybeResumeProcessing() {
        assert (mailboxProcessor.isMailboxThread());

        if (triggeredCheckpoints.isEmpty()) {
            waitForRPC.complete(null);
        }
    }

    /** Remove temporary data about a canceled checkpoint. */
    private void cleanupCheckpoint(long checkpointId) {
        assert (mailboxProcessor.isMailboxThread());
        triggeredCheckpoints.remove(checkpointId);
        untriggeredCheckpoints.remove(checkpointId);

        maybeResumeProcessing();
    }

    // ---------------------------

    /** Implementation of {@link DataOutput} that wraps a specific {@link Output}. */
    public static class AsyncDataOutputToOutput<T> implements DataOutput<T> {

        private final Output<StreamRecord<T>> output;
        private final InternalSourceReaderMetricGroup metricGroup;
        @Nullable private final WatermarkGauge inputWatermarkGauge;

        public AsyncDataOutputToOutput(
                Output<StreamRecord<T>> output,
                InternalSourceReaderMetricGroup metricGroup,
                @Nullable WatermarkGauge inputWatermarkGauge) {

            this.output = checkNotNull(output);
            this.inputWatermarkGauge = inputWatermarkGauge;
            this.metricGroup = metricGroup;
        }

        @Override
        public void emitRecord(StreamRecord<T> streamRecord) {
            metricGroup.recordEmitted(streamRecord.getTimestamp());
            output.collect(streamRecord);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            output.emitLatencyMarker(latencyMarker);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            long watermarkTimestamp = watermark.getTimestamp();
            if (inputWatermarkGauge != null) {
                inputWatermarkGauge.setCurrentWatermark(watermarkTimestamp);
            }
            metricGroup.watermarkEmitted(watermarkTimestamp);
            output.emitWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            output.emitWatermarkStatus(watermarkStatus);
        }
    }

    private static class UntriggeredCheckpoint {
        private final CheckpointMetaData metadata;
        private final CheckpointOptions checkpointOptions;

        private UntriggeredCheckpoint(
                CheckpointMetaData metadata, CheckpointOptions checkpointOptions) {
            this.metadata = metadata;
            this.checkpointOptions = checkpointOptions;
        }

        public CheckpointMetaData getMetadata() {
            return metadata;
        }

        public CheckpointOptions getCheckpointOptions() {
            return checkpointOptions;
        }
    }
}
