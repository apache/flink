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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in a
 * synchronized block that locks on the lock Object. Also, the modification of the state and the
 * emission of elements must happen in the same block of code that is protected by the synchronized
 * block.
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@Deprecated
@Internal
public class SourceStreamTask<
                OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private enum FinishingReason {
        END_OF_DATA(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_DRAIN(StopMode.DRAIN),
        STOP_WITH_SAVEPOINT_NO_DRAIN(StopMode.NO_DRAIN);

        private final StopMode stopMode;

        FinishingReason(StopMode stopMode) {
            this.stopMode = stopMode;
        }

        StopMode toStopMode() {
            return this.stopMode;
        }
    }

    /**
     * Indicates whether this Task was purposefully finished, in this case we want to ignore
     * exceptions thrown after finishing, to ensure shutdown works smoothly.
     *
     * <p>Moreover we differentiate drain and no drain cases to see if we need to call finish() on
     * the operators.
     */
    private volatile FinishingReason finishingReason = FinishingReason.END_OF_DATA;

    public SourceStreamTask(Environment env) throws Exception {
        this(env, new Object());
    }

    private SourceStreamTask(Environment env, Object lock) throws Exception {
        super(
                env,
                null,
                FatalExitExceptionHandler.INSTANCE,
                StreamTaskActionExecutor.synchronizedExecutor(lock));
        this.lock = Preconditions.checkNotNull(lock);
        this.sourceThread = new LegacySourceFunctionThread();

        getEnvironment().getMetricGroup().getIOMetricGroup().setEnableBusyTime(false);
    }

    @Override
    protected void init() {
        // we check if the source is actually inducing the checkpoints, rather
        // than the trigger
        SourceFunction<?> source = mainOperator.getUserFunction();
        if (source instanceof ExternallyInducedSource) {
            externallyInducedCheckpoints = true;

            ExternallyInducedSource.CheckpointTrigger triggerHook =
                    new ExternallyInducedSource.CheckpointTrigger() {

                        @Override
                        public void triggerCheckpoint(long checkpointId) throws FlinkException {
                            // TODO - we need to see how to derive those. We should probably not
                            // encode this in the
                            // TODO -   source's trigger message, but do a handshake in this task
                            // between the trigger
                            // TODO -   message from the master, and the source's trigger
                            // notification
                            final CheckpointOptions checkpointOptions =
                                    CheckpointOptions.forConfig(
                                            CheckpointType.CHECKPOINT,
                                            CheckpointStorageLocationReference.getDefault(),
                                            configuration.isExactlyOnceCheckpointMode(),
                                            configuration.isUnalignedCheckpointsEnabled(),
                                            configuration.getAlignedCheckpointTimeout().toMillis());
                            final long timestamp = System.currentTimeMillis();

                            final CheckpointMetaData checkpointMetaData =
                                    new CheckpointMetaData(checkpointId, timestamp, timestamp);

                            try {
                                SourceStreamTask.super
                                        .triggerCheckpointAsync(
                                                checkpointMetaData, checkpointOptions)
                                        .get();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new FlinkException(e.getMessage(), e);
                            }
                        }
                    };

            ((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
        }
        getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .gauge(
                        MetricNames.CHECKPOINT_START_DELAY_TIME,
                        this::getAsyncCheckpointStartDelayNanos);
        recordWriter.setMaxOverdraftBuffersPerGate(0);
    }

    @Override
    protected void advanceToEndOfEventTime() throws Exception {
        operatorChain.getMainOperatorOutput().emitWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    protected void cleanUpInternal() {
        // does not hold any resources, so no cleanup needed
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {

        controller.suspendDefaultAction();

        // Against the usual contract of this method, this implementation is not step-wise but
        // blocking instead for
        // compatibility reasons with the current source interface (source functions run as a loop,
        // not in steps).
        sourceThread.setTaskDescription(getName());

        sourceThread.start();

        sourceThread
                .getCompletionFuture()
                .whenComplete(
                        (Void ignore, Throwable sourceThreadThrowable) -> {
                            if (sourceThreadThrowable != null) {
                                mailboxProcessor.reportThrowable(sourceThreadThrowable);
                            } else {
                                notifyEndOfData();
                                mailboxProcessor.suspend();
                            }
                        });
    }

    @Override
    protected void cancelTask() {
        if (stopped.compareAndSet(false, true)) {
            cancelOperator();
        }
    }

    private void cancelOperator() {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            if (sourceThread.isAlive()) {
                interruptSourceThread();
            } else if (!sourceThread.getCompletionFuture().isDone()) {
                // sourceThread not alive and completion future not done means source thread
                // didn't start and we need to manually complete the future
                sourceThread.getCompletionFuture().complete(null);
            }
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        super.maybeInterruptOnCancel(toInterrupt, taskName, timeout);
        interruptSourceThread();
    }

    private void interruptSourceThread() {
        // Nothing need to do if the source is finished on restore
        if (operatorChain != null && operatorChain.isTaskDeployedAsFinished()) {
            return;
        }

        if (sourceThread.isAlive()) {
            sourceThread.interrupt();
        }
    }

    @Override
    protected CompletableFuture<Void> getCompletionFuture() {
        return sourceThread.getCompletionFuture();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        if (!externallyInducedCheckpoints) {
            if (isSynchronousSavepoint(checkpointOptions.getCheckpointType())) {
                return triggerStopWithSavepointAsync(checkpointMetaData, checkpointOptions);
            } else {
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }
        } else if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)) {
            // see FLINK-25256
            throw new IllegalStateException(
                    "Using externally induced sources, we can not enforce taking a full checkpoint."
                            + "If you are restoring from a snapshot in NO_CLAIM mode, please use"
                            + " either CLAIM or LEGACY mode.");
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    private boolean isSynchronousSavepoint(SnapshotType snapshotType) {
        return snapshotType.isSavepoint() && ((SavepointType) snapshotType).isSynchronous();
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        mainMailboxExecutor.execute(
                () ->
                        stopOperatorForStopWithSavepoint(
                                checkpointMetaData.getCheckpointId(),
                                ((SavepointType) checkpointOptions.getCheckpointType())
                                        .shouldDrain()),
                "stop legacy source for stop-with-savepoint --drain");
        return sourceThread
                .getCompletionFuture()
                .thenCompose(
                        ignore ->
                                super.triggerCheckpointAsync(
                                        checkpointMetaData, checkpointOptions));
    }

    private void stopOperatorForStopWithSavepoint(long checkpointId, boolean drain) {
        setSynchronousSavepoint(checkpointId);
        finishingReason =
                drain
                        ? FinishingReason.STOP_WITH_SAVEPOINT_DRAIN
                        : FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN;
        if (mainOperator != null) {
            mainOperator.stop();
        }
    }

    @Override
    protected void declineCheckpoint(long checkpointId) {
        if (!externallyInducedCheckpoints) {
            super.declineCheckpoint(checkpointId);
        }
    }

    /** Runnable that executes the source function in the head operator. */
    private class LegacySourceFunctionThread extends Thread {

        private final CompletableFuture<Void> completionFuture;

        LegacySourceFunctionThread() {
            this.completionFuture = new CompletableFuture<>();
        }

        @Override
        public void run() {
            try {
                if (!operatorChain.isTaskDeployedAsFinished()) {
                    LOG.debug(
                            "Legacy source {} skip execution since the task is finished on restore",
                            getTaskNameWithSubtaskAndId());
                    mainOperator.run(lock, operatorChain);
                }
                completeProcessing();
                completionFuture.complete(null);
            } catch (Throwable t) {
                // Note, t can be also an InterruptedException
                if (isCanceled()
                        && ExceptionUtils.findThrowable(t, InterruptedException.class)
                                .isPresent()) {
                    completionFuture.completeExceptionally(new CancelTaskException(t));
                } else {
                    completionFuture.completeExceptionally(t);
                }
            }
        }

        private void completeProcessing() throws InterruptedException, ExecutionException {
            if (!isCanceled() && !isFailing()) {
                mainMailboxExecutor
                        .submit(
                                () -> {
                                    // theoretically the StreamSource can implement BoundedOneInput,
                                    // so we need to call it here
                                    final StopMode stopMode = finishingReason.toStopMode();
                                    if (stopMode == StopMode.DRAIN) {
                                        operatorChain.endInput(1);
                                    }
                                    endData(stopMode);
                                },
                                "SourceStreamTask finished processing data.")
                        .get();
            }
        }

        public void setTaskDescription(final String taskDescription) {
            setName("Legacy Source Thread - " + taskDescription);
        }

        /**
         * @return future that is completed once this thread completes. If this task {@link
         *     #isFailing()} and this thread is not alive (e.g. not started) returns a normally
         *     completed future.
         */
        CompletableFuture<Void> getCompletionFuture() {
            return isFailing() && !isAlive()
                    ? CompletableFuture.completedFuture(null)
                    : completionFuture;
        }
    }
}
