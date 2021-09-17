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
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
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
 */
@Internal
public class SourceStreamTask<
                OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
        extends StreamTask<OUT, OP> {

    private final LegacySourceFunctionThread sourceThread;
    private final Object lock;

    private volatile boolean externallyInducedCheckpoints;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private enum FinishingReason {
        END_OF_DATA(true),
        STOP_WITH_SAVEPOINT_DRAIN(true),
        STOP_WITH_SAVEPOINT_NO_DRAIN(false);

        private final boolean shouldCallFinish;

        FinishingReason(boolean shouldCallFinish) {
            this.shouldCallFinish = shouldCallFinish;
        }

        boolean shouldCallFinish() {
            return this.shouldCallFinish;
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
                                mailboxProcessor.suspend();
                            }
                        });
    }

    @Override
    protected void cancelTask() {
        if (stopped.compareAndSet(false, true)) {
            if (isFailing()) {
                interruptSourceThread(true);
            }
            cancelOperator(true);
        }
    }

    @Override
    protected void finishTask() {
        this.finishingReason = FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN;
        /**
         * Currently stop with savepoint relies on the EndOfPartitionEvents propagation and performs
         * clean shutdown after the stop with savepoint (which can produce some records to process
         * after the savepoint while stopping). If we interrupt source thread, we might leave the
         * network stack in an inconsistent state. So, if we want to relay on the clean shutdown, we
         * can not interrupt the source thread.
         */
        cancelOperator(false);
    }

    private void cancelOperator(boolean interruptThread) {
        try {
            if (mainOperator != null) {
                mainOperator.cancel();
            }
        } finally {
            interruptSourceThread(interruptThread);
        }
    }

    private void interruptSourceThread(boolean interrupt) {
        // Nothing need to do if the source is finished on restore
        if (operatorChain != null && operatorChain.isTaskDeployedAsFinished()) {
            return;
        }

        if (sourceThread.isAlive()) {
            if (interrupt) {
                sourceThread.interrupt();
            }
        } else if (!sourceThread.getCompletionFuture().isDone()) {
            // source thread didn't start
            sourceThread.getCompletionFuture().complete(null);
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
            if (checkpointOptions.getCheckpointType().shouldDrain()) {
                return triggerStopWithSavepointWithDrainAsync(
                        checkpointMetaData, checkpointOptions);
            } else {
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions);
            }
        } else {
            // we do not trigger checkpoints here, we simply state whether we can trigger them
            synchronized (lock) {
                return CompletableFuture.completedFuture(isRunning());
            }
        }
    }

    private CompletableFuture<Boolean> triggerStopWithSavepointWithDrainAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        mainMailboxExecutor.execute(
                () ->
                        stopOperatorForStopWithSavepointWithDrain(
                                checkpointMetaData.getCheckpointId()),
                "stop legacy source for stop-with-savepoint --drain");
        return assertTriggeringCheckpointExceptions(
                sourceThread
                        .getCompletionFuture()
                        .thenCompose(
                                ignore ->
                                        super.triggerCheckpointAsync(
                                                checkpointMetaData, checkpointOptions)),
                checkpointMetaData.getCheckpointId());
    }

    private void stopOperatorForStopWithSavepointWithDrain(long checkpointId) {
        setSynchronousSavepoint(checkpointId, true);
        finishingReason = FinishingReason.STOP_WITH_SAVEPOINT_DRAIN;
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

    /** Runnable that executes the the source function in the head operator. */
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
                } else if (finishingReason == FinishingReason.STOP_WITH_SAVEPOINT_NO_DRAIN) {
                    // swallow all exceptions if the source was stopped without drain
                    completionFuture.complete(null);
                } else {
                    completionFuture.completeExceptionally(t);
                }
            }
        }

        private void completeProcessing() throws InterruptedException, ExecutionException {
            if (finishingReason.shouldCallFinish() && !isCanceled() && !isFailing()) {
                mainMailboxExecutor
                        .submit(
                                () -> {
                                    // theoretically the StreamSource can implement BoundedOneInput,
                                    // so we
                                    // need to call it here
                                    operatorChain.endInput(1);
                                    endData();
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
