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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SingleCheckpointBarrierHandler} is used for triggering checkpoint while reading the first
 * barrier and keeping track of the number of received barriers and consumed barriers. It can
 * handle/track just single checkpoint at a time. The behaviour when to actually trigger the
 * checkpoint and what the {@link CheckpointableInput} should do is controlled by {@link
 * BarrierHandlerState}.
 */
@Internal
@NotThreadSafe
public class SingleCheckpointBarrierHandler extends CheckpointBarrierHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SingleCheckpointBarrierHandler.class);

    private final String taskName;
    private final ControllerImpl context;
    private final BiFunction<Callable<?>, Duration, Cancellable> registerTimer;
    private final SubtaskCheckpointCoordinator subTaskCheckpointCoordinator;
    private final CheckpointableInput[] inputs;
    private int numBarriersReceived;

    /**
     * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the
     * same barrier from different channels.
     */
    private long currentCheckpointId = -1L;

    private long lastCancelledOrCompletedCheckpointId = -1L;

    private int numOpenChannels;

    private CompletableFuture<Void> allBarriersReceivedFuture = new CompletableFuture<>();

    private BarrierHandlerState currentState;
    private long firstBarrierArrivalTime;
    private Cancellable currentAlignmentTimer;
    private final boolean alternating;

    @VisibleForTesting
    public static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            Clock clock,
            CheckpointableInput... inputs) {
        return unaligned(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                (int)
                        Arrays.stream(inputs)
                                .flatMap(gate -> gate.getChannelInfos().stream())
                                .count(),
                (callable, duration) -> {
                    throw new IllegalStateException(
                            "Strictly unaligned checkpoints should never register any callbacks");
                },
                inputs);
    }

    public static SingleCheckpointBarrierHandler unaligned(
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new WaitingForFirstBarrierUnaligned(false, inputs),
                false,
                registerTimer,
                inputs);
    }

    public static SingleCheckpointBarrierHandler aligned(
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                null,
                clock,
                numOpenChannels,
                new WaitingForFirstBarrier(inputs),
                false,
                registerTimer,
                inputs);
    }

    public static SingleCheckpointBarrierHandler alternating(
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrier(inputs),
                true,
                registerTimer,
                inputs);
    }

    private SingleCheckpointBarrierHandler(
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            @Nullable SubtaskCheckpointCoordinator subTaskCheckpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BarrierHandlerState currentState,
            boolean alternating,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            CheckpointableInput[] inputs) {
        super(toNotifyOnCheckpoint, clock);

        this.taskName = taskName;
        this.numOpenChannels = numOpenChannels;
        this.currentState = currentState;
        this.alternating = alternating;
        this.registerTimer = registerTimer;
        this.subTaskCheckpointCoordinator = subTaskCheckpointCoordinator;
        this.context = new ControllerImpl();
        this.inputs = inputs;
    }

    @Override
    public void processBarrier(CheckpointBarrier barrier, InputChannelInfo channelInfo)
            throws IOException {
        long barrierId = barrier.getId();
        LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            if (!barrier.getCheckpointOptions().isUnalignedCheckpoint()) {
                inputs[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
            }
            return;
        }

        checkNewCheckpoint(barrier);
        checkState(currentCheckpointId == barrierId);

        if (numBarriersReceived++ == 0) {
            if (getNumOpenChannels() == 1) {
                markAlignmentStartAndEnd(barrier.getTimestamp());
            } else {
                markAlignmentStart(barrier.getTimestamp());
            }
        }

        // we must mark alignment end before calling currentState.barrierReceived which might
        // trigger a checkpoint with unfinished future for alignment duration
        if (numBarriersReceived == numOpenChannels) {
            if (getNumOpenChannels() > 1) {
                markAlignmentEnd();
            }
        }

        try {
            currentState = currentState.barrierReceived(context, channelInfo, barrier);
        } catch (CheckpointException e) {
            abortInternal(barrier.getId(), e);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }

        if (numBarriersReceived == numOpenChannels) {
            numBarriersReceived = 0;
            lastCancelledOrCompletedCheckpointId = currentCheckpointId;
            LOG.debug(
                    "{}: Received all barriers for checkpoint {}.", taskName, currentCheckpointId);
            resetAlignmentTimer();
            allBarriersReceivedFuture.complete(null);
        }
    }

    private void triggerCheckpoint(CheckpointBarrier trigger) throws IOException {
        LOG.debug(
                "{}: Triggering checkpoint {} on the barrier announcement at {}.",
                taskName,
                trigger.getId(),
                trigger.getTimestamp());
        notifyCheckpoint(trigger);
    }

    @Override
    public void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException {
        if (checkNewCheckpoint(announcedBarrier)) {
            firstBarrierArrivalTime = getClock().relativeTimeNanos();
            if (alternating) {
                registerAlignmentTimer(announcedBarrier);
            }
        }

        long barrierId = announcedBarrier.getId();
        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            LOG.debug(
                    "{}: Obsolete announcement of checkpoint {} for channel {}.",
                    taskName,
                    barrierId,
                    channelInfo);
            return;
        }

        currentState = currentState.announcementReceived(context, channelInfo, sequenceNumber);
    }

    private void registerAlignmentTimer(CheckpointBarrier announcedBarrier) {
        this.currentAlignmentTimer =
                registerTimer.apply(
                        () -> {
                            long barrierId = announcedBarrier.getId();
                            try {
                                if (currentCheckpointId == barrierId
                                        && !getAllBarriersReceivedFuture(barrierId).isDone()) {
                                    currentState =
                                            currentState.alignmentTimeout(
                                                    context, announcedBarrier);
                                }
                            } catch (CheckpointException ex) {
                                this.abortInternal(barrierId, ex);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            currentAlignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(
                                announcedBarrier.getCheckpointOptions().getAlignmentTimeout()));
    }

    private boolean checkNewCheckpoint(CheckpointBarrier barrier) throws IOException {
        long barrierId = barrier.getId();
        if (currentCheckpointId < barrierId) {
            if (isCheckpointPending()) {
                cancelSubsumedCheckpoint(barrierId);
            }
            currentCheckpointId = barrierId;
            numBarriersReceived = 0;
            allBarriersReceivedFuture = new CompletableFuture<>();
            return true;
        }
        return false;
    }

    @Override
    public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier)
            throws IOException {
        final long cancelledId = cancelBarrier.getCheckpointId();
        if (cancelledId > currentCheckpointId
                || (cancelledId == currentCheckpointId && numBarriersReceived > 0)) {
            LOG.debug("{}: Received cancellation {}.", taskName, cancelledId);
            abortInternal(
                    cancelledId,
                    new CheckpointException(
                            CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
        }
    }

    private void abortInternal(long cancelledId, CheckpointFailureReason reason)
            throws IOException {
        abortInternal(cancelledId, new CheckpointException(reason));
    }

    private void abortInternal(long cancelledId, CheckpointException exception) throws IOException {
        LOG.debug(
                "{}: Aborting checkpoint {} after exception {}.",
                taskName,
                currentCheckpointId,
                exception);
        // by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
        // at zero means that no checkpoint barrier can start a new alignment
        currentCheckpointId = Math.max(cancelledId, currentCheckpointId);
        lastCancelledOrCompletedCheckpointId =
                Math.max(lastCancelledOrCompletedCheckpointId, cancelledId);
        numBarriersReceived = 0;
        resetAlignmentTimer();
        currentState = currentState.abort(cancelledId);
        notifyAbort(cancelledId, exception);
        allBarriersReceivedFuture.completeExceptionally(exception);
    }

    private void resetAlignmentTimer() {
        if (currentAlignmentTimer != null) {
            currentAlignmentTimer.cancel();
            currentAlignmentTimer = null;
        }
    }

    @Override
    public void processEndOfPartition() throws IOException {
        numOpenChannels--;

        if (isCheckpointPending()) {
            LOG.warn(
                    "{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
                    taskName,
                    currentCheckpointId);
            abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_INPUT_END_OF_STREAM);
        }
    }

    @Override
    public long getLatestCheckpointId() {
        return currentCheckpointId;
    }

    @Override
    public void close() throws IOException {
        resetAlignmentTimer();
        allBarriersReceivedFuture.cancel(false);
        super.close();
    }

    @Override
    protected boolean isCheckpointPending() {
        return currentCheckpointId != lastCancelledOrCompletedCheckpointId
                && currentCheckpointId >= 0;
    }

    private void cancelSubsumedCheckpoint(long barrierId) throws IOException {
        LOG.warn(
                "{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. "
                        + "Skipping current checkpoint.",
                taskName,
                barrierId,
                currentCheckpointId);
        abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_SUBSUMED);
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        if (checkpointId < currentCheckpointId) {
            return FutureUtils.completedVoidFuture();
        }
        if (checkpointId > currentCheckpointId) {
            throw new IllegalStateException(
                    "Checkpoint " + checkpointId + " has not been started at all");
        }
        return allBarriersReceivedFuture;
    }

    @VisibleForTesting
    int getNumOpenChannels() {
        return numOpenChannels;
    }

    @Override
    public String toString() {
        return String.format(
                "%s: current checkpoint: %d, current barriers: %d, open channels: %d",
                taskName, currentCheckpointId, numBarriersReceived, numOpenChannels);
    }

    private final class ControllerImpl implements BarrierHandlerState.Controller {
        @Override
        public void triggerGlobalCheckpoint(CheckpointBarrier checkpointBarrier)
                throws IOException {
            SingleCheckpointBarrierHandler.this.triggerCheckpoint(checkpointBarrier);
        }

        @Override
        public boolean isTimedOut(CheckpointBarrier barrier) {
            return barrier.getCheckpointOptions().isTimeoutable()
                    && barrier.getId() <= currentCheckpointId
                    && barrier.getCheckpointOptions().getAlignmentTimeout() * 1_000_000
                            < (getClock().relativeTimeNanos() - firstBarrierArrivalTime);
        }

        @Override
        public boolean allBarriersReceived() {
            return numBarriersReceived == numOpenChannels;
        }

        @Override
        public void initInputsCheckpoint(CheckpointBarrier checkpointBarrier)
                throws CheckpointException {
            checkState(subTaskCheckpointCoordinator != null);
            long barrierId = checkpointBarrier.getId();
            subTaskCheckpointCoordinator.initInputsCheckpoint(
                    barrierId, checkpointBarrier.getCheckpointOptions());
        }
    }
}
