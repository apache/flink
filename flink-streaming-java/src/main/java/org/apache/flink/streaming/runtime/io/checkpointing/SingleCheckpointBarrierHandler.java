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
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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

    /**
     * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the
     * same barrier from different channels.
     */
    private long currentCheckpointId = -1L;

    /**
     * The checkpoint barrier of the current pending checkpoint. It is to allow us to access the
     * checkpoint options when processing {@code EndOfPartitionEvent}.
     */
    @Nullable private CheckpointBarrier pendingCheckpointBarrier;

    private final Set<InputChannelInfo> alignedChannels = new HashSet<>();

    private int targetChannelCount;

    private long lastCancelledOrCompletedCheckpointId = -1L;

    private int numOpenChannels;

    private CompletableFuture<Void> allBarriersReceivedFuture = new CompletableFuture<>();

    private BarrierHandlerState currentState;
    private Cancellable currentAlignmentTimer;
    private final boolean alternating;

    @VisibleForTesting
    public static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            boolean enableCheckpointsAfterTasksFinish,
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
                enableCheckpointsAfterTasksFinish,
                inputs);
    }

    public static SingleCheckpointBarrierHandler unaligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrierUnaligned(false, new ChannelState(inputs)),
                false,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler aligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            boolean enableCheckpointAfterTasksFinished,
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
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler alternating(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrier(new ChannelState(inputs)),
                true,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    private SingleCheckpointBarrierHandler(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            @Nullable SubtaskCheckpointCoordinator subTaskCheckpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BarrierHandlerState currentState,
            boolean alternating,
            BiFunction<Callable<?>, Duration, Cancellable> registerTimer,
            CheckpointableInput[] inputs,
            boolean enableCheckpointAfterTasksFinished) {
        super(toNotifyOnCheckpoint, clock, enableCheckpointAfterTasksFinished);

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
    public void processBarrier(
            CheckpointBarrier barrier, InputChannelInfo channelInfo, boolean isRpcTriggered)
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

        markCheckpointAlignedAndTransformState(
                channelInfo,
                barrier,
                state -> state.barrierReceived(context, channelInfo, barrier, !isRpcTriggered));
    }

    protected void markCheckpointAlignedAndTransformState(
            InputChannelInfo alignedChannel,
            CheckpointBarrier barrier,
            FunctionWithException<BarrierHandlerState, BarrierHandlerState, Exception>
                    stateTransformer)
            throws IOException {

        alignedChannels.add(alignedChannel);
        if (alignedChannels.size() == 1) {
            if (targetChannelCount == 1) {
                markAlignmentStartAndEnd(barrier.getId(), barrier.getTimestamp());
            } else {
                markAlignmentStart(barrier.getId(), barrier.getTimestamp());
            }
        }

        // we must mark alignment end before calling currentState.barrierReceived which might
        // trigger a checkpoint with unfinished future for alignment duration
        if (alignedChannels.size() == targetChannelCount) {
            if (targetChannelCount > 1) {
                markAlignmentEnd();
            }
        }

        try {
            currentState = stateTransformer.apply(currentState);
        } catch (CheckpointException e) {
            abortInternal(currentCheckpointId, e);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }

        if (alignedChannels.size() == targetChannelCount) {
            alignedChannels.clear();
            lastCancelledOrCompletedCheckpointId = currentCheckpointId;
            LOG.debug(
                    "{}: All the channels are aligned for checkpoint {}.",
                    taskName,
                    currentCheckpointId);
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
        checkNewCheckpoint(announcedBarrier);

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
        long alignedCheckpointTimeout =
                announcedBarrier.getCheckpointOptions().getAlignedCheckpointTimeout();
        long timePassedSinceCheckpointStart =
                getClock().absoluteTimeMillis() - announcedBarrier.getTimestamp();

        long timerDelay = Math.max(alignedCheckpointTimeout - timePassedSinceCheckpointStart, 0);

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
                        Duration.ofMillis(timerDelay));
    }

    private void checkNewCheckpoint(CheckpointBarrier barrier) throws IOException {
        long barrierId = barrier.getId();
        if (currentCheckpointId >= barrierId) {
            return; // This barrier is not the first for this checkpoint.
        }

        if (isCheckpointPending()) {
            cancelSubsumedCheckpoint(barrierId);
        }
        currentCheckpointId = barrierId;
        pendingCheckpointBarrier = barrier;
        alignedChannels.clear();
        targetChannelCount = numOpenChannels;
        allBarriersReceivedFuture = new CompletableFuture<>();

        if (alternating && barrier.getCheckpointOptions().isTimeoutable()) {
            registerAlignmentTimer(barrier);
        }
    }

    @Override
    public void processCancellationBarrier(
            CancelCheckpointMarker cancelBarrier, InputChannelInfo channelInfo) throws IOException {
        final long cancelledId = cancelBarrier.getCheckpointId();
        if (cancelledId > currentCheckpointId
                || (cancelledId == currentCheckpointId && alignedChannels.size() > 0)) {
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
        pendingCheckpointBarrier = null;
        alignedChannels.clear();
        targetChannelCount = 0;
        resetAlignmentTimer();
        currentState = currentState.abort(cancelledId);
        if (cancelledId == currentCheckpointId) {
            resetAlignment();
        }
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
    public void processEndOfPartition(InputChannelInfo channelInfo) throws IOException {
        numOpenChannels--;

        if (!isCheckpointAfterTasksFinishedEnabled()) {
            if (isCheckpointPending()) {
                LOG.warn(
                        "{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
                        taskName,
                        currentCheckpointId);
                abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_INPUT_END_OF_STREAM);
            }
        } else {
            if (!isCheckpointPending()) {
                return;
            }

            checkState(
                    pendingCheckpointBarrier != null,
                    "pending checkpoint barrier should not be null when"
                            + " there is pending checkpoint.");

            markCheckpointAlignedAndTransformState(
                    channelInfo,
                    pendingCheckpointBarrier,
                    state -> state.endOfPartitionReceived(context, channelInfo));
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
        if (checkpointId < currentCheckpointId || numOpenChannels == 0) {
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
                "%s: current checkpoint: %d, current aligned channels: %d, target channel count: %d",
                taskName, currentCheckpointId, alignedChannels.size(), targetChannelCount);
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
                    && barrier.getCheckpointOptions().getAlignedCheckpointTimeout()
                            < (getClock().absoluteTimeMillis() - barrier.getTimestamp());
        }

        @Override
        public boolean allBarriersReceived() {
            return alignedChannels.size() == targetChannelCount;
        }

        @Nullable
        @Override
        public CheckpointBarrier getPendingCheckpointBarrier() {
            return pendingCheckpointBarrier;
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
