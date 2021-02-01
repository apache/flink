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
import org.apache.flink.util.function.TriFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;

/**
 * {@link SingleCheckpointBarrierHandler} is used for triggering checkpoint while reading the first
 * barrier and keeping track of the number of received barriers and consumed barriers. It can
 * handle/track just single checkpoint at a time. The behaviour when to actually trigger the
 * checkpoint and what the {@link CheckpointableInput} should do is controlled by {@link
 * CheckpointBarrierBehaviourController}.
 */
@Internal
@NotThreadSafe
public class SingleCheckpointBarrierHandler extends CheckpointBarrierHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SingleCheckpointBarrierHandler.class);

    private final String taskName;

    private final CheckpointBarrierBehaviourController controller;

    private int numBarriersReceived;

    /**
     * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the
     * same barrier from different channels.
     */
    private long currentCheckpointId = -1L;

    private long lastCancelledOrCompletedCheckpointId = -1L;

    private int numOpenChannels;

    private CompletableFuture<Void> allBarriersReceivedFuture = FutureUtils.completedVoidFuture();

    @VisibleForTesting
    public static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                (int)
                        Arrays.stream(inputs)
                                .flatMap(gate -> gate.getChannelInfos().stream())
                                .count(),
                new UnalignedController(checkpointCoordinator, inputs));
    }

    SingleCheckpointBarrierHandler(
            String taskName,
            AbstractInvokable toNotifyOnCheckpoint,
            int numOpenChannels,
            CheckpointBarrierBehaviourController controller) {
        super(toNotifyOnCheckpoint);

        this.taskName = taskName;
        this.numOpenChannels = numOpenChannels;
        this.controller = controller;
    }

    @Override
    public void processBarrier(CheckpointBarrier barrier, InputChannelInfo channelInfo)
            throws IOException {
        long barrierId = barrier.getId();
        LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            controller.obsoleteBarrierReceived(channelInfo, barrier);
            return;
        }

        checkSubsumedCheckpoint(channelInfo, barrier);

        if (numBarriersReceived == 0) {
            if (getNumOpenChannels() == 1) {
                markAlignmentStartAndEnd(barrier.getTimestamp());
            } else {
                markAlignmentStart(barrier.getTimestamp());
            }
            allBarriersReceivedFuture = new CompletableFuture<>();

            if (!handleBarrier(
                    barrier,
                    channelInfo,
                    CheckpointBarrierBehaviourController::preProcessFirstBarrier)) {
                return;
            }
        }

        if (!handleBarrier(
                barrier, channelInfo, CheckpointBarrierBehaviourController::barrierReceived)) {
            return;
        }

        if (currentCheckpointId == barrierId) {
            if (++numBarriersReceived == numOpenChannels) {
                if (getNumOpenChannels() > 1) {
                    markAlignmentEnd();
                }
                numBarriersReceived = 0;
                lastCancelledOrCompletedCheckpointId = currentCheckpointId;
                LOG.debug(
                        "{}: Received all barriers for checkpoint {}.",
                        taskName,
                        currentCheckpointId);
                handleBarrier(
                        barrier,
                        channelInfo,
                        CheckpointBarrierBehaviourController::postProcessLastBarrier);
                allBarriersReceivedFuture.complete(null);
            }
        }
    }

    private boolean handleBarrier(
            CheckpointBarrier barrier,
            InputChannelInfo channelInfo,
            TriFunctionWithException<
                            CheckpointBarrierBehaviourController,
                            InputChannelInfo,
                            CheckpointBarrier,
                            Optional<CheckpointBarrier>,
                            Exception>
                    controllerAction)
            throws IOException {
        try {
            Optional<CheckpointBarrier> triggerMaybe =
                    controllerAction.apply(controller, channelInfo, barrier);
            if (triggerMaybe.isPresent()) {
                CheckpointBarrier trigger = triggerMaybe.get();
                LOG.debug(
                        "{}: Triggering checkpoint {} on the barrier announcement at {}.",
                        taskName,
                        trigger.getId(),
                        trigger.getTimestamp());
                notifyCheckpoint(trigger);
            }
            return true;
        } catch (CheckpointException e) {
            LOG.debug(
                    "{}: Aborting checkpoint {} after exception {}.",
                    taskName,
                    currentCheckpointId,
                    e);
            abortInternal(barrier.getId(), e);
            return false;
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException {
        checkSubsumedCheckpoint(channelInfo, announcedBarrier);

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

        controller.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);
    }

    private void checkSubsumedCheckpoint(InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException {
        long barrierId = barrier.getId();
        if (currentCheckpointId < barrierId) {
            if (isCheckpointPending()) {
                cancelSubsumedCheckpoint(barrierId);
            }
            currentCheckpointId = barrierId;
            numBarriersReceived = 0;
            controller.preProcessFirstBarrierOrAnnouncement(barrier);
        }
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
        // by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
        // at zero means that no checkpoint barrier can start a new alignment
        currentCheckpointId = Math.max(cancelledId, currentCheckpointId);
        lastCancelledOrCompletedCheckpointId =
                Math.max(lastCancelledOrCompletedCheckpointId, cancelledId);
        numBarriersReceived = 0;
        controller.abortPendingCheckpoint(cancelledId, exception);
        notifyAbort(cancelledId, exception);
        allBarriersReceivedFuture.completeExceptionally(exception);
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
}
