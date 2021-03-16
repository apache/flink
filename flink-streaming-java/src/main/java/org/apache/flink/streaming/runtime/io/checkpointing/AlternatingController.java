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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** Controller that can alternate between aligned and unaligned checkpoints. */
@Internal
public class AlternatingController implements CheckpointBarrierBehaviourController {

    private static final Logger LOG = LoggerFactory.getLogger(AlternatingController.class);

    private final AlignedController alignedController;
    private final UnalignedController unalignedController;
    private final Clock clock;

    private CheckpointBarrierBehaviourController activeController;
    private long firstBarrierArrivalTime = Long.MAX_VALUE;
    private long lastSeenBarrier = -1L;
    private long lastCompletedBarrier = -1L;
    private Cancellable registeredTimer = null;

    public AlternatingController(
            AlignedController alignedController,
            UnalignedController unalignedController,
            Clock clock) {
        this.activeController = this.alignedController = alignedController;
        this.unalignedController = unalignedController;
        this.clock = clock;
    }

    @Override
    public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
        // clean up sequence numbers in both controllers
        alignedController.preProcessFirstBarrierOrAnnouncement(barrier);
        unalignedController.preProcessFirstBarrierOrAnnouncement(barrier);
        activeController = chooseController(barrier);
    }

    @Override
    public Optional<CheckpointBarrier> barrierAnnouncement(
            InputChannelInfo channelInfo,
            CheckpointBarrier announcedBarrier,
            int sequenceNumber,
            DelayedActionRegistration delayedActionRegistration)
            throws IOException, CheckpointException {
        boolean firstAnnouncementForId = false;
        if (lastSeenBarrier < announcedBarrier.getId()) {
            lastSeenBarrier = announcedBarrier.getId();
            firstBarrierArrivalTime = getArrivalTime(announcedBarrier);
            firstAnnouncementForId = true;
        }

        Optional<CheckpointBarrier> maybeTimedOut = asTimedOut(announcedBarrier);
        announcedBarrier = maybeTimedOut.orElse(announcedBarrier);
        checkState(
                !activeController
                        .barrierAnnouncement(
                                channelInfo,
                                announcedBarrier,
                                sequenceNumber,
                                delayedActionRegistration)
                        .isPresent());

        if (maybeTimedOut.isPresent() && activeController == alignedController) {
            // Let's timeout this barrier
            return switchToUnaligned(announcedBarrier);
        } else if (firstAnnouncementForId
                && announcedBarrier.getCheckpointOptions().isTimeoutable()
                && activeController == alignedController) {
            scheduleSwitchToUnaligned(delayedActionRegistration, announcedBarrier);
        }

        return Optional.empty();
    }

    private void scheduleSwitchToUnaligned(
            DelayedActionRegistration delayedActionRegistration,
            CheckpointBarrier announcedBarrier) {
        if (registeredTimer != null) {
            registeredTimer.cancel();
        }
        registeredTimer =
                delayedActionRegistration.schedule(
                        () -> {
                            long barrierId = announcedBarrier.getId();
                            if (lastSeenBarrier == barrierId
                                    && lastCompletedBarrier < barrierId
                                    && activeController == alignedController) {
                                // Let's timeout this barrier
                                LOG.info(
                                        "Checkpoint alignment for barrier {} actively timed out."
                                                + "Switching over to unaligned checkpoints.",
                                        barrierId);
                                CheckpointBarrier unaligned = announcedBarrier.asUnaligned();
                                return switchToUnaligned(unaligned);
                            }
                            return Optional.empty();
                        },
                        Duration.ofMillis(
                                announcedBarrier.getCheckpointOptions().getAlignmentTimeout() + 1));
    }

    private long getArrivalTime(CheckpointBarrier announcedBarrier) {
        if (announcedBarrier.getCheckpointOptions().isTimeoutable()) {
            return clock.relativeTimeNanos();
        } else {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public Optional<CheckpointBarrier> barrierReceived(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException {
        if (barrier.getCheckpointOptions().isUnalignedCheckpoint()
                && activeController == alignedController) {
            Optional<CheckpointBarrier> maybeTrigger = switchToUnaligned(barrier);
            activeController.barrierReceived(channelInfo, barrier);
            return maybeTrigger;
        }

        Optional<CheckpointBarrier> maybeTimedOut = asTimedOut(barrier);
        barrier = maybeTimedOut.orElse(barrier);

        checkState(!activeController.barrierReceived(channelInfo, barrier).isPresent());

        if (maybeTimedOut.isPresent()) {
            if (activeController == alignedController) {
                LOG.info(
                        "Received a barrier for a checkpoint {} with timed out alignment. Switching"
                                + " over to unaligned checkpoints.",
                        barrier.getId());
                return switchToUnaligned(maybeTimedOut.get());
            } else {
                alignedController.resumeConsumption(channelInfo);
            }
        } else if (!barrier.getCheckpointOptions().isUnalignedCheckpoint()
                && activeController == unalignedController) {
            alignedController.resumeConsumption(channelInfo);
        }
        return Optional.empty();
    }

    @Override
    public Optional<CheckpointBarrier> preProcessFirstBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException {
        if (lastSeenBarrier < barrier.getId()) {
            lastSeenBarrier = barrier.getId();
            firstBarrierArrivalTime = getArrivalTime(barrier);
        }
        if (activeController == unalignedController) {
            barrier = barrier.asUnaligned();
        } else if (activeController == alignedController
                && barrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            checkState(!switchToUnaligned(barrier).isPresent());
        }
        return activeController.preProcessFirstBarrier(channelInfo, barrier);
    }

    private Optional<CheckpointBarrier> switchToUnaligned(CheckpointBarrier barrier)
            throws IOException, CheckpointException {
        checkState(alignedController == activeController);

        // timeout all not yet processed barriers for which alignedController has processed an
        // announcement
        Map<InputChannelInfo, Integer> announcedUnalignedBarriers =
                unalignedController.getSequenceNumberInAnnouncedChannels();
        for (Map.Entry<InputChannelInfo, Integer> entry :
                alignedController.getSequenceNumberInAnnouncedChannels().entrySet()) {
            InputChannelInfo unProcessedChannelInfo = entry.getKey();
            int announcedBarrierSequenceNumber = entry.getValue();
            if (announcedUnalignedBarriers.containsKey(unProcessedChannelInfo)) {
                checkState(
                        announcedUnalignedBarriers.get(unProcessedChannelInfo)
                                == announcedBarrierSequenceNumber);
            } else {
                unalignedController.barrierAnnouncement(
                        unProcessedChannelInfo,
                        barrier,
                        announcedBarrierSequenceNumber,
                        ILLEGAL_REGISTRATION);
            }
        }
        activeController = unalignedController;

        // get blocked channels before resuming consumption
        List<InputChannelInfo> blockedChannels = alignedController.getBlockedChannels();
        // alignedController might have already processed some barriers, so
        // "migrate"/forward those
        // calls to unalignedController.
        Optional<CheckpointBarrier> maybeTrigger = Optional.empty();
        for (int i = 0; i < blockedChannels.size(); i++) {
            InputChannelInfo blockedChannel = blockedChannels.get(i);
            if (i == 0) {
                maybeTrigger = unalignedController.preProcessFirstBarrier(blockedChannel, barrier);
            } else {
                unalignedController.barrierReceived(blockedChannel, barrier);
            }
        }

        alignedController.resumeConsumption();
        return maybeTrigger;
    }

    @Override
    public Optional<CheckpointBarrier> postProcessLastBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException {
        if (lastCompletedBarrier < barrier.getId()) {
            lastCompletedBarrier = barrier.getId();
        }
        return activeController.postProcessLastBarrier(channelInfo, barrier);
    }

    @Override
    public void abortPendingCheckpoint(long cancelledId, CheckpointException exception)
            throws IOException {
        activeController.abortPendingCheckpoint(cancelledId, exception);
        if (lastCompletedBarrier < cancelledId) {
            lastCompletedBarrier = cancelledId;
        }
    }

    @Override
    public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException {
        chooseController(barrier).obsoleteBarrierReceived(channelInfo, barrier);
    }

    private boolean isAligned(CheckpointBarrier barrier) {
        return barrier.getCheckpointOptions().needsAlignment();
    }

    private CheckpointBarrierBehaviourController chooseController(CheckpointBarrier barrier) {
        return isAligned(barrier) ? alignedController : unalignedController;
    }

    private Optional<CheckpointBarrier> asTimedOut(CheckpointBarrier barrier) {
        return Optional.of(barrier).filter(this::canTimeout).map(CheckpointBarrier::asUnaligned);
    }

    private boolean canTimeout(CheckpointBarrier barrier) {
        return barrier.getCheckpointOptions().isTimeoutable()
                && barrier.getId() <= lastSeenBarrier
                && barrier.getCheckpointOptions().getAlignmentTimeout() * 1_000_000
                        < (clock.relativeTimeNanos() - firstBarrierArrivalTime);
    }
}
