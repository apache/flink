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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Controller that can alternate between aligned and unaligned checkpoints.
 */
@Internal
public class AlternatingController implements CheckpointBarrierBehaviourController {
	private final AlignedController alignedController;
	private final UnalignedController unalignedController;
	private CheckpointBarrierBehaviourController activeController;

	public AlternatingController(
			AlignedController alignedController,
			UnalignedController unalignedController) {
		this.activeController = this.alignedController = alignedController;
		this.unalignedController = unalignedController;
	}

	@Override
	public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
		activeController = chooseController(barrier);
	}

	@Override
	public boolean barrierAnnouncement(
			InputChannelInfo channelInfo,
			CheckpointBarrier announcedBarrier,
			int sequenceNumber) throws IOException {

		activeController.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);

		if (unalignedController == activeController) {
			// already unaligned checkpoint
			return false;
		}

		checkState(activeController == alignedController);
		if (announcedBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
			return timeoutToUnalignedCheckpoint(announcedBarrier);
		}
		return false;
	}

	private boolean timeoutToUnalignedCheckpoint(CheckpointBarrier announcedBarrier) throws IOException {
		for (Entry<InputChannelInfo, Integer> entry : alignedController.getSequenceNumberInAnnouncedChannels().entrySet()) {
			InputChannelInfo unProcessedChannelInfo = entry.getKey();
			int announcedBarrierSequenceNumber = entry.getValue();
			unalignedController.barrierAnnouncement(unProcessedChannelInfo, announcedBarrier, announcedBarrierSequenceNumber);
		}

		// get blocked channels before resuming consumption
		Collection<InputChannelInfo> blockedChannels = alignedController.getBlockedChannels();
		alignedController.resumeConsumption();
		activeController = unalignedController;

		if (blockedChannels.isEmpty()) {
			// alignedController didn't process any barriers, this announcement is for the first barrier.
			// Checkpoint will be triggered on this.preProcessFirstBarrier call
			return false;
		} else {
			// alignedController has already processed some barriers, so "migrate"/forward those calls to unalignedController.
			// preProcessFirstBarrier has already been triggered, so we need to trigger the checkpoint here.
			unalignedController.preProcessFirstBarrier(blockedChannels.iterator().next(), announcedBarrier);
			for (InputChannelInfo blockedChannel : blockedChannels) {
				unalignedController.barrierReceived(blockedChannel, announcedBarrier);
			}
			return true;
		}
	}

	@Override
	public void barrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) {
		/**
		 * we can be in aligned mode and this can be the a time-outed barrier, but we do not do the
		 * {@link #timeoutToUnalignedCheckpoint}) here, as we will relay on the register timer to kick in.
		 */
		activeController.barrierReceived(channelInfo, barrier);
	}

	@Override
	public boolean preProcessFirstBarrier(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) throws IOException {
		checkActiveController(barrier);
		return activeController.preProcessFirstBarrier(channelInfo, barrier);
	}

	@Override
	public boolean postProcessLastBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
		checkActiveController(barrier);
		return activeController.postProcessLastBarrier(channelInfo, barrier);
	}

	@Override
	public void abortPendingCheckpoint(long cancelledId, CheckpointException exception) throws IOException {
		activeController.abortPendingCheckpoint(cancelledId, exception);
	}

	@Override
	public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
		chooseController(barrier).obsoleteBarrierReceived(channelInfo, barrier);
	}

	private void checkActiveController(CheckpointBarrier barrier) {
		if (isAligned(barrier)) {
			checkState(activeController == alignedController);
		}
		else {
			checkState(activeController == unalignedController);
		}
	}

	private boolean isAligned(CheckpointBarrier barrier) {
		return barrier.getCheckpointOptions().needsAlignment();
	}

	private CheckpointBarrierBehaviourController chooseController(CheckpointBarrier barrier) {
		return isAligned(barrier) ? alignedController : unalignedController;
	}
}
