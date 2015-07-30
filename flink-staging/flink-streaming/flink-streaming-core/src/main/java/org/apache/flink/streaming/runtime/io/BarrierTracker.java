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

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.ArrayDeque;

/**
 * The BarrierTracker keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 * 
 * <p>Unlike the {@link BarrierBuffer}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.</p>
 * 
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.</p>
 */
public class BarrierTracker implements CheckpointBarrierHandler {

	/** The tracker tracks a maximum number of checkpoints, for which some, but not all
	 * barriers have yet arrived. */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;
	
	/** The input gate, to draw the buffers and events from */
	private final InputGate inputGate;
	
	/** The number of channels. Once that many barriers have been received for a checkpoint,
	 * the checkpoint is considered complete. */
	private final int totalNumberOfInputChannels;

	/** All checkpoints for which some (but not all) barriers have been received,
	 * and that are not yet known to be subsumed by newer checkpoints */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;
	
	/** The listener to be notified on complete checkpoints */
	private EventListener<CheckpointBarrier> checkpointHandler;
	
	/** The highest checkpoint ID encountered so far */
	private long latestPendingCheckpointID = -1;
	
	
	public BarrierTracker(InputGate inputGate) {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.pendingCheckpoints = new ArrayDeque<CheckpointBarrierCount>();
	}

	@Override
	public BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException {
		while (true) {
			BufferOrEvent next = inputGate.getNextBufferOrEvent();
			if (next == null) {
				return null;
			}
			else if (next.isBuffer() || next.getEvent().getClass() != CheckpointBarrier.class) {
				return next;
			}
			else {
				processBarrier((CheckpointBarrier) next.getEvent());
			}
		}
	}

	@Override
	public void registerCheckpointEventHandler(EventListener<CheckpointBarrier> checkpointHandler) {
		if (this.checkpointHandler == null) {
			this.checkpointHandler = checkpointHandler;
		}
		else {
			throw new IllegalStateException("BarrierTracker already has a registered checkpoint handler");
		}
	}

	@Override
	public void cleanup() {
		pendingCheckpoints.clear();
	}

	@Override
	public boolean isEmpty() {
		return pendingCheckpoints.isEmpty();
	}

	private void processBarrier(CheckpointBarrier receivedBarrier) {
		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			if (checkpointHandler != null) {
				checkpointHandler.onEvent(receivedBarrier);
			}
			return;
		}
		
		// general path for multiple input channels
		final long barrierId = receivedBarrier.getId();

		// find the checkpoint barrier in the queue of bending barriers
		CheckpointBarrierCount cbc = null;
		int pos = 0;
		
		for (CheckpointBarrierCount next : pendingCheckpoints) {
			if (next.checkpointId == barrierId) {
				cbc = next;
				break;
			}
			pos++;
		}
		
		if (cbc != null) {
			// add one to the count to that barrier and check for completion
			int numBarriersNew = cbc.incrementBarrierCount();
			if (numBarriersNew == totalNumberOfInputChannels) {
				// checkpoint can be triggered
				// first, remove this checkpoint and all all prior pending
				// checkpoints (which are now subsumed)
				for (int i = 0; i <= pos; i++) {
					pendingCheckpoints.pollFirst();
				}
				
				// notify the listener
				if (checkpointHandler != null) {
					checkpointHandler.onEvent(receivedBarrier);
				}
			}
		}
		else {
			// first barrier for that checkpoint ID
			// add it only if it is newer than the latest checkpoint.
			// if it is not newer than the latest checkpoint ID, then there cannot be a
			// successful checkpoint for that ID anyways
			if (barrierId > latestPendingCheckpointID) {
				latestPendingCheckpointID = barrierId;
				pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));
				
				// make sure we do not track too many checkpoints
				if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
					pendingCheckpoints.pollFirst();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	private static final class CheckpointBarrierCount {
		
		private final long checkpointId;
		
		private int barrierCount;
		
		private CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}
		
		@Override
		public int hashCode() {
			return (int) ((checkpointId >>> 32) ^ checkpointId) + 17 * barrierCount; 
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof  CheckpointBarrierCount) {
				CheckpointBarrierCount that = (CheckpointBarrierCount) obj;
				return this.checkpointId == that.checkpointId && this.barrierCount == that.barrierCount;
			}
			else {
				return false;
			}
		}

		@Override
		public String toString() {
			return String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
