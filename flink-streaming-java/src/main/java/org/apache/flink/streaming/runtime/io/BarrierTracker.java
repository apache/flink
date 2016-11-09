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
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineOnCancellationBarrierException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;

import java.util.ArrayDeque;

/**
 * The BarrierTracker keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 * 
 * <p>Unlike the {@link BarrierBuffer}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.
 * 
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.
 */
@Internal
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
	private StatefulTask toNotifyOnCheckpoint;
	
	/** The highest checkpoint ID encountered so far */
	private long latestPendingCheckpointID = -1;

	// ------------------------------------------------------------------------
	
	public BarrierTracker(InputGate inputGate) {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.pendingCheckpoints = new ArrayDeque<CheckpointBarrierCount>();
	}

	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		while (true) {
			BufferOrEvent next = inputGate.getNextBufferOrEvent();
			if (next == null || next.isBuffer()) {
				// buffer or input exhausted
				return next;
			}
			else if (next.getEvent().getClass() == CheckpointBarrier.class) {
				processBarrier((CheckpointBarrier) next.getEvent());
			}
			else if (next.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCheckpointAbortBarrier((CancelCheckpointMarker) next.getEvent());
			}
			else {
				// some other event
				return next;
			}
		}
	}

	@Override
	public void registerCheckpointEventHandler(StatefulTask toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierTracker already has a registered checkpoint notifyee");
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

	@Override
	public long getAlignmentDurationNanos() {
		// this one does not do alignment at all
		return 0L;
	}

	private void processBarrier(CheckpointBarrier receivedBarrier) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			notifyCheckpoint(barrierId, receivedBarrier.getTimestamp());
			return;
		}

		// general path for multiple input channels

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
				// checkpoint can be triggered (or is aborted and all barriers have been seen)
				// first, remove this checkpoint and all all prior pending
				// checkpoints (which are now subsumed)
				for (int i = 0; i <= pos; i++) {
					pendingCheckpoints.pollFirst();
				}

				// notify the listener
				if (!cbc.isAborted()) {
					notifyCheckpoint(receivedBarrier.getId(), receivedBarrier.getTimestamp());
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

	private void processCheckpointAbortBarrier(CancelCheckpointMarker barrier) throws Exception {
		final long checkpointId = barrier.getCheckpointId();

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			notifyAbort(checkpointId);
			return;
		}

		// -- general path for multiple input channels --

		// find the checkpoint barrier in the queue of pending barriers
		// while doing this we "abort" all checkpoints before that one
		CheckpointBarrierCount cbc;
		while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
			pendingCheckpoints.removeFirst();
		}

		if (cbc != null && cbc.checkpointId() == checkpointId) {
			// make sure the checkpoint is remembered as aborted
			if (cbc.markAborted()) {
				// this was the first time the checkpoint was aborted - notify
				notifyAbort(checkpointId);
			}

			// we still count the barriers to be able to remove the entry once all barriers have been seen
			if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
				// we can remove this entry
				pendingCheckpoints.removeFirst();
			}
		}
		else {
			notifyAbort(checkpointId);

			// first barrier for this checkpoint - remember it as aborted
			// since we polled away all entries with lower checkpoint IDs
			// this entry will become the new first entry
			if (pendingCheckpoints.size() < MAX_CHECKPOINTS_TO_TRACK) {
				CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
				abortedMarker.markAborted();
				pendingCheckpoints.addFirst(abortedMarker);
			}
		}
	}

	private void notifyCheckpoint(long checkpointId, long timestamp) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);

			checkpointMetaData
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(checkpointMetaData);
		}
	}

	private void notifyAbort(long checkpointId) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(
					checkpointId, new CheckpointDeclineOnCancellationBarrierException());
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount;

		private boolean aborted;

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
