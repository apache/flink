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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public abstract class CheckpointBarrierHandler {

	/** The listener to be notified on complete checkpoints. */
	private final AbstractInvokable toNotifyOnCheckpoint;

	private long latestCheckpointStartDelayNanos;

	public CheckpointBarrierHandler(AbstractInvokable toNotifyOnCheckpoint) {
		this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
	}

	public abstract void releaseBlocksAndResetBarriers();

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	public abstract boolean isBlocked(int channelIndex);

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception;

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception;

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processEndOfPartition() throws Exception;

	public abstract long getLatestCheckpointId();

	public abstract long getAlignmentDurationNanos();

	public long getCheckpointStartDelayNanos() {
		return latestCheckpointStartDelayNanos;
	}

	public abstract void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception;

	public Optional<BufferReceivedListener> getBufferReceivedListener() {
		return Optional.empty();
	}

	protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier, long bufferedBytes, long alignmentDurationNanos) throws IOException {
		CheckpointMetaData checkpointMetaData =
			new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

		CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
			.setBytesBufferedInAlignment(bufferedBytes)
			.setAlignmentDurationNanos(alignmentDurationNanos)
			.setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);

		toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
			checkpointMetaData,
			checkpointBarrier.getCheckpointOptions(),
			checkpointMetrics);
	}

	protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
		toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
	}

	protected void markCheckpointStart(long checkpointCreationTimestamp) {
		latestCheckpointStartDelayNanos = 1_000_000 * Math.max(
			0,
			System.currentTimeMillis() - checkpointCreationTimestamp);
	}
}
