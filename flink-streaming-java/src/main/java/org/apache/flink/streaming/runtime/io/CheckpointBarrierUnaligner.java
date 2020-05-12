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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointBarrierUnaligner} is used for triggering checkpoint while reading the first barrier
 * and keeping track of the number of received barriers and consumed barriers.
 */
@Internal
@NotThreadSafe
public class CheckpointBarrierUnaligner extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierUnaligner.class);

	private final String taskName;

	/**
	 * Tag the state of which input channel has pending in-flight buffers; that is, already received buffers that
	 * predate the checkpoint barrier of the current checkpoint.
	 */
	private final boolean[] hasInflightBuffers;

	private int numBarrierConsumed;

	/**
	 * Contains the offsets of the channel indices for each gate when flattening the channels of all gates.
	 *
	 * <p>For example, consider 3 gates with 4 channels, {@code gateChannelOffsets = [0, 4, 8]}.
	 */
	private final int[] gateChannelOffsets;

	private final InputChannelInfo[] channelInfos;

	/**
	 * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the same barrier from
	 * different channels.
	 *
	 * <p>Note: this checkpoint is valid in respect to <b>consumed</b> barriers in contrast to
	 * {@link ThreadSafeUnaligner#currentReceivedCheckpointId}.
	 */
	private long currentConsumedCheckpointId = -1L;

	/** Encapsulates state that is shared between netty threads and task thread. */
	private final ThreadSafeUnaligner threadSafeUnaligner;

	CheckpointBarrierUnaligner(
			int[] numberOfInputChannelsPerGate,
			ChannelStateWriter channelStateWriter,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);

		this.taskName = taskName;

		final int numGates = numberOfInputChannelsPerGate.length;

		gateChannelOffsets = new int[numGates];
		for (int index = 1; index < numGates; index++) {
			gateChannelOffsets[index] = gateChannelOffsets[index - 1] + numberOfInputChannelsPerGate[index - 1];
		}

		final int totalNumChannels = gateChannelOffsets[numGates - 1] + numberOfInputChannelsPerGate[numGates - 1];
		hasInflightBuffers = new boolean[totalNumChannels];

		channelInfos = IntStream.range(0, numGates)
			.mapToObj(gateIndex -> IntStream.range(0, numberOfInputChannelsPerGate[gateIndex])
				.mapToObj(channelIndex -> new InputChannelInfo(gateIndex, channelIndex)))
			.flatMap(Function.identity())
			.toArray(InputChannelInfo[]::new);

		threadSafeUnaligner = new ThreadSafeUnaligner(totalNumChannels,	checkNotNull(channelStateWriter), this);
	}

	@Override
	public void releaseBlocksAndResetBarriers() {
		if (isCheckpointPending()) {
			// make sure no additional data is persisted
			Arrays.fill(hasInflightBuffers, false);
			// the next barrier that comes must assume it is the first
			numBarrierConsumed = 0;
		}
		threadSafeUnaligner.resetReceivedBarriers(currentConsumedCheckpointId);
	}

	/**
	 * For unaligned checkpoint, it never blocks processing from the task aspect.
	 *
	 * <p>For PoC, we do not consider the possibility that the unaligned checkpoint would
	 * not perform due to the max configured unaligned checkpoint size.
	 */
	@Override
	public boolean isBlocked(int channelIndex) {
		return false;
	}

	/**
	 * We still need to trigger checkpoint while reading the first barrier from one channel, because this might happen
	 * earlier than the previous async trigger via mailbox by netty thread. And the {@link AbstractInvokable} has the
	 * deduplication logic to guarantee trigger checkpoint only once finally.
	 *
	 * <p>Note this is also suitable for the trigger case of local input channel.
	 */
	@Override
	public void processBarrier(
			CheckpointBarrier receivedBarrier,
			int channelIndex) throws Exception {
		long barrierId = receivedBarrier.getId();
		if (currentConsumedCheckpointId > barrierId || (currentConsumedCheckpointId == barrierId && !isCheckpointPending())) {
			// ignore old and cancelled barriers
			return;
		}
		if (currentConsumedCheckpointId < barrierId) {
			currentConsumedCheckpointId = barrierId;
			numBarrierConsumed = 0;
			Arrays.fill(hasInflightBuffers, true);
		}
		if (currentConsumedCheckpointId == barrierId) {
			hasInflightBuffers[channelIndex] = false;
			numBarrierConsumed++;
		}
		// processBarrier is called from task thread and can actually happen before notifyBarrierReceived on empty
		// buffer queues
		// to avoid replicating any logic, we simply call notifyBarrierReceived here as well
		threadSafeUnaligner.notifyBarrierReceived(receivedBarrier, channelInfos[channelIndex]);
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long barrierId = cancelBarrier.getCheckpointId();

		if (currentConsumedCheckpointId >= barrierId && !isCheckpointPending()) {
			return;
		}

		if (isCheckpointPending()) {
			LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
							"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentConsumedCheckpointId);
		} else if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Checkpoint {} canceled, aborting alignment.", taskName, barrierId);
		}
		releaseBlocksAndResetBarriers();
		currentConsumedCheckpointId = barrierId;
		threadSafeUnaligner.setCurrentReceivedCheckpointId(currentConsumedCheckpointId);
		notifyAbortOnCancellationBarrier(barrierId);
	}

	@Override
	public void processEndOfPartition() throws Exception {
		threadSafeUnaligner.onChannelClosed();

		if (isCheckpointPending()) {
			// let the task know we skip a checkpoint
			notifyAbort(
				currentConsumedCheckpointId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			// no chance to complete this checkpoint
			releaseBlocksAndResetBarriers();
		}
	}

	@Override
	public long getLatestCheckpointId() {
		return currentConsumedCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d", taskName, currentConsumedCheckpointId);
	}

	@Override
	public void close() throws IOException {
		super.close();
		threadSafeUnaligner.close();
	}

	@Override
	public boolean hasInflightData(long checkpointId, int channelIndex) {
		if (checkpointId < currentConsumedCheckpointId) {
			return false;
		}
		if (checkpointId > currentConsumedCheckpointId) {
			return true;
		}
		return hasInflightBuffers[channelIndex];
	}

	@Override
	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		return threadSafeUnaligner.getAllBarriersReceivedFuture(checkpointId);
	}

	@Override
	public Optional<BufferReceivedListener> getBufferReceivedListener() {
		return Optional.of(threadSafeUnaligner);
	}

	@Override
	protected boolean isCheckpointPending() {
		return numBarrierConsumed > 0;
	}

	private int getFlattenedChannelIndex(InputChannelInfo channelInfo) {
		return gateChannelOffsets[channelInfo.getGateIdx()] + channelInfo.getInputChannelIdx();
	}

	@ThreadSafe
	private static class ThreadSafeUnaligner implements BufferReceivedListener, Closeable {

		/**
		 * Tag the state of which input channel has not received the barrier, such that newly arriving buffers need
		 * to be written in the unaligned checkpoint.
		 */
		private final boolean[] storeNewBuffers;

		/**
		 * The number of input channels which has read the barrier by task.
		 */
		private int numBarriersReceived;

		/** A future indicating that all barriers of the a given checkpoint have been read. */
		private CompletableFuture<Void> allBarriersReceivedFuture = FutureUtils.completedVoidFuture();

		/**
		 * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the same barrier from
		 * different channels.
		 *
		 * <p>Note: this checkpoint is valid in respect to <b>received</b> barriers in contrast to
		 * {@link CheckpointBarrierUnaligner#currentConsumedCheckpointId}.
		 */
		private long currentReceivedCheckpointId = -1L;

		/** The number of opened channels. */
		private int numOpenChannels;

		private final ChannelStateWriter channelStateWriter;

		private final CheckpointBarrierUnaligner handler;

		public ThreadSafeUnaligner(
				int totalNumChannels,
				ChannelStateWriter channelStateWriter,
				CheckpointBarrierUnaligner handler) {
			storeNewBuffers = new boolean[totalNumChannels];
			this.channelStateWriter = channelStateWriter;
			this.handler = handler;
			numOpenChannels = totalNumChannels;
		}

		@Override
		public synchronized void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) throws IOException {
			long barrierId = barrier.getId();

			if (currentReceivedCheckpointId < barrierId) {
				handleNewCheckpoint(barrier);
				handler.executeInTaskThread(() -> handler.notifyCheckpoint(barrier, 0), "notifyCheckpoint");
			}

			int channelIndex = handler.getFlattenedChannelIndex(channelInfo);
			if (barrierId == currentReceivedCheckpointId && storeNewBuffers[channelIndex]) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}: Received barrier from channel {} @ {}.", handler.taskName, channelIndex, barrierId);
				}

				storeNewBuffers[channelIndex] = false;

				if (++numBarriersReceived == numOpenChannels) {
					allBarriersReceivedFuture.complete(null);
				}
			}
		}

		@Override
		public synchronized void notifyBufferReceived(Buffer buffer, InputChannelInfo channelInfo) {
			if (storeNewBuffers[handler.getFlattenedChannelIndex(channelInfo)]) {
				channelStateWriter.addInputData(
					currentReceivedCheckpointId,
					channelInfo,
					ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
					buffer);
			} else {
				buffer.recycleBuffer();
			}
		}

		@Override
		public synchronized void close() throws IOException {
			allBarriersReceivedFuture.cancel(false);
		}

		boolean isCheckpointPending() {
			return numBarriersReceived > 0;
		}

		private synchronized void handleNewCheckpoint(CheckpointBarrier barrier) throws IOException {
			long barrierId = barrier.getId();
			if (!allBarriersReceivedFuture.isDone() && isCheckpointPending()) {
				// we did not complete the current checkpoint, another started before
				LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					handler.taskName,
					barrierId,
					currentReceivedCheckpointId);

				// let the task know we are not completing this
				long currentCheckpointId = currentReceivedCheckpointId;
				handler.executeInTaskThread(() ->
					handler.notifyAbort(currentCheckpointId,
						new CheckpointException(
							"Barrier id: " + barrierId,
							CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED)),
					"notifyAbort");
			}

			currentReceivedCheckpointId = barrierId;
			Arrays.fill(storeNewBuffers, true);
			numBarriersReceived = 0;
			allBarriersReceivedFuture = new CompletableFuture<>();
			channelStateWriter.start(barrierId, barrier.getCheckpointOptions());
		}

		public synchronized void resetReceivedBarriers(long checkpointId) {
			if (checkpointId >= currentReceivedCheckpointId && numBarriersReceived > 0) {
				// avoid more data being serialized after abortion
				Arrays.fill(storeNewBuffers, false);
				// the next barrier that comes must assume it is the first
				numBarriersReceived = 0;
			}
		}

		public synchronized CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
			if (checkpointId < currentReceivedCheckpointId) {
				return FutureUtils.completedVoidFuture();
			}
			if (checkpointId > currentReceivedCheckpointId) {
				throw new IllegalStateException("Checkpoint " + checkpointId + " has not been started at all");
			}
			return allBarriersReceivedFuture;
		}

		public synchronized void onChannelClosed() {
			numOpenChannels--;
		}

		public synchronized void setCurrentReceivedCheckpointId(long currentReceivedCheckpointId) {
			this.currentReceivedCheckpointId = Math.max(currentReceivedCheckpointId, this.currentReceivedCheckpointId);
		}

		@VisibleForTesting
		public synchronized int getNumOpenChannels() {
			return numOpenChannels;
		}
	}

	@VisibleForTesting
	public int getNumOpenChannels() {
		return threadSafeUnaligner.getNumOpenChannels();
	}
}
