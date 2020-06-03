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
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;

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

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
import static org.apache.flink.util.CloseableIterator.ofElement;
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
			SubtaskCheckpointCoordinator checkpointCoordinator,
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

		threadSafeUnaligner = new ThreadSafeUnaligner(totalNumChannels,	checkNotNull(checkpointCoordinator), this);
	}

	/**
	 * We still need to trigger checkpoint via {@link ThreadSafeUnaligner#notifyBarrierReceived(CheckpointBarrier, InputChannelInfo)}
	 * while reading the first barrier from one channel, because this might happen
	 * earlier than the previous async trigger via mailbox by netty thread.
	 *
	 * <p>Note this is also suitable for the trigger case of local input channel.
	 */
	@Override
	public void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
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
		threadSafeUnaligner.notifyBarrierReceived(receivedBarrier, channelInfos[channelIndex]);
	}

	@Override
	public void abortPendingCheckpoint(long checkpointId, CheckpointException exception) throws IOException {
		threadSafeUnaligner.tryAbortPendingCheckpoint(checkpointId, exception);

		if (checkpointId > currentConsumedCheckpointId) {
			resetPendingCheckpoint(checkpointId);
		}
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long cancelledId = cancelBarrier.getCheckpointId();
		boolean shouldAbort = threadSafeUnaligner.setCancelledCheckpointId(cancelledId);
		if (shouldAbort) {
			notifyAbort(
				cancelledId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
		}

		if (cancelledId >= currentConsumedCheckpointId) {
			resetPendingCheckpoint(cancelledId);
			currentConsumedCheckpointId = cancelledId;
		}
	}

	@Override
	public void processEndOfPartition() throws Exception {
		threadSafeUnaligner.onChannelClosed();
		resetPendingCheckpoint(-1L);
	}

	private void resetPendingCheckpoint(long checkpointId) {
		if (isCheckpointPending()) {
			LOG.warn("{}: Received barrier or EndOfPartition(-1) {} before completing current checkpoint {}. " +
					"Skipping current checkpoint.",
				taskName,
				checkpointId,
				currentConsumedCheckpointId);

			Arrays.fill(hasInflightBuffers, false);
			numBarrierConsumed = 0;
		}
	}

	@Override
	public long getLatestCheckpointId() {
		return currentConsumedCheckpointId;
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
	public boolean hasInflightData(long checkpointId, InputChannelInfo channelInfo) {
		if (checkpointId < currentConsumedCheckpointId) {
			return false;
		}
		if (checkpointId > currentConsumedCheckpointId) {
			return true;
		}
		return hasInflightBuffers[getFlattenedChannelIndex(channelInfo)];
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

	@VisibleForTesting
	int getNumOpenChannels() {
		return threadSafeUnaligner.getNumOpenChannels();
	}

	@VisibleForTesting
	ThreadSafeUnaligner getThreadSafeUnaligner() {
		return threadSafeUnaligner;
	}

	private void notifyCheckpoint(CheckpointBarrier barrier) throws IOException {
		// ignore the previous triggered checkpoint by netty thread if it was already canceled or aborted before.
		if (barrier.getId() >= threadSafeUnaligner.getCurrentCheckpointId()) {
			super.notifyCheckpoint(barrier, 0);
		}
	}

	@ThreadSafe
	static class ThreadSafeUnaligner implements BufferReceivedListener, Closeable {

		/**
		 * Tag the state of which input channel has not received the barrier, such that newly arriving buffers need
		 * to be written in the unaligned checkpoint.
		 */
		private final boolean[] storeNewBuffers;

		/** The number of input channels which has received or processed the barrier. */
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

		private int numOpenChannels;

		private final SubtaskCheckpointCoordinator checkpointCoordinator;

		private final CheckpointBarrierUnaligner handler;

		ThreadSafeUnaligner(int totalNumChannels, SubtaskCheckpointCoordinator checkpointCoordinator, CheckpointBarrierUnaligner handler) {
			this.numOpenChannels = totalNumChannels;
			this.storeNewBuffers = new boolean[totalNumChannels];
			this.checkpointCoordinator = checkpointCoordinator;
			this.handler = handler;
		}

		@Override
		public synchronized void notifyBarrierReceived(CheckpointBarrier barrier, InputChannelInfo channelInfo) throws IOException {
			long barrierId = barrier.getId();

			if (currentReceivedCheckpointId < barrierId) {
				handleNewCheckpoint(barrier);
				handler.executeInTaskThread(() -> handler.notifyCheckpoint(barrier), "notifyCheckpoint");
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
				checkpointCoordinator.getChannelStateWriter().addInputData(
					currentReceivedCheckpointId,
					channelInfo,
					ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
					ofElement(buffer, Buffer::recycleBuffer));
			} else {
				buffer.recycleBuffer();
			}
		}

		@Override
		public synchronized void close() throws IOException {
			allBarriersReceivedFuture.cancel(false);
		}

		private synchronized void handleNewCheckpoint(CheckpointBarrier barrier) throws IOException {
			long barrierId = barrier.getId();
			if (!allBarriersReceivedFuture.isDone()) {
				CheckpointException exception = new CheckpointException("Barrier id: " + barrierId, CHECKPOINT_DECLINED_SUBSUMED);
				if (isCheckpointPending()) {
					// we did not complete the current checkpoint, another started before
					LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
							"Skipping current checkpoint.",
						handler.taskName,
						barrierId,
						currentReceivedCheckpointId);

					// let the task know we are not completing this
					final long currentCheckpointId = currentReceivedCheckpointId;
					handler.executeInTaskThread(() -> handler.notifyAbort(currentCheckpointId, exception), "notifyAbort");
				}
				allBarriersReceivedFuture.completeExceptionally(exception);
			}

			currentReceivedCheckpointId = barrierId;
			Arrays.fill(storeNewBuffers, true);
			numBarriersReceived = 0;
			allBarriersReceivedFuture = new CompletableFuture<>();
			checkpointCoordinator.initCheckpoint(barrierId, barrier.getCheckpointOptions());
		}

		synchronized CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
			if (checkpointId < currentReceivedCheckpointId) {
				return FutureUtils.completedVoidFuture();
			}
			if (checkpointId > currentReceivedCheckpointId) {
				throw new IllegalStateException("Checkpoint " + checkpointId + " has not been started at all");
			}
			return allBarriersReceivedFuture;
		}

		synchronized void onChannelClosed() throws IOException {
			numOpenChannels--;

			if (resetPendingCheckpoint()) {
				handler.notifyAbort(
					currentReceivedCheckpointId,
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			}
		}

		synchronized boolean setCancelledCheckpointId(long cancelledId) {
			if (currentReceivedCheckpointId > cancelledId || (currentReceivedCheckpointId == cancelledId && numBarriersReceived == 0)) {
				return false;
			}

			resetPendingCheckpoint();
			currentReceivedCheckpointId = cancelledId;
			return true;
		}

		synchronized void tryAbortPendingCheckpoint(long checkpointId, CheckpointException exception) throws IOException {
			if (checkpointId > currentReceivedCheckpointId && resetPendingCheckpoint()) {
				handler.notifyAbort(currentReceivedCheckpointId, exception);
			}
		}

		private boolean resetPendingCheckpoint() {
			if (numBarriersReceived == 0) {
				return false;
			}

			Arrays.fill(storeNewBuffers, false);
			numBarriersReceived = 0;
			return true;
		}

		@VisibleForTesting
		synchronized int getNumOpenChannels() {
			return numOpenChannels;
		}

		@VisibleForTesting
		synchronized long getCurrentCheckpointId() {
			return currentReceivedCheckpointId;
		}

		@VisibleForTesting
		boolean isCheckpointPending() {
			return numBarriersReceived > 0;
		}
	}
}
