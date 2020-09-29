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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel reads recovered state from previous unaligned checkpoint snapshots
 * via {@link ChannelStateReader}.
 */
public abstract class RecoveredInputChannel extends InputChannel implements ChannelStateHolder {

	private static final Logger LOG = LoggerFactory.getLogger(RecoveredInputChannel.class);

	private final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();
	private final CompletableFuture<?> stateConsumedFuture = new CompletableFuture<>();
	protected final BufferManager bufferManager;

	@GuardedBy("receivedBuffers")
	private boolean isReleased;

	protected ChannelStateWriter channelStateWriter;

	/** The buffer number of recovered buffers. Starts at MIN_VALUE to have no collisions with actual buffer numbers. */
	private int sequenceNumber = Integer.MIN_VALUE;

	RecoveredInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			int initialBackoff,
			int maxBackoff,
			Counter numBytesIn,
			Counter numBuffersIn) {
		super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, numBytesIn, numBuffersIn);

		bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
	}

	@Override
	public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
		checkState(this.channelStateWriter == null, "Already initialized");
		this.channelStateWriter = checkNotNull(channelStateWriter);
	}

	public abstract InputChannel toInputChannel() throws IOException;

	CompletableFuture<?> getStateConsumedFuture() {
		return stateConsumedFuture;
	}

	protected void readRecoveredState(ChannelStateReader reader) throws IOException, InterruptedException {
		ReadResult result = ReadResult.HAS_MORE_DATA;
		while (result == ReadResult.HAS_MORE_DATA) {
			Buffer buffer = bufferManager.requestBufferBlocking();
			result = internalReaderRecoveredState(reader, buffer);
		}
		finishReadRecoveredState();
	}

	private ReadResult internalReaderRecoveredState(ChannelStateReader reader, Buffer buffer) throws IOException {
		ReadResult result;
		try {
			result = reader.readInputData(channelInfo, buffer);
		} catch (Throwable t) {
			buffer.recycleBuffer();
			throw t;
		}
		if (buffer.readableBytes() > 0) {
			onRecoveredStateBuffer(buffer);
		} else {
			buffer.recycleBuffer();
		}
		return result;
	}

	private void onRecoveredStateBuffer(Buffer buffer) {
		boolean recycleBuffer = true;
		try {
			final boolean wasEmpty;
			synchronized (receivedBuffers) {
				// Similar to notifyBufferAvailable(), make sure that we never add a buffer
				// after releaseAllResources() released all buffers from receivedBuffers.
				if (isReleased) {
					return;
				}

				wasEmpty = receivedBuffers.isEmpty();
				receivedBuffers.add(buffer);
				recycleBuffer = false;
			}

			if (wasEmpty) {
				notifyChannelNonEmpty();
			}
		} finally {
			if (recycleBuffer) {
				buffer.recycleBuffer();
			}
		}
	}

	private void finishReadRecoveredState() throws IOException {
		onRecoveredStateBuffer(EventSerializer.toBuffer(EndOfChannelStateEvent.INSTANCE, false));
		bufferManager.releaseFloatingBuffers();
		LOG.debug("{}/{} finished recovering input.", inputGate.getOwningTaskName(), channelInfo);
	}

	@Nullable
	private BufferAndAvailability getNextRecoveredStateBuffer() throws IOException {
		final Buffer next;
		final Buffer.DataType nextDataType;

		synchronized (receivedBuffers) {
			checkState(!isReleased, "Trying to read from released RecoveredInputChannel");
			next = receivedBuffers.poll();
			nextDataType = peekDataTypeUnsafe();
		}

		if (next == null) {
			return null;
		} else if (isEndOfChannelStateEvent(next)) {
			stateConsumedFuture.complete(null);
			return null;
		} else {
			return new BufferAndAvailability(next, nextDataType, 0, sequenceNumber++);
		}
	}

	private boolean isEndOfChannelStateEvent(Buffer buffer) throws IOException {
		if (buffer.isBuffer()) {
			return false;
		}

		AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
		buffer.setReaderIndex(0);
		return event.getClass() == EndOfChannelStateEvent.class;
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException {
		checkError();
		return Optional.ofNullable(getNextRecoveredStateBuffer());
	}

	private Buffer.DataType peekDataTypeUnsafe() {
		assert Thread.holdsLock(receivedBuffers);

		final Buffer first = receivedBuffers.peek();
		return first != null ? first.getDataType() : Buffer.DataType.NONE;
	}

	@Override
	public void resumeConsumption() {
		throw new UnsupportedOperationException("RecoveredInputChannel should never be blocked.");
	}

	@Override
	void requestSubpartition(int subpartitionIndex) {
		throw new UnsupportedOperationException("RecoveredInputChannel should never request partition.");
	}

	@Override
	void sendTaskEvent(TaskEvent event) {
		throw new UnsupportedOperationException("RecoveredInputChannel should never send any task events.");
	}

	@Override
	boolean isReleased() {
		synchronized (receivedBuffers) {
			return isReleased;
		}
	}

	void releaseAllResources() throws IOException {
		ArrayDeque<Buffer> releasedBuffers = new ArrayDeque<>();
		boolean shouldRelease = false;

		synchronized (receivedBuffers) {
			if (!isReleased) {
				isReleased = true;
				shouldRelease = true;
				releasedBuffers.addAll(receivedBuffers);
				receivedBuffers.clear();
			}
		}

		if (shouldRelease) {
			bufferManager.releaseAllBuffers(releasedBuffers);
		}
	}

	@VisibleForTesting
	protected int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}
}
