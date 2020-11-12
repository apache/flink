/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper class for persisting channel state via {@link ChannelStateWriter}.
 */
@NotThreadSafe
final class ChannelStatePersister {
	protected final InputChannelInfo channelInfo;

	private enum CheckpointStatus {COMPLETED, BARRIER_PENDING, BARRIER_RECEIVED}

	private CheckpointStatus checkpointStatus = CheckpointStatus.COMPLETED;

	private long lastSeenBarrier = -1L;

	/**
	 * Writer must be initialized before usage. {@link #startPersisting(long, List)} enforces this invariant.
	 */
	@Nullable
	private final ChannelStateWriter channelStateWriter;

	ChannelStatePersister(@Nullable ChannelStateWriter channelStateWriter, InputChannelInfo channelInfo) {
		this.channelStateWriter = channelStateWriter;
		this.channelInfo = channelInfo;
	}

	protected void startPersisting(long barrierId, List<Buffer> knownBuffers) {
		checkState(isInitialized(), "Channel state writer not injected");

		if (checkpointStatus != CheckpointStatus.BARRIER_RECEIVED && lastSeenBarrier < barrierId) {
			checkpointStatus = CheckpointStatus.BARRIER_PENDING;
			lastSeenBarrier = barrierId;
		}
		if (knownBuffers.size() > 0) {
			channelStateWriter.addInputData(
				barrierId,
				channelInfo,
				ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
				CloseableIterator.fromList(knownBuffers, Buffer::recycleBuffer));
		}
	}

	protected boolean isInitialized() {
		return channelStateWriter != null;
	}

	protected void stopPersisting(long id) {
		if (id >= lastSeenBarrier) {
			checkpointStatus = CheckpointStatus.COMPLETED;
			lastSeenBarrier = id;
		}
	}

	protected void maybePersist(Buffer buffer) {
		if (checkpointStatus == CheckpointStatus.BARRIER_PENDING && buffer.isBuffer()) {
			channelStateWriter.addInputData(
				lastSeenBarrier,
				channelInfo,
				ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
				CloseableIterator.ofElement(buffer.retainBuffer(), Buffer::recycleBuffer));
		}
	}

	protected boolean checkForBarrier(Buffer buffer) throws IOException {
		final AbstractEvent priorityEvent = parsePriorityEvent(buffer);
		if (priorityEvent instanceof CheckpointBarrier) {
			if (((CheckpointBarrier) priorityEvent).getId() >= lastSeenBarrier) {
				checkpointStatus = CheckpointStatus.BARRIER_RECEIVED;
				lastSeenBarrier = ((CheckpointBarrier) priorityEvent).getId();
				return true;
			}
		}
		return false;
	}

	/**
	 * Parses the buffer as an event and returns the {@link CheckpointBarrier} if the event is indeed a barrier or
	 * returns null in all other cases.
	 */
	@Nullable
	protected AbstractEvent parsePriorityEvent(Buffer buffer) throws IOException {
		if (buffer.isBuffer() || !buffer.getDataType().hasPriority()) {
			return null;
		}

		AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
		// reset the buffer because it would be deserialized again in SingleInputGate while getting next buffer.
		// we can further improve to avoid double deserialization in the future.
		buffer.setReaderIndex(0);
		return event;
	}

	protected boolean hasBarrierReceived() {
		return checkpointStatus == CheckpointStatus.BARRIER_RECEIVED;
	}
}
