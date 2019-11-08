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
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A default abstract based class for {@link BufferStorage} implementations.
 */
@Internal
public abstract class AbstractBufferStorage implements BufferStorage {

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractBufferStorage.class);

	/**
	 * The pending blocked buffer/event sequences. Must be consumed before requesting further data
	 * from the input gate.
	 */
	protected final ArrayDeque<BufferOrEventSequence> queuedBuffered = new ArrayDeque<>();

	protected final long maxBufferedBytes;

	protected final String taskName;

	/**
	 * The sequence of buffers/events that has been unblocked and must now be consumed before
	 * requesting further data from the input gate.
	 */
	protected BufferOrEventSequence currentBuffered;

	/** The number of bytes in the queued spilled sequences. */
	protected long rolledBytes;

	protected AbstractBufferStorage(long maxBufferedBytes, String taskName) {
		checkArgument(maxBufferedBytes == -1 || maxBufferedBytes > 0);

		this.maxBufferedBytes = maxBufferedBytes;
		this.taskName = taskName;
	}

	@Override
	public boolean isFull() {
		return maxBufferedBytes > 0 && (getRolledBytes() + getPendingBytes()) > maxBufferedBytes;
	}

	@Override
	public void rollOver() throws IOException {
		if (currentBuffered == null) {
			// common case: no more buffered data
			currentBuffered = rollOverReusingResources();
			if (currentBuffered != null) {
				currentBuffered.open();
			}
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			LOG.debug("{}: Checkpoint skipped via buffered data:" +
				"Pushing back current alignment buffers and feeding back new alignment data first.", taskName);

			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			BufferOrEventSequence bufferedNow = rollOverWithoutReusingResources();
			if (bufferedNow != null) {
				bufferedNow.open();
				queuedBuffered.addFirst(currentBuffered);
				rolledBytes += currentBuffered.size();
				currentBuffered = bufferedNow;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Size of buffered data: {} bytes",
				taskName,
				currentBuffered == null ? 0L : currentBuffered.size());
		}
	}

	/**
	 * Starts a new sequence of buffers and event without reusing the same resources and
	 * returns the current sequence of buffers for reading.
	 *
	 * @return The readable sequence of buffers and events, or 'null', if nothing was added.
	 */
	protected abstract BufferOrEventSequence rollOverWithoutReusingResources() throws IOException;

	/**
	 * Starts a new sequence of buffers and event reusing the same resources and
	 * returns the current sequence of buffers for reading.
	 *
	 * @return The readable sequence of buffers and events, or 'null', if nothing was added.
	 */
	protected abstract BufferOrEventSequence rollOverReusingResources() throws IOException;

	@Override
	public void close() throws IOException {
		if (currentBuffered != null) {
			currentBuffered.cleanup();
		}
		for (BufferOrEventSequence seq : queuedBuffered) {
			seq.cleanup();
		}
		queuedBuffered.clear();
		rolledBytes = 0L;
	}

	@Override
	public long getRolledBytes() {
		return rolledBytes;
	}

	@Override
	public boolean isEmpty() {
		return currentBuffered == null;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException {
		if (currentBuffered == null) {
			return Optional.empty();
		}
		// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
		Optional<BufferOrEvent> next = Optional.ofNullable(currentBuffered.getNext());
		if (!next.isPresent()) {
			completeBufferedSequence();
		}
		return next;
	}

	protected void completeBufferedSequence() throws IOException {
		LOG.debug("{}: Finished feeding back buffered data.", taskName);

		currentBuffered.cleanup();
		currentBuffered = queuedBuffered.pollFirst();
		if (currentBuffered != null) {
			currentBuffered.open();
			rolledBytes -= currentBuffered.size();
		}
	}

	@Override
	public long getMaxBufferedBytes() {
		return maxBufferedBytes;
	}
}
