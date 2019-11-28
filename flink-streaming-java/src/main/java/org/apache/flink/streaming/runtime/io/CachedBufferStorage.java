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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link CachedBufferStorage} takes the buffers and events from a data stream and adds them to
 * a memory queue. After a number of elements have been cached, the {@link CachedBufferStorage}
 * can "roll over":
 * It presents the cached elements as a readable sequence, and creates a new memory queue.
 *
 * <p>This {@link CachedBufferStorage} can be used in credit-based flow control for better barrier
 * alignment in exactly-once mode.
 */
@Internal
public class CachedBufferStorage implements BufferStorage {
	private static final Logger LOG = LoggerFactory.getLogger(CachedBufferStorage.class);

	/**
	 * The pending blocked buffer/event sequences. Must be consumed before requesting further data
	 * from the input gate.
	 */
	private final ArrayDeque<BufferOrEventSequence> queuedBuffered = new ArrayDeque<>();

	private final long maxBufferedBytes;

	private final String taskName;

	/** The page size, to estimate the total cached data size. */
	private final int pageSize;

	/**
	 * The sequence of buffers/events that has been unblocked and must now be consumed before
	 * requesting further data from the input gate.
	 */
	private BufferOrEventSequence currentBuffered;

	/** The number of bytes in the queued cached sequences. */
	private long rolledBytes;

	/** The number of bytes cached since the last roll over. */
	private long bytesBlocked;

	/** The current memory queue for caching the buffers or events. */
	private ArrayDeque<BufferOrEvent> currentBuffers;

	/**
	 * Create a new {@link CachedBufferStorage} with unlimited storage.
	 *
	 * @param pageSize The page size used to estimate the cached size.
	 */
	public CachedBufferStorage(int pageSize) {
		this(pageSize, -1, "Unknown");
	}

	/**
	 * Creates a new {@link CachedBufferStorage}, caching the buffers or events in memory queue.
	 *
	 * @param pageSize The page size used to estimate the cached size.
	 */
	public CachedBufferStorage(int pageSize, long maxBufferedBytes, String taskName) {
		checkArgument(maxBufferedBytes == -1 || maxBufferedBytes > 0);

		this.maxBufferedBytes = maxBufferedBytes;
		this.taskName = taskName;
		this.pageSize = pageSize;
		this.currentBuffers = new ArrayDeque<>();
	}

	@Override
	public void add(BufferOrEvent boe) {
		bytesBlocked += pageSize;

		currentBuffers.add(boe);
	}

	@Override
	public void close() {
		BufferOrEvent boe;
		while ((boe = currentBuffers.poll()) != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
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
	public long getPendingBytes() {
		return bytesBlocked;
	}

	@Override
	public boolean isFull() {
		return maxBufferedBytes > 0 && (getRolledBytes() + getPendingBytes()) > maxBufferedBytes;
	}

	@Override
	public void rollOver() {
		if (currentBuffered == null) {
			// common case: no more buffered data
			currentBuffered = rollOverCurrentBuffers();
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			LOG.debug("{}: Checkpoint skipped via buffered data:" +
				"Pushing back current alignment buffers and feeding back new alignment data first.", taskName);

			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			BufferOrEventSequence bufferedNow = rollOverCurrentBuffers();
			if (bufferedNow != null) {
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

	private BufferOrEventSequence rollOverCurrentBuffers() {
		if (bytesBlocked == 0) {
			return null;
		}

		BufferOrEventSequence currentSequence = new BufferOrEventSequence(currentBuffers, bytesBlocked);
		currentBuffers = new ArrayDeque<>();
		bytesBlocked = 0L;

		return currentSequence;
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
	public Optional<BufferOrEvent> pollNext() {
		if (currentBuffered == null) {
			return Optional.empty();
		}
		Optional<BufferOrEvent> next = Optional.ofNullable(currentBuffered.getNext());
		if (!next.isPresent()) {
			completeBufferedSequence();
		}
		return next;
	}

	private void completeBufferedSequence() {
		LOG.debug("{}: Finished feeding back buffered data.", taskName);

		currentBuffered.cleanup();
		currentBuffered = queuedBuffered.pollFirst();
		if (currentBuffered != null) {
			rolledBytes -= currentBuffered.size();
		}
	}

	@Override
	public long getMaxBufferedBytes() {
		return maxBufferedBytes;
	}

	// ------------------------------------------------------------------------

	/**
	 * This class represents a sequence of cached buffers and events, created by the
	 * {@link CachedBufferStorage}.
	 */
	private static class BufferOrEventSequence {

		/** The sequence of buffers and events to be consumed. */
		private final ArrayDeque<BufferOrEvent> queuedBuffers;

		/** The total size of the cached data. */
		private final long size;

		/**
		 * Creates a reader that reads a sequence of buffers and events.
		 *
		 * @param size The total size of cached data.
		 */
		BufferOrEventSequence(ArrayDeque<BufferOrEvent> buffers, long size) {
			this.queuedBuffers = buffers;
			this.size = size;
		}

		/**
		 * Gets the next BufferOrEvent from the sequence, or {@code null}, if the
		 * sequence is exhausted.
		 *
		 * @return The next BufferOrEvent from the buffered sequence, or {@code null} (end of sequence).
		 */
		@Nullable
		public BufferOrEvent getNext() {
			return queuedBuffers.poll();
		}

		/**
		 * Cleans up all the resources held by the sequence.
		 */
		public void cleanup() {
			BufferOrEvent boe;
			while ((boe = queuedBuffers.poll()) != null) {
				if (boe.isBuffer()) {
					boe.getBuffer().recycleBuffer();
				}
			}
		}

		/**
		 * Gets the size of the sequence.
		 */
		public long size() {
			return size;
		}
	}
}
