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

	private final long maxBufferedBytes;

	private final String taskName;

	/**
	 * The pending blocked buffer/event sequences. Must be consumed before requesting further data
	 * from the input gate.
	 */
	private final ArrayDeque<BufferOrEventSequence> rolledOverBuffersQueue = new ArrayDeque<>();

	/**
	 * The sequence of buffers/events that has been unblocked and must now be consumed before
	 * requesting further data from the input gate. This is effectively a "zero" element of
	 * {@link #rolledOverBuffersQueue}.
	 *
	 * <p>TODO: probably this field could be removed in favour of using
	 * {@code rolledOverBuffersQueue.peek()}.
	 */
	private BufferOrEventSequence rolledOverBuffers;

	/** The page size, to estimate the total cached data size. */
	private final int pageSize;

	/** The number of bytes in the queued cached sequences. */
	private long rolledBytes;

	/** The number of bytes cached since the last roll over. */
	private long bytesBlocked;

	/** The current memory queue for caching the buffers or events. */
	private ArrayDeque<BufferOrEvent> cachedBuffers;

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
		this.cachedBuffers = new ArrayDeque<>();
	}

	@Override
	public void add(BufferOrEvent boe) {
		bytesBlocked += pageSize;

		cachedBuffers.add(boe);
	}

	@Override
	public void close() {
		BufferOrEvent boe;
		while ((boe = cachedBuffers.poll()) != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
		if (rolledOverBuffers != null) {
			rolledOverBuffers.cleanup();
		}
		for (BufferOrEventSequence seq : rolledOverBuffersQueue) {
			seq.cleanup();
		}
		rolledOverBuffersQueue.clear();
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
		if (rolledOverBuffers == null) {
			// common case: no more buffered data
			rolledOverBuffers = rollOverCachedBuffers();
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			LOG.debug("{}: Checkpoint skipped via buffered data:" +
				"Pushing back current alignment buffers and feeding back new alignment data first.", taskName);

			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			BufferOrEventSequence bufferedNow = rollOverCachedBuffers();
			if (bufferedNow != null) {
				rolledOverBuffersQueue.addFirst(rolledOverBuffers);
				rolledBytes += rolledOverBuffers.size();
				rolledOverBuffers = bufferedNow;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Size of buffered data: {} bytes",
				taskName,
				rolledOverBuffers == null ? 0L : rolledOverBuffers.size());
		}
	}

	private BufferOrEventSequence rollOverCachedBuffers() {
		if (bytesBlocked == 0) {
			return null;
		}

		BufferOrEventSequence currentSequence = new BufferOrEventSequence(cachedBuffers, bytesBlocked);
		cachedBuffers = new ArrayDeque<>();
		bytesBlocked = 0L;

		return currentSequence;
	}

	@Override
	public long getRolledBytes() {
		return rolledBytes;
	}

	@Override
	public boolean isEmpty() {
		return rolledOverBuffers == null;
	}

	@Override
	public Optional<BufferOrEvent> pollNext() {
		if (rolledOverBuffers == null) {
			return Optional.empty();
		}
		Optional<BufferOrEvent> next = Optional.ofNullable(rolledOverBuffers.getNext());
		if (!next.isPresent()) {
			completeBufferedSequence();
		}
		return next;
	}

	private void completeBufferedSequence() {
		LOG.debug("{}: Finished feeding back buffered data.", taskName);

		rolledOverBuffers.cleanup();
		rolledOverBuffers = rolledOverBuffersQueue.pollFirst();
		if (rolledOverBuffers != null) {
			rolledBytes -= rolledOverBuffers.size();
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
