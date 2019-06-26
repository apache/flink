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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;

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
public class CachedBufferStorage extends AbstractBufferStorage {

	/** The page size, to estimate the total cached data size. */
	private final int pageSize;

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
		super(maxBufferedBytes, taskName);
		this.pageSize = pageSize;
		this.currentBuffers = new ArrayDeque<>();
	}

	@Override
	public void add(BufferOrEvent boe) {
		bytesBlocked += pageSize;

		currentBuffers.add(boe);
	}

	/**
	 * It is never reusing resources and is defaulting to {@link #rollOverWithoutReusingResources()}.
	 */
	@Override
	public BufferOrEventSequence rollOverReusingResources() {
		return rollOverWithoutReusingResources();
	}

	@Override
	public BufferOrEventSequence rollOverWithoutReusingResources() {
		if (bytesBlocked == 0) {
			return null;
		}

		CachedBufferOrEventSequence currentSequence = new CachedBufferOrEventSequence(currentBuffers, bytesBlocked);
		currentBuffers = new ArrayDeque<>();
		bytesBlocked = 0L;

		return currentSequence;
	}

	@Override
	public void close() throws IOException {
		BufferOrEvent boe;
		while ((boe = currentBuffers.poll()) != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
		super.close();
	}

	@Override
	public long getPendingBytes() {
		return bytesBlocked;
	}

	// ------------------------------------------------------------------------

	/**
	 * This class represents a sequence of cached buffers and events, created by the
	 * {@link CachedBufferStorage}.
	 */
	public static class CachedBufferOrEventSequence implements BufferOrEventSequence {

		/** The sequence of buffers and events to be consumed. */
		private final ArrayDeque<BufferOrEvent> queuedBuffers;

		/** The total size of the cached data. */
		private final long size;

		/**
		 * Creates a reader that reads a sequence of buffers and events.
		 *
		 * @param size The total size of cached data.
		 */
		CachedBufferOrEventSequence(ArrayDeque<BufferOrEvent> buffers, long size) {
			this.queuedBuffers = buffers;
			this.size = size;
		}

		@Override
		public void open() {}

		@Override
		@Nullable
		public BufferOrEvent getNext() {
			return queuedBuffers.poll();
		}

		@Override
		public void cleanup() {
			BufferOrEvent boe;
			while ((boe = queuedBuffers.poll()) != null) {
				if (boe.isBuffer()) {
					boe.getBuffer().recycleBuffer();
				}
			}
		}

		@Override
		public long size() {
			return size;
		}
	}
}
