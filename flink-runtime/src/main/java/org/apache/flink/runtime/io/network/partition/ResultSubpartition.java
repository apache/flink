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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A single subpartition of a {@link ResultPartition} instance.
 */
public abstract class ResultSubpartition {

	/** The index of the subpartition at the parent partition. */
	protected final int index;

	/** The parent partition this subpartition belongs to. */
	protected final ResultPartition parent;

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	protected final ArrayDeque<BufferConsumer> buffers = new ArrayDeque<>();

	/** The number of non-event buffers currently in this subpartition. */
	@GuardedBy("buffers")
	private int buffersInBacklog;

	// - Statistics ----------------------------------------------------------

	/** The total number of buffers (both data and event buffers). */
	private long totalNumberOfBuffers;

	/** The total number of bytes (both data and event buffers). */
	private long totalNumberOfBytes;

	public ResultSubpartition(int index, ResultPartition parent) {
		this.index = index;
		this.parent = parent;
	}

	protected void updateStatistics(BufferConsumer buffer) {
		totalNumberOfBuffers++;
	}

	protected void updateStatistics(Buffer buffer) {
		totalNumberOfBytes += buffer.getSize();
	}

	protected long getTotalNumberOfBuffers() {
		return totalNumberOfBuffers;
	}

	protected long getTotalNumberOfBytes() {
		return totalNumberOfBytes;
	}

	/**
	 * Notifies the parent partition about a consumed {@link ResultSubpartitionView}.
	 */
	protected void onConsumedSubpartition() {
		parent.onConsumedSubpartition(index);
	}

	protected Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	/**
	 * Adds the given buffer.
	 *
	 * <p>The request may be executed synchronously, or asynchronously, depending on the
	 * implementation.
	 *
	 * <p><strong>IMPORTANT:</strong> Before adding new {@link BufferConsumer} previously added must be in finished
	 * state. Because of the performance reasons, this is only enforced during the data reading.
	 *
	 * @param bufferConsumer
	 * 		the buffer to add (transferring ownership to this writer)
	 * @return true if operation succeeded and bufferConsumer was enqueued for consumption.
	 * @throws IOException
	 * 		thrown in case of errors while adding the buffer
	 */
	public abstract boolean add(BufferConsumer bufferConsumer) throws IOException;

	public abstract void flush();

	public abstract void finish() throws IOException;

	public abstract void release() throws IOException;

	public abstract ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException;

	abstract int releaseMemory() throws IOException;

	public abstract boolean isReleased();

	/**
	 * Gets the number of non-event buffers in this subpartition.
	 *
	 * <p><strong>Beware:</strong> This method should only be used in tests in non-concurrent access
	 * scenarios since it does not make any concurrency guarantees.
	 */
	@VisibleForTesting
	public int getBuffersInBacklog() {
		return buffersInBacklog;
	}

	/**
	 * Makes a best effort to get the current size of the queue.
	 * This method must not acquire locks or interfere with the task and network threads in
	 * any way.
	 */
	public abstract int unsynchronizedGetNumberOfQueuedBuffers();

	/**
	 * Decreases the number of non-event buffers by one after fetching a non-event
	 * buffer from this subpartition (for access by the subpartition views).
	 *
	 * @return backlog after the operation
	 */
	public int decreaseBuffersInBacklog(Buffer buffer) {
		synchronized (buffers) {
			return decreaseBuffersInBacklogUnsafe(buffer != null && buffer.isBuffer());
		}
	}

	protected int decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
		assert Thread.holdsLock(buffers);
		if (isBuffer) {
			buffersInBacklog--;
		}
		return buffersInBacklog;
	}

	/**
	 * Increases the number of non-event buffers by one after adding a non-event
	 * buffer into this subpartition.
	 */
	protected void increaseBuffersInBacklog(BufferConsumer buffer) {
		assert Thread.holdsLock(buffers);

		if (buffer != null && buffer.isBuffer()) {
			buffersInBacklog++;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A combination of a {@link Buffer} and the backlog length indicating
	 * how many non-event buffers are available in the subpartition.
	 */
	public static final class BufferAndBacklog {

		private final Buffer buffer;
		private final boolean isMoreAvailable;
		private final int buffersInBacklog;
		private final boolean nextBufferIsEvent;

		public BufferAndBacklog(Buffer buffer, boolean isMoreAvailable, int buffersInBacklog, boolean nextBufferIsEvent) {
			this.buffer = checkNotNull(buffer);
			this.buffersInBacklog = buffersInBacklog;
			this.isMoreAvailable = isMoreAvailable;
			this.nextBufferIsEvent = nextBufferIsEvent;
		}

		public Buffer buffer() {
			return buffer;
		}

		public boolean isMoreAvailable() {
			return isMoreAvailable;
		}

		public int buffersInBacklog() {
			return buffersInBacklog;
		}

		public boolean nextBufferIsEvent() {
			return nextBufferIsEvent;
		}
	}

}
