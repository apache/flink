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

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class SpillableSubpartitionView implements ResultSubpartitionView {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableSubpartitionView.class);

	/** The subpartition this view belongs to. */
	private final SpillableSubpartition parent;

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<BufferConsumer> buffers;

	/** IO manager if we need to spill (for spilled case). */
	private final IOManager ioManager;

	/** Size of memory segments (for spilled case). */
	private final int memorySegmentSize;

	/**
	 * The buffer availability listener. As long as in-memory, notifications
	 * happen on a buffer per buffer basis as spilling may happen after a
	 * notification has been sent out.
	 */
	private final BufferAvailabilityListener listener;

	private final AtomicBoolean isReleased = new AtomicBoolean(false);

	/** Remember the number of buffers this view was created with. */
	private final long numBuffers;

	/**
	 * The next buffer to hand out. Everytime this is set to a non-null value,
	 * a listener notification happens.
	 */
	private BufferConsumer nextBuffer;

	private volatile SpilledSubpartitionView spilledView;

	SpillableSubpartitionView(
		SpillableSubpartition parent,
		ArrayDeque<BufferConsumer> buffers,
		IOManager ioManager,
		int memorySegmentSize,
		BufferAvailabilityListener listener) {

		this.parent = checkNotNull(parent);
		this.buffers = checkNotNull(buffers);
		this.ioManager = checkNotNull(ioManager);
		this.memorySegmentSize = memorySegmentSize;
		this.listener = checkNotNull(listener);

		synchronized (buffers) {
			numBuffers = buffers.size();
			nextBuffer = buffers.poll();
		}

		if (nextBuffer != null) {
			listener.notifyDataAvailable();
		}
	}

	int releaseMemory() throws IOException {
		synchronized (buffers) {
			if (spilledView != null || nextBuffer == null) {
				// Already spilled or nothing in-memory
				return 0;
			} else {
				// We don't touch next buffer, because a notification has
				// already been sent for it. Only when it is consumed, will
				// it be recycled.

				// Create the spill writer and write all buffers to disk
				BufferFileWriter spillWriter = ioManager.createBufferFileWriter(ioManager.createChannel());

				long spilledBytes = 0;

				int numBuffers = buffers.size();
				for (int i = 0; i < numBuffers; i++) {
					try (BufferConsumer bufferConsumer = buffers.remove()) {
						Buffer buffer = bufferConsumer.build();
						checkState(bufferConsumer.isFinished(), "BufferConsumer must be finished before " +
							"spilling. Otherwise we would not be able to simply remove it from the queue. This should " +
							"be guaranteed by creating ResultSubpartitionView only once Subpartition isFinished.");
						parent.updateStatistics(buffer);
						spilledBytes += buffer.getSize();
						spillWriter.writeBlock(buffer);
					}
				}

				spilledView = new SpilledSubpartitionView(
					parent,
					memorySegmentSize,
					spillWriter,
					numBuffers,
					listener);

				LOG.debug("Spilling {} bytes for sub partition {} of {}.",
					spilledBytes,
					parent.index,
					parent.parent.getPartitionId());

				return numBuffers;
			}
		}
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException, InterruptedException {
		Buffer current = null;
		boolean nextBufferIsEvent = false;
		int newBacklog = 0; // this is always correct if current is non-null!
		boolean isMoreAvailable = false;

		synchronized (buffers) {
			if (isReleased.get()) {
				return null;
			} else if (nextBuffer != null) {
				current = nextBuffer.build();
				checkState(nextBuffer.isFinished(),
					"We can only read from SpillableSubpartition after it was finished");

				newBacklog = parent.decreaseBuffersInBacklogUnsafe(nextBuffer.isBuffer());
				nextBuffer.close();
				nextBuffer = buffers.poll();

				if (nextBuffer != null) {
					nextBufferIsEvent = !nextBuffer.isBuffer();
					isMoreAvailable = true;
				}

				parent.updateStatistics(current);
				// if we are spilled (but still process a non-spilled nextBuffer), we don't know the
				// state of nextBufferIsEvent or whether more buffers are available
				if (spilledView == null) {
					return new BufferAndBacklog(current, isMoreAvailable, newBacklog, nextBufferIsEvent);
				}
			}
		} // else: spilled

		SpilledSubpartitionView spilled = spilledView;
		if (spilled != null) {
			if (current != null) {
				return new BufferAndBacklog(current, spilled.isAvailable(), newBacklog, spilled.nextBufferIsEvent());
			} else {
				return spilled.getNextBuffer();
			}
		} else {
			throw new IllegalStateException("No in-memory buffers available, but also nothing spilled.");
		}
	}

	@Override
	public void notifyDataAvailable() {
		// We do the availability listener notification one by one
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			SpilledSubpartitionView spilled = spilledView;
			if (spilled != null) {
				spilled.releaseAllResources();
			}
			// we are never giving this buffer out in getNextBuffer(), so we need to clean it up
			synchronized (buffers) {
				if (nextBuffer != null) {
					nextBuffer.close();
					nextBuffer = null;
				}
			}
		}
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		SpilledSubpartitionView spilled = spilledView;
		if (spilled != null) {
			spilled.notifySubpartitionConsumed();
		} else {
			parent.onConsumedSubpartition();
		}
	}

	@Override
	public boolean isReleased() {
		SpilledSubpartitionView spilled = spilledView;
		if (spilled != null) {
			return spilled.isReleased();
		} else {
			return parent.isReleased() || isReleased.get();
		}
	}

	@Override
	public boolean nextBufferIsEvent() {
		synchronized (buffers) {
			if (isReleased.get()) {
				return false;
			} else if (nextBuffer != null) {
				return !nextBuffer.isBuffer();
			}
		} // else: spilled

		checkState(spilledView != null, "No in-memory buffers available, but also nothing spilled.");

		return spilledView.nextBufferIsEvent();
	}

	@Override
	public boolean isAvailable() {
		synchronized (buffers) {
			if (nextBuffer != null) {
				return true;
			}
			else if (spilledView == null) {
				return false;
			}
		} // else: spilled

		return spilledView.isAvailable();
	}

	@Override
	public Throwable getFailureCause() {
		SpilledSubpartitionView spilled = spilledView;
		if (spilled != null) {
			return spilled.getFailureCause();
		} else {
			return parent.getFailureCause();
		}
	}

	@Override
	public String toString() {
		boolean hasSpilled = spilledView != null;

		return String.format("SpillableSubpartitionView(index: %d, buffers: %d, spilled? %b) of ResultPartition %s",
			parent.index,
			numBuffers,
			hasSpilled,
			parent.parent.getPartitionId());
	}
}
