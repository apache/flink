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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.util.event.NotificationListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Reader for a spilled sub partition.
 *
 * <p>The partition availability listener is notified about available buffers
 * only when the spilling is done. Spilling is done async and if it is still
 * in progress, we wait with the notification until the spilling is done.
 *
 * <p>Reads of the spilled file are done in synchronously.
 */
class SpilledSubpartitionView implements ResultSubpartitionView, NotificationListener {

	private static final Logger LOG = LoggerFactory.getLogger(SpilledSubpartitionView.class);

	/** The subpartition this view belongs to. */
	private final SpillableSubpartition parent;

	/** Writer for spills. */
	private final BufferFileWriter spillWriter;

	/** The synchronous file reader to do the actual I/O. */
	@GuardedBy("this")
	private final BufferFileReader fileReader;

	/** The buffer pool to read data into. */
	private final SpillReadBufferPool bufferPool;

	/** Buffer availability listener. */
	private final BufferAvailabilityListener availabilityListener;

	/** The total number of spilled buffers. */
	private final long numberOfSpilledBuffers;

	/** Flag indicating whether all resources have been released. */
	private AtomicBoolean isReleased = new AtomicBoolean();

	/** The next buffer to hand out. */
	@GuardedBy("this")
	private Buffer nextBuffer;

	/** Flag indicating whether a spill is still in progress. */
	private volatile boolean isSpillInProgress = true;

	SpilledSubpartitionView(
		SpillableSubpartition parent,
		int memorySegmentSize,
		BufferFileWriter spillWriter,
		long numberOfSpilledBuffers,
		BufferAvailabilityListener availabilityListener) throws IOException {

		this.parent = checkNotNull(parent);
		this.bufferPool = new SpillReadBufferPool(2, memorySegmentSize);
		this.spillWriter = checkNotNull(spillWriter);
		this.fileReader = new SynchronousBufferFileReader(spillWriter.getChannelID(), false);
		checkArgument(numberOfSpilledBuffers >= 0);
		this.numberOfSpilledBuffers = numberOfSpilledBuffers;
		this.availabilityListener = checkNotNull(availabilityListener);

		// Check whether async spilling is still in progress. If not, this returns
		// false and we can notify our availability listener about all available buffers.
		// Otherwise, we notify only when the spill writer callback happens.
		if (!spillWriter.registerAllRequestsProcessedListener(this)) {
			isSpillInProgress = false;
			availabilityListener.notifyDataAvailable();
			LOG.debug("No spilling in progress. Notified about {} available buffers.", numberOfSpilledBuffers);
		} else {
			LOG.debug("Spilling in progress. Waiting with notification about {} available buffers.", numberOfSpilledBuffers);
		}
	}

	/**
	 * This is the call back method for the spill writer. If a spill is still
	 * in progress when this view is created we wait until this method is called
	 * before we notify the availability listener.
	 */
	@Override
	public void onNotification() {
		isSpillInProgress = false;
		availabilityListener.notifyDataAvailable();
		LOG.debug("Finished spilling. Notified about {} available buffers.", numberOfSpilledBuffers);
	}

	@Nullable
	@Override
	public BufferAndBacklog getNextBuffer() throws IOException, InterruptedException {
		if (isSpillInProgress) {
			return null;
		}

		Buffer current;
		boolean nextBufferIsEvent;
		synchronized (this) {
			if (nextBuffer == null) {
				current = requestAndFillBuffer();
			} else {
				current = nextBuffer;
			}
			nextBuffer = requestAndFillBuffer();
			nextBufferIsEvent = nextBuffer != null && !nextBuffer.isBuffer();
		}

		if (current == null) {
			return null;
		}

		int newBacklog = parent.decreaseBuffersInBacklog(current);
		return new BufferAndBacklog(current, newBacklog > 0 || nextBufferIsEvent, newBacklog, nextBufferIsEvent);
	}

	@Nullable
	private Buffer requestAndFillBuffer() throws IOException, InterruptedException {
		assert Thread.holdsLock(this);

		if (fileReader.hasReachedEndOfFile()) {
			return null;
		}
		// TODO This is fragile as we implicitly expect that multiple calls to
		// this method don't happen before recycling buffers returned earlier.
		Buffer buffer = bufferPool.requestBufferBlocking();
		fileReader.readInto(buffer);
		return buffer;
	}

	@Override
	public void notifyDataAvailable() {
		// We do the availability listener notification either directly on
		// construction of this view (when everything has been spilled) or
		// as soon as spilling is done and we are notified about it in the
		// #onNotification callback.
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		parent.onConsumedSubpartition();
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			// TODO This can block until all buffers are written out to
			// disk if a spill is in-progress before deleting the file.
			// It is possibly called from the Netty event loop threads,
			// which can bring down the network.
			spillWriter.closeAndDelete();

			synchronized (this) {
				fileReader.close();
				if (nextBuffer != null) {
					nextBuffer.recycleBuffer();
					nextBuffer = null;
				}
			}

			bufferPool.destroy();
		}
	}

	@Override
	public boolean isReleased() {
		return parent.isReleased() || isReleased.get();
	}

	@Override
	public boolean nextBufferIsEvent() {
		synchronized (this) {
			if (nextBuffer == null) {
				try {
					nextBuffer = requestAndFillBuffer();
				} catch (Exception e) {
					// we can ignore this here (we will get it again once getNextBuffer() is called)
					return false;
				}
			}
			return nextBuffer != null && !nextBuffer.isBuffer();
		}
	}

	@Override
	public synchronized boolean isAvailable() {
		if (nextBuffer != null) {
			return true;
		}
		return !fileReader.hasReachedEndOfFile();
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	@Override
	public String toString() {
		return String.format("SpilledSubpartitionView(index: %d, buffers: %d) of ResultPartition %s",
			parent.index,
			numberOfSpilledBuffers,
			parent.parent.getPartitionId());
	}

	/**
	 * A buffer pool to provide buffer to read the file into.
	 *
	 * <p>This pool ensures that a consuming input gate makes progress in all cases, even when all
	 * buffers of the input gate buffer pool have been requested by remote input channels.
	 */
	private static class SpillReadBufferPool implements BufferRecycler {

		private final Queue<Buffer> buffers;

		private boolean isDestroyed;

		SpillReadBufferPool(int numberOfBuffers, int memorySegmentSize) {
			this.buffers = new ArrayDeque<>(numberOfBuffers);

			synchronized (buffers) {
				for (int i = 0; i < numberOfBuffers; i++) {
					buffers.add(new NetworkBuffer(MemorySegmentFactory.allocateUnpooledOffHeapMemory(
						memorySegmentSize, null), this));
				}
			}
		}

		@Override
		public void recycle(MemorySegment memorySegment) {
			synchronized (buffers) {
				if (isDestroyed) {
					memorySegment.free();
				} else {
					buffers.add(new NetworkBuffer(memorySegment, this));
					buffers.notifyAll();
				}
			}
		}

		private Buffer requestBufferBlocking() throws InterruptedException {
			synchronized (buffers) {
				while (true) {
					if (isDestroyed) {
						return null;
					}

					Buffer buffer = buffers.poll();

					if (buffer != null) {
						return buffer;
					}
					// Else: wait for a buffer
					buffers.wait();
				}
			}
		}

		private void destroy() {
			synchronized (buffers) {
				isDestroyed = true;
				buffers.notifyAll();
			}
		}
	}
}
