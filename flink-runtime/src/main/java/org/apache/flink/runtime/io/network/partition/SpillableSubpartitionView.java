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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

class SpillableSubpartitionView implements ResultSubpartitionView {

	/** The subpartition this view belongs to. */
	private final SpillableSubpartition parent;

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<Buffer> buffers;

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

	/**
	 * The next buffer to hand out. Everytime this is set to a non-null value,
	 * a listener notification happens.
	 */
	private Buffer nextBuffer;

	private volatile SpilledSubpartitionView spilledView;

	SpillableSubpartitionView(
		SpillableSubpartition parent,
		ArrayDeque<Buffer> buffers,
		IOManager ioManager,
		int memorySegmentSize,
		BufferAvailabilityListener listener) {

		this.parent = checkNotNull(parent);
		this.buffers = checkNotNull(buffers);
		this.ioManager = checkNotNull(ioManager);
		this.memorySegmentSize = memorySegmentSize;
		this.listener = checkNotNull(listener);

		synchronized (buffers) {
			nextBuffer = buffers.poll();
		}

		if (nextBuffer != null) {
			listener.notifyBuffersAvailable(1);
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

				int numBuffers = buffers.size();
				for (int i = 0; i < numBuffers; i++) {
					Buffer buffer = buffers.remove();
					try {
						spillWriter.writeBlock(buffer);
					} finally {
						buffer.recycle();
					}
				}

				spilledView = new SpilledSubpartitionView(
					parent,
					memorySegmentSize,
					spillWriter,
					numBuffers,
					listener);

				return numBuffers;
			}
		}
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {
		synchronized (buffers) {
			if (isReleased.get()) {
				return null;
			} else if (nextBuffer != null) {
				Buffer current = nextBuffer;
				nextBuffer = buffers.poll();

				if (nextBuffer != null) {
					listener.notifyBuffersAvailable(1);
				}

				return current;
			}
		} // else: spilled

		SpilledSubpartitionView spilled = spilledView;
		if (spilled != null) {
			return spilled.getNextBuffer();
		} else {
			throw new IllegalStateException("No in-memory buffers available, but also nothing spilled.");
		}
	}

	@Override
	public void notifyBuffersAvailable(long buffers) throws IOException {
		// We do the availability listener notification one by one
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			SpilledSubpartitionView spilled = spilledView;
			if (spilled != null) {
				spilled.releaseAllResources();
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
		return String.format("SpillableSubpartitionView(index: %d) of ResultPartition %s",
			parent.index,
			parent.parent.getPartitionId());
	}
}
