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
import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * View over a spilled subpartition.
 *
 * <p> Reads are done synchronously.
 */
class SpilledSubpartitionViewSyncIO implements ResultSubpartitionView {

	/** The subpartition this view belongs to. */
	private final ResultSubpartition parent;

	/** The synchronous file reader to do the actual I/O. */
	private final BufferFileReader fileReader;

	/** The buffer pool to read data into. */
	private final SpillReadBufferPool bufferPool;

	/** Flag indicating whether all resources have been released. */
	private AtomicBoolean isReleased = new AtomicBoolean();

	SpilledSubpartitionViewSyncIO(
			ResultSubpartition parent,
			int memorySegmentSize,
			FileIOChannel.ID channelId,
			long initialSeekPosition) throws IOException {

		checkArgument(initialSeekPosition >= 0, "Initial seek position is < 0.");

		this.parent = checkNotNull(parent);

		this.bufferPool = new SpillReadBufferPool(2, memorySegmentSize);

		this.fileReader = new SynchronousBufferFileReader(channelId, false);

		if (initialSeekPosition > 0) {
			fileReader.seekToPosition(initialSeekPosition);
		}
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {

		if (fileReader.hasReachedEndOfFile()) {
			return null;
		}

		// It's OK to request the buffer in a blocking fashion as the buffer pool is NOT shared
		// among all consumed subpartitions.
		final Buffer buffer = bufferPool.requestBufferBlocking();

		fileReader.readInto(buffer);

		return buffer;
	}

	@Override
	public boolean registerListener(NotificationListener listener) throws IOException {
		return false;
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		parent.onConsumedSubpartition();
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			fileReader.close();
			bufferPool.destroy();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased.get();
	}

	@Override
	public Throwable getFailureCause() {
		return parent.getFailureCause();
	}

	/**
	 * A buffer pool to provide buffer to read the file into.
	 *
	 * <p> This pool ensures that a consuming input gate makes progress in all cases, even when all
	 * buffers of the input gate buffer pool have been requested by remote input channels.
	 *
	 * TODO Replace with asynchronous buffer pool request as this introduces extra buffers per
	 * consumed subpartition.
	 */
	private static class SpillReadBufferPool implements BufferRecycler {

		private final Queue<Buffer> buffers;

		private boolean isDestroyed;

		public SpillReadBufferPool(int numberOfBuffers, int memorySegmentSize) {
			this.buffers = new ArrayDeque<Buffer>(numberOfBuffers);

			synchronized (buffers) {
				for (int i = 0; i < numberOfBuffers; i++) {
					buffers.add(new Buffer(new MemorySegment(new byte[memorySegmentSize]), this));
				}
			}
		}

		@Override
		public void recycle(MemorySegment memorySegment) {
			synchronized (buffers) {
				if (isDestroyed) {
					memorySegment.free();
				}
				else {
					buffers.add(new Buffer(memorySegment, this));
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
