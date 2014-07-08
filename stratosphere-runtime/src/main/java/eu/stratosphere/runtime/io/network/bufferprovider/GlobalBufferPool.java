/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.bufferprovider;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.core.memory.MemorySegment;

/**
 * A global buffer pool for the network stack.
 * <p>
 * All buffers used by the network stack come from this pool. Requests to this pool are mediated by instances of
 * {@link LocalBufferPool}.
 * <p>
 * The size and number of buffers can be configured via the global system config.
 */
public final class GlobalBufferPool {

	private final static Log LOG = LogFactory.getLog(GlobalBufferPool.class);

	// -----------------------------------------------------------------------------------------------------------------

	/** Total number of buffers */
	private final int numBuffers;

	/** Size of each buffer (in bytes) */
	private final int bufferSize;

	/** The available buffers */
	private final Queue<MemorySegment> buffers;

	private boolean isDestroyed;

	// -----------------------------------------------------------------------------------------------------------------

	public GlobalBufferPool(int numBuffers, int bufferSize) {
		this.numBuffers = numBuffers;
		this.bufferSize = bufferSize;

		buffers = new ArrayBlockingQueue<MemorySegment>(numBuffers);

		final int mb = 1 << 20;
		final int memRequiredMb = (numBuffers * bufferSize) / mb;

		for (int i = 0; i < numBuffers; i++) {
			try {
				byte[] buf = new byte[bufferSize];
				buffers.add(new MemorySegment(buf));
			} catch (OutOfMemoryError err) {
				int memAllocatedMb = ((i + 1) * bufferSize) / mb;

				String msg = String.format("Tried to allocate %d buffers of size %d bytes each (total: %d MB) " +
						"and ran out of memory after %d buffers (%d MB).",
						numBuffers, bufferSize, memRequiredMb, i + 1, memAllocatedMb);
				throw new OutOfMemoryError(msg);
			}
		}

		LOG.info(String.format("Allocated %d buffers of size %d bytes each (total: %d MB).",
				numBuffers, bufferSize, memRequiredMb));
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Requests a buffer <strong>from</strong> the pool.
	 *
	 * @return buffer from pool or <code>null</code>, if no buffer available
	 */
	public MemorySegment requestBuffer() {
		return this.buffers.poll();
	}

	/**
	 * Returns a buffer <em>to</em> the pool.
	 *
	 * @param buffer the buffer to be returned
	 */
	public void returnBuffer(MemorySegment buffer) {
		this.buffers.add(buffer);
	}

	/**
	 * Returns the size of buffers (in bytes).
	 *
	 * @return size of buffers (in bytes)
	 */
	public int getBufferSize() {
		return this.bufferSize;
	}

	/**
	 * Returns the total number of managed buffers.
	 * 
	 * @return total number of managed buffers
	 */
	public int numBuffers() {
		return this.numBuffers;
	}

	/**
	 * Returns the number of currently available buffers.
	 * 
	 * @return currently available number of buffers
	 */
	public int numAvailableBuffers() {
		return this.buffers.size();
	}

	public synchronized void destroy() {
		if (!this.isDestroyed) {
			// mark as shutdown and release memory
			this.isDestroyed = true;

			for (MemorySegment buffer : this.buffers) {
				buffer.free();
			}

			this.buffers.clear();
		}
	}
}
