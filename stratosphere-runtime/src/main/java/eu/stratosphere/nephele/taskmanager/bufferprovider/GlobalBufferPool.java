/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager.bufferprovider;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.memory.MemorySegment;

public final class GlobalBufferPool {

	private final static Log LOG = LogFactory.getLog(GlobalBufferPool.class);
	
	/**
	 * The singleton instance of the global buffer pool.
	 */
	private static GlobalBufferPool instance = null;

	/**
	 * The default number of buffers to create at startup.
	 */
	private static final int DEFAULT_NUMBER_OF_BUFFERS = 2048;

	/**
	 * The default buffer size in bytes.
	 */
	public static final int DEFAULT_BUFFER_SIZE_IN_BYTES = 64 * 1024; // 64k

	/**
	 * The number of buffers created at startup.
	 */
	private final int numberOfBuffers;

	/**
	 * The size of read/write buffers in bytes.
	 */
	private final int bufferSizeInBytes;

	private final Queue<MemorySegment> buffers;

	/**
	 * Returns the singleton instance of the global buffer pool. If the instance does not already exist, it is also
	 * created by calling this method.
	 * 
	 * @return the singleton instance of the global buffer pool
	 */
	public static synchronized GlobalBufferPool getInstance() {

		if (instance == null) {
			instance = new GlobalBufferPool();
		}

		return instance;
	}

	/**
	 * Constructs the global buffer pool.
	 */
	private GlobalBufferPool() {

		this.numberOfBuffers = GlobalConfiguration.getInteger("channel.network.numberOfBuffers",
			DEFAULT_NUMBER_OF_BUFFERS);
		this.bufferSizeInBytes = GlobalConfiguration.getInteger("channel.network.bufferSizeInBytes",
			DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.buffers = new ArrayBlockingQueue<MemorySegment>(this.numberOfBuffers);

		// Initialize buffers
		for (int i = 0; i < this.numberOfBuffers; i++) {
			// allocate byteBuffer
			final byte[] segMemory = new byte[this.bufferSizeInBytes];
			final MemorySegment readBuffer = new MemorySegment(segMemory);
			this.buffers.add(readBuffer);
		}

		LOG.info("Initialized global buffer pool with " + this.numberOfBuffers + " buffers with a size "
			+ this.bufferSizeInBytes + " bytes each");
	}

	/**
	 * Returns the maximum size of a buffer available at this pool in bytes.
	 * 
	 * @return the maximum size of a buffer available at this pool in bytes
	 */
	public int getMaximumBufferSize() {

		return this.bufferSizeInBytes;
	}

	/**
	 * Locks a buffer from the global buffer pool and returns it to the caller of this method.
	 * 
	 * @return the locked buffer from the pool or <code>null</code> if currently no global buffer is available
	 */
	public MemorySegment lockGlobalBuffer() {

		return this.buffers.poll();
	}

	/**
	 * Releases a lock on a previously locked buffer and returns the buffer to the global pool.
	 * 
	 * @param releasedBuffer
	 *        the previously locked buffer to be released
	 */
	public void releaseGlobalBuffer(final MemorySegment releasedBuffer) {
		this.buffers.add(releasedBuffer);
	}

	/**
	 * Returns the total number of buffers managed by this pool.
	 * 
	 * @return the total number of buffers managed by this pool
	 */
	public int getTotalNumberOfBuffers() {

		return this.numberOfBuffers;
	}

	/**
	 * Returns the number of buffers which are currently available at this pool.
	 * 
	 * @return the number of buffers which are currently available at this pool
	 */
	public int getCurrentNumberOfBuffers() {

		return this.buffers.size();
	}
}
