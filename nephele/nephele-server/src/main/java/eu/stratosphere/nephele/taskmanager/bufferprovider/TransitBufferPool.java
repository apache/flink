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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;

public class TransitBufferPool implements BufferProvider {

	/**
	 * The log object used to report error
	 */
	private final static Log LOG = LogFactory.getLog(TransitBufferPool.class);

	/**
	 * The singleton instance of the transit buffer pool.
	 */
	private static TransitBufferPool instance = null;

	/**
	 * The default number of buffers to create at startup.
	 */
	private static final int DEFAULT_NUMBER_OF_TRANSIT_BUFFERS = 16;

	/**
	 * The size of read/write buffers in bytes.
	 */
	private final int bufferSizeInBytes;

	/**
	 * The byte buffers in the transit pool
	 */
	private final Queue<ByteBuffer> buffers;

	/**
	 * Returns the singleton instance of the transit buffer pool. If the instance does not already exist, it is also
	 * created by calling this method.
	 * 
	 * @return the singleton instance of the transit buffer pool
	 */
	public static synchronized TransitBufferPool getInstance() {

		if (instance == null) {
			instance = new TransitBufferPool();
		}

		return instance;
	}

	/**
	 * Constructs the transit buffer pool.
	 */
	private TransitBufferPool() {

		final int numberOfBuffers = GlobalConfiguration.getInteger("channel.network.numberOfTransitBuffers",
			DEFAULT_NUMBER_OF_TRANSIT_BUFFERS);
		this.bufferSizeInBytes = GlobalConfiguration.getInteger("channel.network.bufferSizeInBytes",
			GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.buffers = new ArrayDeque<ByteBuffer>(numberOfBuffers);

		// Initialize buffers
		for (int i = 0; i < numberOfBuffers; i++) {
			final ByteBuffer readBuffer = ByteBuffer.allocateDirect(this.bufferSizeInBytes);
			this.buffers.add(readBuffer);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.bufferSizeInBytes;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		try {
			return requestBufferInternal(minimumSizeOfBuffer, false);
		} catch (final InterruptedException e) {
			LOG.error("Caught unexpected InterruptedException");
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer) throws IOException, InterruptedException {

		return requestBufferInternal(minimumSizeOfBuffer, true);
	}

	private Buffer requestBufferInternal(final int minimumSizeOfBuffer, final boolean block)
			throws InterruptedException {

		if (minimumSizeOfBuffer > this.bufferSizeInBytes) {
			throw new IllegalArgumentException("Buffer of " + minimumSizeOfBuffer
				+ " bytes is requested, but maximum buffer size is " + this.bufferSizeInBytes);
		}
		
		ByteBuffer byteBuffer;

		synchronized (this.buffers) {

			if (block) {

				while (this.buffers.isEmpty()) {
					this.buffers.wait();
				}

				byteBuffer = this.buffers.poll();

			} else {

				byteBuffer = this.buffers.poll();

				if (byteBuffer == null) {
					return null;
				}
			}

		}

		return BufferFactory.createFromMemory(minimumSizeOfBuffer, byteBuffer, this.buffers);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {
		
		return true;
	}

}
