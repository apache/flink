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

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;

public final class LocalBufferCache implements BufferProvider {

	private final static Log LOG = LogFactory.getLog(LocalBufferCache.class);

	private final GlobalBufferPool globalBufferPool;

	private final int maximumBufferSize;

	private int designatedNumberOfBuffers;

	private int requestedNumberOfBuffers = 0;

	private final boolean isShared;

	private boolean asynchronousEventOccurred = false;

	private final AsynchronousEventListener eventListener;

	private final Queue<ByteBuffer> buffers = new ArrayDeque<ByteBuffer>();

	public LocalBufferCache(final int designatedNumberOfBuffers, final boolean isShared,
			final AsynchronousEventListener eventListener) {

		this.globalBufferPool = GlobalBufferPool.getInstance();
		this.maximumBufferSize = this.globalBufferPool.getMaximumBufferSize();
		this.designatedNumberOfBuffers = designatedNumberOfBuffers;
		this.isShared = isShared;
		this.eventListener = eventListener;
	}

	public LocalBufferCache(final int designatedNumberOfBuffers, final boolean isShared) {
		this(designatedNumberOfBuffers, isShared, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer, final int minimumReserve) throws IOException {

		try {
			return requestBufferInternal(minimumSizeOfBuffer, minimumReserve, false);
		} catch (InterruptedException e) {
			LOG.error("Caught unexpected InterruptedException");
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBufferBlocking(final int minimumSizeOfBuffer, final int minimumReserve)
			throws IOException, InterruptedException {

		return requestBufferInternal(minimumSizeOfBuffer, minimumReserve, true);
	}

	private Buffer requestBufferInternal(final int minimumSizeOfBuffer, int minimumReserve, final boolean block)
			throws IOException, InterruptedException {

		if (minimumSizeOfBuffer > this.maximumBufferSize) {
			throw new IllegalArgumentException("Buffer of " + minimumSizeOfBuffer
				+ " bytes is requested, but maximum buffer size is " + this.maximumBufferSize);
		}

		while (true) {

			boolean async = false;

			synchronized (this.buffers) {

				// Make sure we return excess buffers immediately
				while (this.requestedNumberOfBuffers > this.designatedNumberOfBuffers) {

					final ByteBuffer buffer = this.buffers.poll();
					if (buffer == null) {
						break;
					}

					this.globalBufferPool.releaseGlobalBuffer(buffer);
					this.requestedNumberOfBuffers--;
				}

				if (minimumReserve > this.designatedNumberOfBuffers) {
					LOG.warn("Minimum reserve " + minimumReserve + " is larger than number of designated buffers "
						+ this.designatedNumberOfBuffers + ", reducing reserve...");
					minimumReserve = this.designatedNumberOfBuffers;
				}

				while (this.buffers.size() <= minimumReserve) {

					// Check if the number of cached buffers matches the number of designated buffers
					if (this.requestedNumberOfBuffers < this.designatedNumberOfBuffers) {

						final ByteBuffer buffer = this.globalBufferPool.lockGlobalBuffer();
						if (buffer != null) {

							this.buffers.add(buffer);
							this.requestedNumberOfBuffers++;
							continue;
						}
					}

					if (this.asynchronousEventOccurred) {
						this.asynchronousEventOccurred = false;
						async = true;
						break;
					}

					if (block) {
						this.buffers.wait();
					} else {
						return null;
					}
				}

				if (!async) {
					final ByteBuffer byteBuffer = this.buffers.poll();
					return BufferFactory.createFromMemory(minimumSizeOfBuffer, byteBuffer, this.buffers);
				}
			}

			if (this.eventListener != null) {
				this.eventListener.asynchronousEventOccurred();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getMaximumBufferSize() {

		return this.maximumBufferSize;
	}

	/**
	 * Sets the designated number of buffers for this local buffer cache.
	 * 
	 * @param designatedNumberOfBuffers
	 *        the designated number of buffers for this local buffer cache
	 */
	public void setDesignatedNumberOfBuffers(final int designatedNumberOfBuffers) {

		synchronized (this.buffers) {

			this.designatedNumberOfBuffers = designatedNumberOfBuffers;

			// Make sure we return excess buffers immediately
			while (this.requestedNumberOfBuffers > this.designatedNumberOfBuffers) {

				if (this.buffers.isEmpty()) {
					break;
				}

				this.globalBufferPool.releaseGlobalBuffer(this.buffers.poll());
				this.requestedNumberOfBuffers--;
			}

			this.buffers.notify();
		}
	}

	public void clear() {

		synchronized (this.buffers) {

			if (this.requestedNumberOfBuffers != this.buffers.size()) {
				LOG.error("Clear is called, but some buffers are still missing...");
			}

			while (!this.buffers.isEmpty()) {
				this.globalBufferPool.releaseGlobalBuffer(this.buffers.poll());
			}

			this.requestedNumberOfBuffers = 0;
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShared() {

		return this.isShared;
	}

	public int getNumberOfAvailableBuffers() {

		synchronized (this.buffers) {
			return this.buffers.size();
		}
	}

	public int getDesignatedNumberOfBuffers() {

		synchronized (this.buffers) {
			return this.designatedNumberOfBuffers;
		}
	}

	public int getRequestedNumberOfBuffers() {

		synchronized (this.buffers) {
			return this.requestedNumberOfBuffers;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportAsynchronousEvent() {

		synchronized (this.buffers) {
			this.asynchronousEventOccurred = true;
			this.buffers.notify();
		}
	}
}
