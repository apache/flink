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
import eu.stratosphere.nephele.io.channels.MemoryBufferPoolConnector;

public final class LocalBufferPool implements BufferProvider {

	private static final class LocalBufferPoolConnector implements MemoryBufferPoolConnector {

		private final LocalBufferPool localBufferPool;

		private LocalBufferPoolConnector(final LocalBufferPool localBufferPool) {
			this.localBufferPool = localBufferPool;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void recycle(final ByteBuffer byteBuffer) {

			this.localBufferPool.recycleBuffer(byteBuffer);
		}

	}

	private final static Log LOG = LogFactory.getLog(LocalBufferPool.class);

	private final GlobalBufferPool globalBufferPool;

	private final int maximumBufferSize;

	private int designatedNumberOfBuffers;

	private int requestedNumberOfBuffers = 0;

	private final boolean isShared;

	private boolean asynchronousEventOccurred = false;

	private boolean isDestroyed = false;

	private final AsynchronousEventListener eventListener;

	private final Queue<ByteBuffer> buffers = new ArrayDeque<ByteBuffer>();

	private final LocalBufferPoolConnector bufferPoolConnector;

	public LocalBufferPool(final int designatedNumberOfBuffers, final boolean isShared,
			final AsynchronousEventListener eventListener) {

		this.globalBufferPool = GlobalBufferPool.getInstance();
		this.maximumBufferSize = this.globalBufferPool.getMaximumBufferSize();
		this.designatedNumberOfBuffers = designatedNumberOfBuffers;
		this.isShared = isShared;
		this.eventListener = eventListener;
		this.bufferPoolConnector = new LocalBufferPoolConnector(this);
	}

	public LocalBufferPool(final int designatedNumberOfBuffers, final boolean isShared) {
		this(designatedNumberOfBuffers, isShared, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Buffer requestEmptyBuffer(final int minimumSizeOfBuffer) throws IOException {

		try {
			return requestBufferInternal(minimumSizeOfBuffer, false);
		} catch (InterruptedException e) {
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

	private Buffer requestBufferInternal(final int minimumSizeOfBuffer, final boolean block) throws IOException,
			InterruptedException {

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

				while (this.buffers.isEmpty()) {

					// Check if the number of cached buffers matches the number of designated buffers
					if (this.requestedNumberOfBuffers < this.designatedNumberOfBuffers) {

						final ByteBuffer buffer = this.globalBufferPool.lockGlobalBuffer();
						if (buffer != null) {

							this.buffers.add(buffer);
							this.requestedNumberOfBuffers++;
							continue;
						}
					}

					if (this.asynchronousEventOccurred && block) {
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
					return BufferFactory.createFromMemory(minimumSizeOfBuffer, byteBuffer, this.bufferPoolConnector);
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

	public void destroy() {

		synchronized (this.buffers) {

			if (this.isDestroyed) {
				LOG.error("destroy is called on LocalBufferPool multiple times");
				return;
			}

			this.isDestroyed = true;

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

	private void recycleBuffer(final ByteBuffer byteBuffer) {

		synchronized (this.buffers) {

			if (this.isDestroyed) {
				this.globalBufferPool.releaseGlobalBuffer(this.buffers.poll());
				this.requestedNumberOfBuffers--;
				return;
			}

			this.buffers.add(byteBuffer);
			this.buffers.notify();
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
