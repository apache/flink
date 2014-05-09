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

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.BufferRecycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A buffer pool used to manage a designated number of buffers from a {@link GlobalBufferPool}.
 * <p>
 * A local buffer pool mediates buffer requests to the global buffer pool to ensure dead-lock free operation of the
 * network stack by limiting the number of designated buffers per local buffer pool. It also implements the default
 * mechanism for buffer recycling, which ensures that every buffer is ultimately returned to the global buffer pool.
 */
public final class LocalBufferPool implements BufferProvider {

	private static final class LocalBufferPoolRecycler implements BufferRecycler {

		private final LocalBufferPool bufferPool;

		private LocalBufferPoolRecycler(LocalBufferPool bufferPool) {
			this.bufferPool = bufferPool;
		}

		@Override
		public void recycle(MemorySegment buffer) {
			this.bufferPool.recycleBuffer(buffer);
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	/** Time (ms) to wait before retry for blocking buffer requests */
	private static final int WAIT_TIME = 100;

	/** Global buffer pool to request buffers from */
	private final GlobalBufferPool globalBufferPool;

	/** Buffers managed by this local buffer pool */
	private final Queue<MemorySegment> buffers = new ArrayDeque<MemorySegment>();

	/** The recycler via which to return buffers to this local buffer pool */
	private final LocalBufferPoolRecycler recycler;

	/** Queue of buffer availability listeners */
	private final Queue<BufferAvailabilityListener> listeners = new ArrayDeque<BufferAvailabilityListener>();

	/** Size of each buffer in this pool (in bytes) */
	private final int bufferSize;

	/** Number of buffers assigned to this local buffer pool */
	private int numDesignatedBuffers;

	/** Number of buffers requested from the global buffer pool */
	private int numRequestedBuffers;

	/** Flag to indicate whether an asynchronous event has been reported */
	private boolean hasAsyncEventOccurred;

	/** Flag to indicate whether this local buffer pool has been destroyed */
	private boolean isDestroyed;

	// -----------------------------------------------------------------------------------------------------------------

	public LocalBufferPool(GlobalBufferPool globalBufferPool, int numDesignatedBuffers) {
		this.globalBufferPool = globalBufferPool;
		this.bufferSize = globalBufferPool.getBufferSize();
		this.numDesignatedBuffers = numDesignatedBuffers;

		this.recycler = new LocalBufferPoolRecycler(this);
	}
	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Buffer requestBuffer(int minBufferSize) throws IOException {
		try {
			return requestBuffer(minBufferSize, false);
		} catch (InterruptedException e) {
			throw new IOException("Unexpected InterruptedException while non-blocking buffer request.");
		}
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException {
		return requestBuffer(minBufferSize, true);
	}

	/**
	 * Requests a buffer from this local buffer pool.
	 * <p>
	 * A non-blocking call to this method will only return a buffer, if one is available in the local pool after
	 * having returned excess buffers. Otherwise, it will return <code>null</code>.
	 * <p>
	 * A blocking call will request a new buffer from the global buffer and block until one is available or an
	 * asynchronous event has been reported via {@link #reportAsynchronousEvent()}.
	 *
	 * @param minBufferSize minimum size of requested buffer (in bytes)
	 * @param isBlocking flag to indicate whether to block until buffer is available
	 * @return buffer from the global buffer pool or <code>null</code>, if no buffer available
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Buffer requestBuffer(int minBufferSize, boolean isBlocking) throws IOException, InterruptedException {
		if (minBufferSize > this.bufferSize) {
			throw new IllegalArgumentException(String.format("Too large buffer requested (requested %d, maximum %d).",
					minBufferSize, this.bufferSize));
		}

		while (true) {
			boolean isAsyncRequest = false;

			synchronized (this.buffers) {
				// Return excess buffers to global buffer pool
				while (this.numRequestedBuffers > this.numDesignatedBuffers) {
					final MemorySegment buffer = this.buffers.poll();
					if (buffer == null) {
						break;
					}

					this.globalBufferPool.returnBuffer(buffer);
					this.numRequestedBuffers--;
				}

				// Request buffers from global buffer pool
				while (this.buffers.isEmpty()) {
					if (this.numRequestedBuffers < this.numDesignatedBuffers) {
						final MemorySegment buffer = this.globalBufferPool.requestBuffer();

						if (buffer != null) {
							this.buffers.add(buffer);

							this.numRequestedBuffers++;
							continue;
						}
					}

					if (this.hasAsyncEventOccurred && isBlocking) {
						this.hasAsyncEventOccurred = false;
						isAsyncRequest = true;
						break;
					}

					if (isBlocking) {
						this.buffers.wait(WAIT_TIME);
					} else {
						return null;
					}
				}

				if (!isAsyncRequest) {
					return new Buffer(this.buffers.poll(), minBufferSize, this.recycler);
				}
			}
		}
	}

	@Override
	public int getBufferSize() {
		return this.bufferSize;
	}

	@Override
	public void reportAsynchronousEvent() {
		synchronized (this.buffers) {
			this.hasAsyncEventOccurred = true;
			this.buffers.notify();
		}
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		synchronized (this.buffers) {
			if (!this.buffers.isEmpty()) {
				return BufferAvailabilityRegistration.NOT_REGISTERED_BUFFER_AVAILABLE;
			}

			if (this.isDestroyed) {
				return BufferAvailabilityRegistration.NOT_REGISTERED_BUFFER_POOL_DESTROYED;
			}

			this.listeners.add(listener);
		}

		return BufferAvailabilityRegistration.REGISTERED;
	}

	/**
	 * Sets the designated number of buffers for this local buffer pool and returns excess buffers to the global buffer
	 * pool.
	 * <p>
	 * The designated number of buffers determines how many buffers this buffer pool is allowed to manage. New buffers
	 * can only be requested, if the requested number of buffers is less than the designated number. If possible, excess
	 * buffers will be returned to the global buffer pool.
	 *
	 * @param numDesignatedBuffers number of buffers designated for this local buffer pool
	 */
	public void setNumDesignatedBuffers(int numDesignatedBuffers) {
		synchronized (this.buffers) {
			this.numDesignatedBuffers = numDesignatedBuffers;

			// Return excess buffers to global buffer pool
			while (this.numRequestedBuffers > this.numDesignatedBuffers) {
				if (this.buffers.isEmpty()) {
					break;
				}

				this.globalBufferPool.returnBuffer(this.buffers.poll());
				this.numRequestedBuffers--;
			}

			this.buffers.notify();
		}
	}

	/**
	 * Returns the number of buffers available in the local buffer pool.
	 *
	 * @return number of available buffers
	 */
	public int numAvailableBuffers() {
		synchronized (this.buffers) {
			return this.buffers.size();
		}
	}

	/**
	 * Returns the number of buffers, which have been requested from the global buffer pool.
	 *
	 * @return number of buffers requested from the global buffer pool
	 */
	public int numRequestedBuffers() {
		synchronized (this.buffers) {
			return this.numRequestedBuffers;
		}
	}

	/**
	 * Returns the designated number of buffers for this local buffer pool.
	 *
	 * @return number of designated buffers for this buffer pool
	 */
	public int numDesignatedBuffers() {
		synchronized (this.buffers) {
			return this.numDesignatedBuffers;
		}
	}

	/**
	 * Destroys this local buffer pool and immediately returns all available buffers to the global buffer pool.
	 * <p>
	 * Buffers, which have been requested from this local buffer pool via <code>requestBuffer</code> cannot be returned
	 * immediately and will be returned when the respective buffer is recycled (see {@link #recycleBuffer(MemorySegment)}).
	 */
	public void destroy() {
		synchronized (this.buffers) {
			if (this.isDestroyed) {
				return;
			}

			this.isDestroyed = true;

			// return all buffers
			while (!this.buffers.isEmpty()) {
				this.globalBufferPool.returnBuffer(this.buffers.poll());
				this.numRequestedBuffers--;
			}
		}
	}

	/**
	 * Returns a buffer to the buffer pool and notifies listeners about the availability of a new buffer.
	 *
	 * @param buffer buffer to return to the buffer pool
	 */
	private void recycleBuffer(MemorySegment buffer) {
		synchronized (this.buffers) {
			if (this.isDestroyed) {
				this.globalBufferPool.returnBuffer(buffer);
				this.numRequestedBuffers--;
			} else {
				if (!this.listeners.isEmpty()) {
					Buffer availableBuffer = new Buffer(buffer, buffer.size(), this.recycler);
					try {
						this.listeners.poll().bufferAvailable(availableBuffer);
					} catch (Exception e) {
						this.buffers.add(buffer);
						this.buffers.notify();
					}
				} else {
					this.buffers.add(buffer);
					this.buffers.notify();
				}
			}
		}
	}
}
