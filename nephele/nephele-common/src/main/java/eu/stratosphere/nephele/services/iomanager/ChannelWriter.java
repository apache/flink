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

package eu.stratosphere.nephele.services.iomanager;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A writer to an underlying {@link GatheringByteChannel}.
 * 
 * @author Alexander Alexandrov
 */
public final class ChannelWriter extends ChannelAccess<Buffer.Output> implements Writer {
	protected final BlockingQueue<Buffer.Output> emptyBufferQueue;

	protected final int emptyBufferQueueSize;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	protected ChannelWriter(Channel.ID channelID, BlockingQueue<IORequest<Buffer.Output>> requestQueue,
			Collection<Buffer.Output> buffers, boolean filledBuffers)
																		throws ServiceException {
		super(channelID, requestQueue);

		try {
			this.fileChannel.truncate(0);
		} catch (IOException e) {
			throw new ServiceException(e);
		}

		if (filledBuffers) {
			this.emptyBufferQueue = new ArrayBlockingQueue<Buffer.Output>(buffers.size(), true);
			this.emptyBufferQueueSize = buffers.size();
			for (Buffer.Output buffer : buffers)
				requestQueue.add(new IORequest<Buffer.Output>(this, buffer));
		} else {
			// initialize the empty buffer queue, all buffers are empty
			this.emptyBufferQueue = new ArrayBlockingQueue<Buffer.Output>(buffers.size(), true, buffers);
			this.emptyBufferQueueSize = buffers.size();
		}

		// mark the first buffer from the empty buffer queue as current
		currentBuffer = nextBuffer();
	}

	@Override
	public synchronized Collection<MemorySegment> close() throws ServiceException {
		if (!isClosing) {
			// set closing flag
			isClosing = true;

			// create a new write request for the current buffer
			requestQueue.add(new IORequest<Buffer.Output>(this, currentBuffer));

			// wait until all pending IORequests are handled
			lock.lock();
			while (emptyBufferQueue.size() < emptyBufferQueueSize) {
				allRequestsHandled.awaitUninterruptibly();
			}
			lock.unlock();

			// flush contents to the underlying channel and close the file
			try {
				fileChannel.force(true);
				file.close();
			} catch (IOException e) {
				throw new ServiceException(e);
			}
		}

		return IOManager.unbindBuffers(emptyBufferQueue);
	}

	@Override
	protected void handleProcessedBuffer(Buffer.Output buffer) {
		// append the buffer to the empty buffers queue
		emptyBufferQueue.add(buffer);

		if (emptyBufferQueue.size() == emptyBufferQueueSize) {
			// notify writer thread waiting for completion of all requests
			lock.lock();
			allRequestsHandled.signalAll();
			lock.unlock();
		}
	}

	/**
	 * Writes the contents of the {@code writable} to the current output buffer.
	 * If the buffer is exhausted while writing, transparently swaps the buffers
	 * and retries writing to the next buffer. Returns {@code true} if the write
	 * operation was successfull or {@code false} if the provided {@code
	 * writable} is larger than the size provided by an empty output buffer.
	 * 
	 * @param writable
	 *        the object writing itself to the current output buffer
	 * @return a boolean flag indicating the success of the write operation
	 */
	public boolean write(IOReadableWritable writable) {
		if (currentBuffer.write(writable)) {
			// object was written on current buffer without a problem
			return true;
		} else {
			// current buffer is full; swap buffers...
			requestQueue.add(new IORequest<Buffer.Output>(this, currentBuffer));
			currentBuffer = nextBuffer();
			// ...and then retry writing with an empty input buffer
			return currentBuffer.write(writable);
		}
	}

	/**
	 * Take the next input buffer from the full buffers queue.
	 * 
	 * @return the next output buffer from the empty buffer queue
	 */
	protected Buffer.Output nextBuffer() {
		try {
			return emptyBufferQueue.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * A worker thread for asynchronous write.
	 * 
	 * @author Alexander Alexandrov
	 */
	protected static class WriterThread implements Runnable {
		private static final IORequest<Buffer.Output> SENTINEL = new IORequest<Buffer.Output>(null, null);

		protected final BlockingQueue<IORequest<Buffer.Output>> requestQueue;

		protected boolean isClosing;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread() {
			this.requestQueue = new LinkedBlockingQueue<IORequest<Buffer.Output>>();
			this.isClosing = false;
		}

		protected void close() {
			if (!isClosing) {
				isClosing = true;
				requestQueue.add(SENTINEL);
			}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run() {
			try {
				// fetch the first write request
				IORequest<Buffer.Output> request = requestQueue.take();

				while (request != SENTINEL) // repeat until the SENTINEL request arrives
				{
					try {
						// write buffer to the specified channel
						request.buffer.writeToChannel(request.channelAccess.fileChannel);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}

					// invoke the processed buffer handler of the request issuing writer object
					request.channelAccess.handleProcessedBuffer(request.buffer);

					// fetch next request
					request = requestQueue.take();
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
