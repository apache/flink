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
import java.nio.channels.ScatteringByteChannel;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A reader from an underlying {@link ScatteringByteChannel}.
 * 
 * @author Alexander Alexandrov
 */
public final class ChannelReader extends ChannelAccess<Buffer.Input> implements Reader {
	protected final BlockingQueue<Buffer.Input> fullBufferQueue;

	protected final int fullBufferQueueSize;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	protected ChannelReader(Channel.ID channelID, BlockingQueue<IORequest<Buffer.Input>> requestQueue,
			Collection<MemorySegment> freeSegments)
													throws ServiceException {
		super(channelID, requestQueue);

		// initialize the full buffer queue
		this.fullBufferQueue = new ArrayBlockingQueue<Buffer.Input>(freeSegments.size(), true);
		this.fullBufferQueueSize = freeSegments.size();

		// add all buffers to the prefetching queue
		for (Buffer.Input buffer : IOManager.createBuffer(Buffer.Type.INPUT, freeSegments)) {
			requestQueue.add(new IORequest<Buffer.Input>(this, buffer));
		}

		// get first full buffer
		currentBuffer = nextBuffer();
	}

	@Override
	public synchronized Collection<MemorySegment> close() throws ServiceException {
		if (!isClosing) {
			// set closing flag
			isClosing = true;

			// put current buffer back to the full buffer queue
			fullBufferQueue.add(currentBuffer);

			// wait until all pending IORequests are handled
			lock.lock();
			while (fullBufferQueue.size() < fullBufferQueueSize) {
				allRequestsHandled.awaitUninterruptibly();
			}
			lock.unlock();

			// close the file
			try {
				file.close();
			} catch (IOException e) {
				throw new ServiceException(e);
			}
		}

		return IOManager.unbindBuffers(fullBufferQueue);
	}

	@Override
	protected void handleProcessedBuffer(Buffer.Input buffer) {
		// append the buffer to the full buffers queue
		fullBufferQueue.add(buffer);

		if (fullBufferQueue.size() == fullBufferQueueSize) {
			// notify writer thread waiting for completion of all requests
			lock.lock();
			allRequestsHandled.signalAll();
			lock.unlock();
		}
	}

	/**
	 * Read the contents of the {@code readable} from the current input buffer.
	 * If the buffer is exhausted while reading, transparently swaps the buffers
	 * and retries reading from the next buffer. Returns {@code true} if the
	 * read operation was successfull or {@code false} if the underlying channel
	 * is exhausted.
	 * 
	 * @param readable
	 *        the object reading from the current input buffer
	 * @return a boolean flag indicating the success of the read operation
	 */
	public boolean read(IOReadableWritable readable) {
		if (currentBuffer.read(readable)) // try to read from current buffer
		{
			// object was read from the current buffer without a problem
			return true;
		} else {
			// current buffer is exhausted, swap buffers...
			requestQueue.add(new IORequest<Buffer.Input>(this, currentBuffer));
			currentBuffer = this.nextBuffer();
			// ...and then retry reading from the next full buffer
			return currentBuffer.read(readable);
		}
	}

	/**
	 * Take the next input buffer from the full buffers queue.
	 * 
	 * @return the next input buffer from the full buffer queue
	 */
	protected Buffer.Input nextBuffer() {
		try {
			return fullBufferQueue.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * A worker thread for asynchronous read.
	 * 
	 * @author Alexander Alexandrov
	 */
	protected static class ReaderThread implements Runnable {
		private static final IORequest<Buffer.Input> SENTINEL = new IORequest<Buffer.Input>(null, null);

		protected final BlockingQueue<IORequest<Buffer.Input>> requestQueue;

		protected boolean isClosing;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected ReaderThread() {
			this.requestQueue = new LinkedBlockingQueue<IORequest<Buffer.Input>>();
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
				// fetch the first read request
				IORequest<Buffer.Input> request = requestQueue.take();

				while (request != SENTINEL) // repeat until the SENTINEL request arrives
				{
					try {
						// read buffer from the specified channel
						request.buffer.readFromChannel(request.channelAccess.fileChannel);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}

					// invoke the processed buffer handler of the request issuing reader object
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
