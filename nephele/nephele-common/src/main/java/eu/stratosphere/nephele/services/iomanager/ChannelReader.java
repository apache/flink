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
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A reader from an underlying {@link ScatteringByteChannel}.
 * 
 * @author Alexander Alexandrov
 */
public final class ChannelReader extends ChannelAccess<Buffer.Input> implements Reader
{
	/**
	 * The current buffer from which is read.
	 */
	private Buffer.Input currentBuffer;
	
	/**
	 * Flag marking this channel as closed.
	 */
	private volatile boolean closed = false;
	
	
	// -------------------------------------------------------------------------
	//                     Constructors / Destructors
	// -------------------------------------------------------------------------

	public ChannelReader(Channel.ID channelID, RequestQueue<IORequest<Buffer.Input>> requestQueue,
			Collection<Buffer.Input> buffers)
	throws IOException
	{
		super(channelID, requestQueue, buffers);

		// add all buffers to the request queue
		for (Buffer.Input buffer : buffers) {
			this.requestQueue.add(new IORequest<Buffer.Input>(this, buffer));
		}
	}

	@Override
	public List<MemorySegment> close() throws IOException
	{
		synchronized (this) {
			if (this.closed) {
				throw new IllegalStateException("Reader is already closing or has been closed.");
			}
			this.closed = true;
		}
		
		// put current buffer back to the full buffer queue
		this.returnBuffers.add(currentBuffer);
		this.currentBuffer = null;
		
		// close the reader, getting all segments back
		List<MemorySegment> segments = super.close();

		// close the file
		fileChannel.close();
		
		return segments;
	}

	/**
	 * Read the contents of the {@code readable} from the current input buffer.
	 * If the buffer is exhausted while reading, transparently swaps the buffers
	 * and retries reading from the next buffer. Returns {@code true} if the
	 * read operation was successful or {@code false} if the underlying channel
	 * is exhausted.
	 * 
	 * @param readable The object reading from the current input buffer
	 * @return a boolean flag indicating the success of the read operation
	 */
	public boolean read(IOReadableWritable readable) throws IOException
	{
		if (this.currentBuffer == null) {
			if (this.closed) {
				throw new IllegalStateException("Reader has been closed.");
			}
			else {
				try {
					this.currentBuffer = this.nextBuffer();
				}
				catch (InterruptedException iex) {
					throw new IOException("IO channel corrupt. Reader was interrupted getting a new buffer.");
				}
			}
		}
		
		if (currentBuffer.read(readable)) // try to read from current buffer
		{
			// object was read from the current buffer without a problem
			return true;
		}
		else {
			// current buffer is exhausted. check the error state of this channel first
			checkErroneous();
			if (this.requestQueue.isClosed()) {
				throw new IOException("The reader's IO path has been closed.");
			}
			
			// current buffer is exhausted, swap buffers...
			this.requestQueue.add(new IORequest<Buffer.Input>(this, currentBuffer));
			try {
				this.currentBuffer = this.nextBuffer();
			}
			catch (InterruptedException iex) {
				throw new IOException("IO channel corrupt. Reader was interrupted getting a new buffer.");
			}
			
			// ...and then retry reading from the next full buffer
			return currentBuffer.read(readable);
		}
	}
	
	/**
	 * Reads the most recently read {@code IOReadableWritable} from the current input buffer.
	 * If the buffer is exhausted while reading, transparently swaps the buffers
	 * and retries reading from the next buffer. Returns {@code true} if the
	 * read operation was successful or {@code false} if the underlying channel
	 * is exhausted.
	 * 
	 * @param object
	 *          to read in from the buffer
	 * @return a boolean value indicating whether the read was successful
	 * @throws UnboundMemoryBackedException
	 */
	public boolean repeatRead(IOReadableWritable readable) {
		if (this.currentBuffer == null) {
			if (this.closed) {
				throw new IllegalStateException("Reader has been closed.");
			}
			else {
				throw new IllegalStateException("No previous read has occurred.");
			}
		}
			
		if (this.currentBuffer.repeatRead(readable)) // try to read from current buffer
		{
			return true;
		}
		else {
			// this should never happen, because the previous read was successful and the
			// buffers are only swapped before reads
			// throw an exception to indicate the problem
			throw new IllegalStateException("BUG: Repeated read failed after a successful read.");
		}
	}

	/**
	 * A worker thread for asynchronous read.
	 * 
	 * @author Alexander Alexandrov
	 */
	protected static class ReaderThread extends Thread
	{
		protected final RequestQueue<IORequest<Buffer.Input>> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected ReaderThread() {
			this.requestQueue = new RequestQueue<IORequest<Buffer.Input>>();
			this.alive = true;
		}
		
		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel readers and an exception is
		 * reported to them, declaring their request queue as closed.
		 */
		protected void shutdown() {
			if (alive) {
				// shut down the thread
				try {
					this.alive = false;
					this.requestQueue.close();
					this.interrupt();
				}
				catch (Throwable t) {}
				
				// notify all pending write requests that the thread has been shut down
				IOException ioex = new IOException("Reading thread has been closed.");
				
				while (!this.requestQueue.isEmpty()) {
					IORequest<Buffer.Input> request = this.requestQueue.poll();
					request.channel.handleProcessedBuffer(request.buffer, ioex);
				}
			}
		}

		// ---------------------------------------------------------------------
		//                             Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run()
		{
			while (this.alive)
			{
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				IORequest<Buffer.Input> request = null;
				while (request == null) {
					try {
						request = this.requestQueue.take();
					}
					catch (InterruptedException iex) {
						if (!this.alive) {
							// exit
							return;
						}
					}
				}
				
				// remember any IO exception that occurs, so it can be reported to the writer
				IOException ioex = null;

				try {
					// read buffer from the specified channel
					request.buffer.readFromChannel(request.channel.fileChannel);
				}
				catch (IOException e) {
					ioex = e;
				}

				// invoke the processed buffer handler of the request issuing reader object
				request.channel.handleProcessedBuffer(request.buffer, ioex);
			} // end while alive
		}
		
	} // end reading thread
	
}
