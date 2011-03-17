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
import java.util.Collection;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A writer to an underlying {@link Channel}. This writer buffers written objects in a buffer, which is handed to
 * a queue for write requests. The queue is polled by a thread, typically a single thread for all instances of this
 * writer, which actually writes the buffers to disk.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public final class ChannelWriter extends ChannelAccess<Buffer.Output> implements Writer
{
	/**
	 * The current buffer that write requests go to.
	 */
	private Buffer.Output currentBuffer;
	
	/**
	 * Flag marking this channel as closed;
	 */
	private volatile boolean closed;

	
	// -------------------------------------------------------------------------
	//                      Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * 
	 * @param channelID
	 * @param requestQueue
	 * @param buffers
	 * @param filledBuffers
	 * @throws IOException
	 */
	protected ChannelWriter(Channel.ID channelID, RequestQueue<IORequest<Buffer.Output>> requestQueue,
			Collection<Buffer.Output> buffers, boolean filledBuffers)
	throws IOException
	{
		super(channelID, requestQueue, buffers);

		try {
			this.fileChannel.truncate(0);
		}
		catch (IOException e) {
			throw new IOException("Channel to path '" + channelID.getPath() + "' could not be opened.", e);
		}

		if (filledBuffers) {
			for (Buffer.Output buffer : buffers) {
				this.requestQueue.add(new IORequest<Buffer.Output>(this, buffer));
			}
		}
		else {
			this.returnBuffers.addAll(buffers);
		}

		// get the first buffer from the empty buffer queue as current
		try {
			this.currentBuffer = nextBuffer();
			this.currentBuffer.rewind();
			checkErroneous();
		}
		catch (InterruptedException iex) {
			throw new IOException("");
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.closed;
	}
	
	/**
	 * Closes this writer. Sends a request to write the current buffer, makes sure all data is written out
	 * and waits for all memory segments to come back.
	 * 
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#close()
	 */
	@Override
	public synchronized List<MemorySegment> close() throws IOException
	{
		synchronized (this) {
			if (this.closed) {
				throw new IllegalStateException("Writer is already closing or has been closed.");
			}
			this.closed = true;
		}
		
		checkErroneous();
		
		// create a new write request for the current buffer
		if (this.currentBuffer != null) {
			this.requestQueue.add(new IORequest<Buffer.Output>(this, this.currentBuffer));
			this.currentBuffer = null;
		}

		final List<MemorySegment> segments = super.close();
		
		// flush contents to the underlying channel and close the file
		this.fileChannel.close();
		
		return segments;
	}


	/**
	 * Writes the contents of the {@code writable} to the current output buffer.
	 * If the buffer is exhausted while writing, transparently swaps the buffers
	 * and retries writing to the next buffer. Returns {@code true} if the write
	 * operation was successful or {@code false} if the provided {@code
	 * writable} is larger than the size provided by an empty output buffer.
	 * 
	 * @param writable The object writing itself to the current output buffer.
	 * @return A boolean flag indicating the success of the write operation,
	 */
	@Override
	public boolean write(IOReadableWritable writable) throws IOException
	{
		if (this.currentBuffer.write(writable)) {
			// object was written on current buffer without a problem
			return true;
		}
		else {
			// current buffer is full, check the error state of this channel
			checkErroneous();
			
			if (this.requestQueue.isClosed()) {
				throw new IOException("The writer's IO path has been closed.");
			}
			
			// write the current buffer and get the next one
			this.requestQueue.add(new IORequest<Buffer.Output>(this, currentBuffer));
			
			try {
				this.currentBuffer = nextBuffer();
				this.currentBuffer.rewind();
				checkErroneous();
			}
			catch (InterruptedException iex) {
				throw new IOException("IO channel corrupt. Writer was interrupted getting a new buffer.");
			}
			
			// retry writing with an empty input buffer
			if (this.currentBuffer.write(writable)) {
				return true;
			}
			else {
				throw new IOException("Object to be written is too large for IO-buffer.");
			}
		}
	}

	/**
	 * A worker thread that asynchronously writes the buffers to disk.
	 */
	protected static final class WriterThread extends Thread
	{
		protected final RequestQueue<IORequest<Buffer.Output>> requestQueue;

		private volatile boolean alive;

		// ---------------------------------------------------------------------
		// Constructors / Destructors
		// ---------------------------------------------------------------------

		protected WriterThread() {
			this.requestQueue = new RequestQueue<IORequest<Buffer.Output>>();
			this.alive = true;
		}

		/**
		 * Shuts the thread down. This operation does not wait for all pending requests to be served, halts the thread
		 * immediately. All buffers of pending requests are handed back to their channel writers and an exception is
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
				IOException ioex = new IOException("Writer thread has been closed.");
				
				while (!this.requestQueue.isEmpty()) {
					IORequest<Buffer.Output> request = this.requestQueue.poll();
					request.channel.handleProcessedBuffer(request.buffer, ioex);
				}
			}
		}

		// ---------------------------------------------------------------------
		// Main loop
		// ---------------------------------------------------------------------

		@Override
		public void run()
		{
			while (this.alive) {
				
				IORequest<Buffer.Output> request = null;
				
				// get the next buffer. ignore interrupts that are not due to a shutdown.
				while (request == null) {
					try {
						request = requestQueue.take();
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
					// write buffer to the specified channel
					request.buffer.writeToChannel(request.channel.fileChannel);
				}
				catch (IOException e) {
					ioex = e;
				}

				// invoke the processed buffer handler of the request issuing writer object
				request.channel.handleProcessedBuffer(request.buffer, ioex);
			} // end while alive
		}
		
	}; // end writer thread
}
