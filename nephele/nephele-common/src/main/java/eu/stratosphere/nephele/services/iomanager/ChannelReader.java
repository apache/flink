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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * A reader from an underlying channel.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public final class ChannelReader extends ChannelAccess<Buffer.Input> implements Reader
{
	/**
	 * The input wrapper, which manages reads that span the border of two buffers.
	 */
	private InputWrapper inputWrapper;
	
	/**
	 * The DataInput interface around the input wrapper.
	 */
	private DataInputStream inputWrapperReader;
	
	/**
	 * The current buffer from which is read.
	 */
	private Buffer.Input currentBuffer;
	
	/**
	 * Flag indicating whether to delete the channel file after the reading is done.
	 */
	private final boolean deleteWhenDone;
	
	/**
	 * Flag indicating that all input has been read.
	 */
	private volatile boolean allRead; 
	
	/**
	 * Flag marking this channel as closed.
	 */
	private volatile boolean closed = false;
	
	/**
	 * Flag indicating that the reader has returned all pairs it has.
	 */
	private boolean done;
	
	
	// -------------------------------------------------------------------------
	//                     Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * @param channelID
	 * @param requestQueue
	 * @param buffers
	 * @param deleteWhenDone
	 * @throws IOException
	 */
	protected ChannelReader(Channel.ID channelID, RequestQueue<IORequest<Buffer.Input>> requestQueue,
			Collection<Buffer.Input> buffers, boolean deleteWhenDone)
	throws IOException
	{
		super(channelID, requestQueue, buffers);
		
		this.inputWrapper = new InputWrapper(64);
		this.inputWrapperReader = new DataInputStream(this.inputWrapper);
		this.deleteWhenDone = deleteWhenDone;

		// add all buffers to the request queue
		for (Buffer.Input buffer : buffers) {
			this.requestQueue.add(new IORequest<Buffer.Input>(this, buffer));
		}
	}
	
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return this.closed;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#close()
	 */
	@Override
	public List<MemorySegment> close() throws IOException
	{
		synchronized (this) {
			if (this.closed) {
				throw new IllegalStateException("Reader is already closing or has been closed.");
			}
			this.closed = true;
		}
		
		checkErroneous();
		
		// put current buffer back to the full buffer queue
		if (this.currentBuffer != null) {
			this.returnBuffers.add(currentBuffer);
			this.currentBuffer = null;
		}
		
		// put the buffer in the input wrapper back, if there is one
		if (this.inputWrapper.getCurrentInput() != null) {
			Buffer.Input buf = this.inputWrapper.getCurrentInput();
			this.inputWrapper.setCurrentInput(null);
			this.returnBuffers.add(buf);
		}
		
		// close the reader, getting all segments back
		List<MemorySegment> segments = super.close();

		// close the file
		if (this.fileChannel.isOpen()) {
			this.fileChannel.close();
		}
		
		// clean up after us, if requested. don't report exceptions, just try
		if (this.deleteWhenDone) {
			try {
				deleteChannel();
			}
			catch (Throwable t) {}
		}
		
		return segments;
	}

	/**
	 * Read the contents of the {@code readable} from the current input buffer.
	 * If the buffer is exhausted while reading, transparently swaps the buffers
	 * and retries reading from the next buffer. Returns {@code true} if the
	 * read operation was successful or {@code false} if the underlying channel
	 * is exhausted.
	 * 
	 * @param readable The object reading from the current input buffer.
	 * @return A boolean flag indicating the success of the read operation.
	 */
	public boolean read(IOReadableWritable readable) throws IOException
	{
		// cache the buffer to avoid to many member variable accesses
		Buffer.Input buffer = this.currentBuffer;
		
		// the buffer is null, if
		// 1) this is the first read call
		// 2) the last read went to the input wrapper
		// 3) the reader has been closed
		// 4) the reader is exhausted
		if (buffer == null)
		{
			if (this.inputWrapper.getCurrentInput() != null) {
				// case 2: last call went to input wrapper
				buffer = this.inputWrapper.getCurrentInput();
				this.currentBuffer = buffer;
				this.inputWrapper.setCurrentInput(null);
			}
			else if (this.closed) {
				// case 3: channel closed
				throw new IllegalStateException("Reader has been closed.");
			}
			else if (this.done) {
				// case 4: completely exhausted
				return false;
			}
			else {
				// case 1: first read
				try {
					buffer = this.nextBuffer();
					this.currentBuffer = buffer;
				}
				catch (InterruptedException iex) {
					throw new IOException("IO channel corrupt. Reader was interrupted getting a new buffer.");
				}
			}
		}
		
		// get the next element from the buffer
		if (buffer.read(readable)) // try to read from current buffer
		{
			// object was read from the current buffer without a problem
			return true;
		}
		else {
			// current buffer is exhausted. check the error state of this channel first
			checkErroneous();
			
			// check if this buffer contains parts of an incomplete object
			boolean remainingBytes = buffer.getRemainingBytes() > 0;
			if (remainingBytes) {
				this.inputWrapper.setRemainingInput(buffer);
			}
			
			// only issue a new request, if not all requests have been served yet
			if (!this.allRead) {
				if (this.requestQueue.isClosed()) {
					throw new IOException("The reader's IO path has been closed.");
				}
			
				// issue request for the next piece of data
				this.requestQueue.add(new IORequest<Buffer.Input>(this, buffer));
			}
			else {
				// no further requests necessary, return the buffer
				this.returnBuffers.add(buffer);
			}
			
			// get the next buffer from the list of filled buffers
			try {
				buffer = nextBuffer();
				checkErroneous();
				
				// check if the channel is exhausted
				if (buffer.getRemainingBytes() == 0) {
					// this buffer contains no data, which means the channel was read completely
					this.done = true;
					this.returnBuffers.add(buffer);
					this.currentBuffer = null;
					
					// if parts of an incomplete object remain, the channel is corrupt.
					if (remainingBytes) {
						throw new IOException("Channel ends with an incomplete record.");
					}
					
					return false;
				}
				
				// check if the next object is to be read from the input wrapper
				if (remainingBytes) {
					this.inputWrapper.setCurrentInput(buffer);
					this.currentBuffer = null;
					readable.read(this.inputWrapperReader);
					return true;
				}
				else {
					// retry reading from the next full buffer
					this.currentBuffer = buffer;
					return buffer.read(readable);
				}
			}
			catch (InterruptedException iex) {
				throw new IOException("IO channel corrupt. Reader was interrupted getting a new buffer.");
			}
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
	public boolean repeatRead(IOReadableWritable readable)
	{
		// the buffer is null, if
		// 1) this is the first read call
		// 2) the last read went to the input wrapper
		// 3) the reader has been closed
		// 4) the reader is exhausted
		if (this.currentBuffer == null) {
			if (this.inputWrapper.getCurrentInput() != null) {
				// case 2: previous read was from wrapping buffer
				this.inputWrapper.rewind();
				try {
					readable.read(inputWrapperReader);
				}
				catch (IOException ioex) {
					// this should never happen since a previous read from the input wrapper succeeded
					throw new IllegalStateException("BUG: Repeated read from wrapped input failed after a successful read.");
				}
				return true;
			}
			else if (this.closed) {
				// case 3: reader closed
				throw new IllegalStateException("Reader has been closed.");
			}
			else if (this.done) {
				// case 4: reader exhausted
				throw new IllegalStateException("The channel has alreday been completely consumed.");
			}
			else {
				// case 1: no previous read
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
	

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.iomanager.ChannelAccess#handleProcessedBuffer(eu.stratosphere.nephele.services.iomanager.Buffer, java.io.IOException)
	 */
	public void handleProcessedBuffer(Buffer.Input buffer, IOException ex) {
		// set flag such that no further requests are issued
		if (buffer.getRemainingBytes() == 0 && !this.allRead) {
			this.allRead = true;
		}
		
		// handle buffer as we had it
		super.handleProcessedBuffer(buffer, ex);
	}
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * Utility class that takes care of objects whose serialized form is split among the remaining bytes of one
	 * buffer and the beginning of another buffer. It copies the first buffer'sremaining bytes into
	 * a temporary byte array. That way, the first buffer is immediately available again. That is important
	 * if we have only one buffer. This wrapper then acts as an input stream that first serves the first
	 * buffer's bytes from the byte array and the delegates all byte retrievals to the second buffer.
	 *
	 * @author Stephan Ewen
	 */
	private static final class InputWrapper extends InputStream
	{
		private Buffer.Input continuation;
		
		private byte[] wrappingArray;
		
		private int len;
		
		private int position;
		
		
		public InputWrapper(int initialBufferSize) {
			this.wrappingArray = new byte[initialBufferSize];
			this.position = 0;
		}
		
		public void setRemainingInput(Buffer.Input buffer) throws IOException {
			final int size = buffer.getRemainingBytes();
			
			if (size > this.wrappingArray.length) {
				this.wrappingArray = new byte[size];
			}
			
			buffer.copyRemainingBytes(this.wrappingArray);
			this.len = size;
			this.position = 0;
		}
		
		public void setCurrentInput(Buffer.Input buffer) {
			this.continuation = buffer;
		}
		
		public Buffer.Input getCurrentInput() {
			return this.continuation;
		}
		
		public void rewind() {
			this.position = 0;
			if (this.continuation != null) {
				this.continuation.rewind();
			}
		}
		
		/* (non-Javadoc)
		 * @see java.io.InputStream#read()
		 */
		@Override
		public int read() throws IOException {
			if (this.position < this.len) {
				return ((int) this.wrappingArray[this.position++]) & 0xff;
			}
			else {
				return this.continuation.getNextByte();
			}
		}
	}
	
	
	
	// ========================================================================
	// ========================================================================

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
					if (!request.buffer.memory.isFree()) {
						request.buffer.readFromChannel(request.channel.fileChannel);
					}
				}
				catch (IOException e) {
					ioex = e;
				}
				catch (Throwable t) {
					ioex = new IOException("The buffer could not be read: " + t.getMessage(), t);
				}

				// invoke the processed buffer handler of the request issuing reader object
				request.channel.handleProcessedBuffer(request.buffer, ioex);
			} // end while alive
		}
		
	} // end reading thread
	
}
